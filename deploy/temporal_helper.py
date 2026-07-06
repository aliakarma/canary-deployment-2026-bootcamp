from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List

from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from cluster.state import ClusterState
from deploy.activities import CanaryDeploymentActivities
from deploy.config import DeploymentConfig
from deploy.state import DeploymentState, DeploymentStatus, StageResult
from deploy.workflows import CanaryDeploymentWorkflow


_cached_env: WorkflowEnvironment | None = None


async def run_temporal_deployment(
    cluster: ClusterState,
    config: DeploymentConfig,
    deployment: DeploymentState | None = None,
    engine: Any = None,
) -> DeploymentState:
    """Helper to run a full progressive rollout workflow against a local ephemeral Temporal Server."""
    global _cached_env
    # 1. Start or reuse ephemeral dev server
    if _cached_env is None:
        _cached_env = await WorkflowEnvironment.start_local()
    client = _cached_env.client

    # 2. Instantiate activities class
    activities_inst = CanaryDeploymentActivities(
        engine=engine,
        cluster=cluster,
        quarantine_system=config.quarantine_system,
        governance_coordinator=config.governance_coordinator,
        audit_logger=config.audit_logger,
        health_check_fn=config.health_check_fn,
        on_stage_complete=config.on_stage_complete,
    )

    # 3. Register worker
    worker = Worker(
        client,
        task_queue="canary-deployment-task-queue",
        workflows=[CanaryDeploymentWorkflow],
        activities=[
            activities_inst.evaluate_start,
            activities_inst.evaluate_stage_start,
            activities_inst.create_snapshot,
            activities_inst.execute_stage,
            activities_inst.run_health_check,
            activities_inst.auto_quarantine,
            activities_inst.evaluate_stage_complete,
            activities_inst.on_stage_complete_callback,
            activities_inst.evaluate_rollback,
            activities_inst.rollback_updated_servers,
            activities_inst.record_event,
            activities_inst.mark_servers_healthy,
        ],
    )

    # Run worker in background
    async with worker:
        summary = cluster.get_deployment_summary()
        versions = summary["versions"]
        source_version = max(versions, key=lambda v: versions[v])
        all_server_ids = [s.id for s in cluster.servers]

        current_time_str = config.current_time.isoformat() if config.current_time else None

        workflow_input = {
            "target_version": config.target_version,
            "stages": config.stages,
            "stage_delay_seconds": config.stage_delay_seconds,
            "max_retries_per_stage": config.max_retries_per_stage,
            "health_check_interval": config.health_check_interval,
            "current_time": current_time_str,
            "source_version": source_version,
            "total_servers": cluster.size,
            "all_server_ids": all_server_ids,
        }

        # Start workflow
        handle = await client.start_workflow(
            CanaryDeploymentWorkflow.run_deployment,
            workflow_input,
            id=f"canary-rollout-{config.target_version}",
            task_queue="canary-deployment-task-queue",
        )

        # Polling task for in-place updates (for dashboard real-time updates)
        poll_task = None
        if deployment is not None:

            async def poll_state():
                try:
                    while True:
                        await asyncio.sleep(0.1)
                        try:
                            wf_state = await handle.query(CanaryDeploymentWorkflow.get_state)
                            if wf_state:
                                deployment.deployment_id = wf_state["deployment_id"]
                                deployment.status = DeploymentStatus(wf_state["status"])
                                deployment.servers_updated = set(wf_state["servers_updated"])
                                deployment.servers_pending = set(wf_state["servers_pending"])
                                deployment.current_stage_index = wf_state["current_stage_index"]
                                deployment.error_message = wf_state["error_message"]

                                # Reconstruct stages
                                deployment.stages.clear()
                                for st in wf_state["stages"]:
                                    sr = StageResult(
                                        stage_index=st["stage_index"],
                                        target_percentage=st["target_percentage"],
                                        servers_updated=st["servers_updated"],
                                        servers_total=st["servers_total"],
                                        health_check_passed=st["health_check_passed"],
                                        started_at=(
                                            datetime.fromisoformat(st["started_at"])
                                            if st.get("started_at")
                                            else datetime.now()
                                        ),
                                        completed_at=(
                                            datetime.fromisoformat(st["completed_at"])
                                            if st.get("completed_at")
                                            else None
                                        ),
                                        duration_seconds=st["duration_seconds"],
                                        error=st["error"],
                                    )
                                    deployment.stages.append(sr)

                                if wf_state.get("started_at"):
                                    deployment.started_at = datetime.fromisoformat(
                                        wf_state["started_at"]
                                    )
                                if wf_state.get("completed_at"):
                                    deployment.completed_at = datetime.fromisoformat(
                                        wf_state["completed_at"]
                                    )
                        except Exception:
                            pass
                except asyncio.CancelledError:
                    pass

            poll_task = asyncio.create_task(poll_state())

        # Monitor abort events in background
        abort_task = None
        if config.abort_event is not None:

            async def watch_abort():
                try:
                    while True:
                        if config.abort_event.is_set():
                            await handle.signal(
                                CanaryDeploymentWorkflow.abort, "Abort listener trigger"
                            )
                            break
                        await asyncio.sleep(0.1)
                except asyncio.CancelledError:
                    pass

            abort_task = asyncio.create_task(watch_abort())

        # Wait for outcome
        try:
            workflow_result = await handle.result()
        finally:
            if poll_task:
                poll_task.cancel()
            if abort_task:
                abort_task.cancel()

        # Final update of the deployment object
        if deployment is not None:
            deployment.status = DeploymentStatus(workflow_result["status"])
            deployment.servers_updated = set(workflow_result["servers_updated"])
            deployment.servers_pending = set(workflow_result["servers_pending"])
            deployment.current_stage_index = workflow_result["current_stage_index"]
            deployment.error_message = workflow_result["error_message"]

            deployment.stages.clear()
            for st in workflow_result["stages"]:
                sr = StageResult(
                    stage_index=st["stage_index"],
                    target_percentage=st["target_percentage"],
                    servers_updated=st["servers_updated"],
                    servers_total=st["servers_total"],
                    health_check_passed=st["health_check_passed"],
                    started_at=(
                        datetime.fromisoformat(st["started_at"])
                        if st.get("started_at")
                        else datetime.now()
                    ),
                    completed_at=(
                        datetime.fromisoformat(st["completed_at"])
                        if st.get("completed_at")
                        else None
                    ),
                    duration_seconds=st["duration_seconds"],
                    error=st["error"],
                )
                deployment.stages.append(sr)

            if workflow_result.get("started_at"):
                deployment.started_at = datetime.fromisoformat(workflow_result["started_at"])
            if workflow_result.get("completed_at"):
                deployment.completed_at = datetime.fromisoformat(workflow_result["completed_at"])

            return deployment

        # Reconstruct output state
        result_state = DeploymentState(
            deployment_id=workflow_result["deployment_id"],
            target_version=workflow_result["target_version"],
            source_version=workflow_result["source_version"],
            total_servers=workflow_result["total_servers"],
            servers_updated=set(workflow_result["servers_updated"]),
        )
        result_state.status = DeploymentStatus(workflow_result["status"])
        result_state.progress_percentage = workflow_result["progress_percentage"]
        result_state.error_message = workflow_result["error_message"]

        for st in workflow_result["stages"]:
            sr = StageResult(
                stage_index=st["stage_index"],
                target_percentage=st["target_percentage"],
                servers_total=st["servers_total"],
                started_at=(
                    datetime.fromisoformat(st["started_at"]) if st.get("started_at") else None
                ),
            )
            sr.completed_at = (
                datetime.fromisoformat(st["completed_at"]) if st.get("completed_at") else None
            )
            sr.duration_seconds = st["duration_seconds"]
            sr.health_check_passed = st["health_check_passed"]
            sr.error = st["error"]
            sr.servers_updated = st["servers_updated"]
            result_state.stages.append(sr)

        if workflow_result.get("started_at"):
            result_state.started_at = datetime.fromisoformat(workflow_result["started_at"])
        if workflow_result.get("completed_at"):
            result_state.completed_at = datetime.fromisoformat(workflow_result["completed_at"])

    return result_state
