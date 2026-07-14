from __future__ import annotations

import asyncio

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from cluster.generator import generate_cluster
from cluster.state import ClusterState
from deploy.activities import CanaryDeploymentActivities
from deploy.workflows import CanaryDeploymentWorkflow


@pytest.mark.anyio
class TestTemporalCanaryRollout:
    """Integration tests verifying the Canary Deployment Workflow on an ephemeral Temporal environment."""

    async def test_successful_rollout_workflow(self) -> None:
        # 1. Setup cluster state
        cluster = ClusterState(generate_cluster(size=5, seed=42))

        # 2. Setup environment and client
        async with await WorkflowEnvironment.start_local() as env:
            activities_inst = CanaryDeploymentActivities(
                cluster=cluster,
                health_check_fn=lambda cs: True,
            )

            # Register worker
            async with Worker(
                env.client,
                task_queue="test-task-queue",
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
            ):
                # Start workflow
                all_server_ids = [s.id for s in cluster.servers]
                workflow_input = {
                    "target_version": "2.0.0",
                    "stages": [20, 100],
                    "stage_delay_seconds": 0.1,
                    "max_retries_per_stage": 0,
                    "health_check_interval": 0.1,
                    "source_version": "1.0.0",
                    "total_servers": 5,
                    "all_server_ids": all_server_ids,
                }

                res = await env.client.execute_workflow(
                    CanaryDeploymentWorkflow.run_deployment,
                    workflow_input,
                    id="test-successful-workflow",
                    task_queue="test-task-queue",
                )

                assert res["status"] == "completed"
                assert len(res["servers_updated"]) == 5
                for s in cluster.servers:
                    assert s.current_version == "2.0.0"

    async def test_failed_rollout_with_rollback(self) -> None:
        # Setup cluster state
        cluster = ClusterState(generate_cluster(size=5, seed=42))

        async with await WorkflowEnvironment.start_local() as env:
            # Inject a failing health check
            activities_inst = CanaryDeploymentActivities(
                cluster=cluster,
                health_check_fn=lambda cs: False,  # Spikes failures
            )

            async with Worker(
                env.client,
                task_queue="test-task-queue",
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
            ):
                all_server_ids = [s.id for s in cluster.servers]
                workflow_input = {
                    "target_version": "2.0.0",
                    "stages": [20, 100],
                    "stage_delay_seconds": 0.1,
                    "max_retries_per_stage": 0,
                    "health_check_interval": 0.1,
                    "source_version": "1.0.0",
                    "total_servers": 5,
                    "all_server_ids": all_server_ids,
                }

                res = await env.client.execute_workflow(
                    CanaryDeploymentWorkflow.run_deployment,
                    workflow_input,
                    id="test-failed-workflow",
                    task_queue="test-task-queue",
                )

                # Should roll back to 1.0.0
                assert res["status"] == "rolled_back"
                for s in cluster.servers:
                    assert s.current_version == "1.0.0"

    async def test_workflow_query_and_abort_signal(self) -> None:
        cluster = ClusterState(generate_cluster(size=5, seed=42))

        async with await WorkflowEnvironment.start_local() as env:
            activities_inst = CanaryDeploymentActivities(
                cluster=cluster,
                health_check_fn=lambda cs: True,
            )

            async with Worker(
                env.client,
                task_queue="test-task-queue",
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
            ):
                all_server_ids = [s.id for s in cluster.servers]
                workflow_input = {
                    "target_version": "2.0.0",
                    "stages": [20, 40, 60, 80, 100],
                    "stage_delay_seconds": 1.0,  # Long delay so we can signal
                    "max_retries_per_stage": 0,
                    "health_check_interval": 0.1,
                    "source_version": "1.0.0",
                    "total_servers": 5,
                    "all_server_ids": all_server_ids,
                }

                handle = await env.client.start_workflow(
                    CanaryDeploymentWorkflow.run_deployment,
                    workflow_input,
                    id="test-abort-workflow",
                    task_queue="test-task-queue",
                )

                # Let stage 0 run and check query
                await asyncio.sleep(0.3)
                state = await handle.query(CanaryDeploymentWorkflow.get_state)
                assert state["status"] in ("in_progress", "paused")

                # Send abort signal
                await handle.signal(CanaryDeploymentWorkflow.abort, "Testing manual abort signal")

                # Wait for outcome
                res = await handle.result()
                assert res["status"] == "aborted"
                for s in cluster.servers:
                    assert s.current_version == "1.0.0"
