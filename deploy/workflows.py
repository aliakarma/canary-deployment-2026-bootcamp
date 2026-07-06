from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

from temporalio import workflow

# Import our types via pass-through to bypass import check constraints.
with workflow.unsafe.imports_passed_through():
    from deploy.audit import DeploymentEventType


@workflow.defn
class CanaryDeploymentWorkflow:
    """Temporal Workflow orchestrating the progressive stages of canary rollout."""

    def __init__(self) -> None:
        self.aborted = False
        self.approval_decision = None
        self.state: Dict[str, Any] = {}

    @workflow.signal
    def abort(self, reason: str = "Manual abort") -> None:
        self.aborted = True
        self.state["status"] = "aborted"
        self.state["error_message"] = reason

    @workflow.signal
    def approve(self, decision: str = "approved") -> None:
        self.approval_decision = decision

    @workflow.query
    def get_state(self) -> Dict[str, Any]:
        return self.state

    @workflow.run
    async def run_deployment(self, config: Dict[str, Any]) -> Dict[str, Any]:
        target_version = config["target_version"]
        stages = config["stages"]
        stage_delay_seconds = config["stage_delay_seconds"]
        max_retries_per_stage = config.get("max_retries_per_stage", 0)
        health_check_interval = config.get("health_check_interval", 0.5)
        current_time_str = config.get("current_time")

        deployment_id = str(workflow.uuid4())[:8]
        source_version = config.get("source_version", "1.0.0")
        total_servers = config["total_servers"]

        self.state = {
            "deployment_id": deployment_id,
            "target_version": target_version,
            "source_version": source_version,
            "total_servers": total_servers,
            "status": "pending",
            "progress_percentage": 0.0,
            "servers_updated": [],
            "servers_pending": config.get("all_server_ids", []),
            "stages": [],
            "current_stage_index": -1,
            "error_message": None,
            "started_at": str(workflow.now()),
            "completed_at": None,
            "duration_seconds": 0.0,
        }

        last_event_id = None

        async def record_event(
            event_type: DeploymentEventType, details: Dict[str, Any] | None = None
        ) -> None:
            nonlocal last_event_id
            last_event_id = await workflow.execute_activity(
                "record_event",
                {
                    "event_type_name": event_type.name,
                    "deployment_id": deployment_id,
                    "details": details,
                    "parent_event_id": last_event_id,
                },
                start_to_close_timeout=timedelta(seconds=5),
            )

        # 1. DEPLOYMENT_START
        await record_event(
            DeploymentEventType.DEPLOYMENT_START,
            {
                "target_version": target_version,
                "source_version": source_version,
                "total_servers": total_servers,
                "stages": stages,
            },
        )

        # Initial Snapshot
        snapshot_id = await workflow.execute_activity(
            "create_snapshot",
            {
                "deployment_id": deployment_id,
                "target_version": target_version,
                "source_version": source_version,
                "total_servers": total_servers,
                "updated_ids": self.state["servers_updated"],
                "metadata": {"event": "deployment_init"},
            },
            start_to_close_timeout=timedelta(seconds=10),
        )
        await record_event(
            DeploymentEventType.SNAPSHOT_CREATE,
            {
                "snapshot_id": snapshot_id,
                "reason": "Initial deployment state snapshot",
                "servers_count": total_servers,
            },
        )

        # Checkpoint: evaluate_start
        decision = await workflow.execute_activity(
            "evaluate_start",
            {
                "deployment_id": deployment_id,
                "target_version": target_version,
                "source_version": source_version,
                "total_servers": total_servers,
                "current_time": current_time_str,
            },
            start_to_close_timeout=timedelta(seconds=5),
        )
        if decision == "BLOCK":
            self.state["status"] = "failed"
            self.state["error_message"] = "Deployment blocked by governance start policy"
            await record_event(
                DeploymentEventType.POLICY_VIOLATION,
                {"reason": self.state["error_message"], "stage_index": -1},
            )
            return self.state

        self.state["status"] = "in_progress"

        try:
            for stage_idx, target_pct in enumerate(stages):
                if self.aborted:
                    raise Exception("Abort signal received before stage start")

                # Checkpoint: evaluate_stage_start
                decision = await workflow.execute_activity(
                    "evaluate_stage_start",
                    {
                        "deployment_id": deployment_id,
                        "target_version": target_version,
                        "source_version": source_version,
                        "total_servers": total_servers,
                        "updated_ids": self.state["servers_updated"],
                        "stage_idx": stage_idx,
                        "target_pct": target_pct,
                        "current_time": current_time_str,
                    },
                    start_to_close_timeout=timedelta(seconds=5),
                )
                if decision == "BLOCK":
                    self.state["status"] = "failed"
                    self.state["error_message"] = (
                        f"Stage {stage_idx} blocked by stage-start policy"
                    )
                    await record_event(
                        DeploymentEventType.POLICY_VIOLATION,
                        {"reason": self.state["error_message"], "stage_index": stage_idx},
                    )
                    return self.state

                # Pre-execution snapshot
                snapshot_id = await workflow.execute_activity(
                    "create_snapshot",
                    {
                        "deployment_id": deployment_id,
                        "target_version": target_version,
                        "source_version": source_version,
                        "total_servers": total_servers,
                        "updated_ids": self.state["servers_updated"],
                        "metadata": {"stage_index": stage_idx, "target_percentage": target_pct},
                    },
                    start_to_close_timeout=timedelta(seconds=10),
                )
                await record_event(
                    DeploymentEventType.SNAPSHOT_CREATE,
                    {
                        "snapshot_id": snapshot_id,
                        "reason": f"Pre-execution snapshot for Stage {stage_idx}",
                        "servers_count": total_servers,
                    },
                )

                stage_started_at = workflow.now()
                self.state["current_stage_index"] = stage_idx

                result = await workflow.execute_activity(
                    "execute_stage",
                    {
                        "deployment_id": deployment_id,
                        "target_version": target_version,
                        "source_version": source_version,
                        "total_servers": total_servers,
                        "updated_ids": self.state["servers_updated"],
                        "stage_idx": stage_idx,
                        "target_pct": target_pct,
                    },
                    start_to_close_timeout=timedelta(seconds=30),
                )

                newly_updated = result["newly_updated_ids"]
                stage_error = result["error"]

                if self.aborted:
                    raise Exception("Abort signal received mid-stage")

                # Update workflow state
                self.state["servers_updated"].extend(newly_updated)
                for s_id in newly_updated:
                    if s_id in self.state["servers_pending"]:
                        self.state["servers_pending"].remove(s_id)

                self.state["progress_percentage"] = (
                    len(self.state["servers_updated"]) / total_servers
                ) * 100

                stage_completed_at = workflow.now()
                stage_duration = (stage_completed_at - stage_started_at).total_seconds()

                stage_res = {
                    "stage_index": stage_idx,
                    "target_percentage": target_pct,
                    "servers_updated": newly_updated,
                    "servers_total": total_servers,
                    "started_at": stage_started_at.isoformat(),
                    "completed_at": stage_completed_at.isoformat(),
                    "duration_seconds": stage_duration,
                    "health_check_passed": None,
                    "error": stage_error,
                }
                self.state["stages"].append(stage_res)

                await record_event(
                    DeploymentEventType.STAGE_TRANSITION,
                    {
                        "stage_index": stage_idx,
                        "target_percentage": target_pct,
                        "servers_updated": newly_updated,
                    },
                )

                if stage_error:
                    if self.aborted:
                        raise Exception("Abort signal received mid-stage")
                    else:
                        raise Exception(f"Stage {stage_idx} failed: {stage_error}")

                # Run health check
                health_passed = await workflow.execute_activity(
                    "run_health_check",
                    {
                        "stage_idx": stage_idx,
                        "target_pct": target_pct,
                    },
                    start_to_close_timeout=timedelta(seconds=15),
                )

                await record_event(
                    DeploymentEventType.HEALTH_CHECK,
                    {
                        "stage_index": stage_idx,
                        "target_percentage": target_pct,
                        "status": "pass" if health_passed else "fail",
                        "retry_count": 0,
                    },
                )

                if not health_passed:
                    retries_remaining = max_retries_per_stage
                    retry_idx = 1
                    while retries_remaining > 0 and not health_passed:
                        await workflow.sleep(timedelta(seconds=health_check_interval))
                        health_passed = await workflow.execute_activity(
                            "run_health_check",
                            {
                                "stage_idx": stage_idx,
                                "target_pct": target_pct,
                            },
                            start_to_close_timeout=timedelta(seconds=15),
                        )
                        await record_event(
                            DeploymentEventType.HEALTH_CHECK,
                            {
                                "stage_index": stage_idx,
                                "target_percentage": target_pct,
                                "status": "pass" if health_passed else "fail",
                                "retry_count": retry_idx,
                            },
                        )
                        retries_remaining -= 1
                        retry_idx += 1

                    if not health_passed:
                        stage_res["health_check_passed"] = False
                        # Quarantine check
                        quarantined_regions = await workflow.execute_activity(
                            "auto_quarantine",
                            start_to_close_timeout=timedelta(seconds=10),
                        )
                        for reg in quarantined_regions:
                            await record_event(
                                DeploymentEventType.QUARANTINE_ACTIVATE,
                                {
                                    "region": reg,
                                    "reason": f"Auto-quarantining region {reg} due to health check failures",
                                },
                            )
                        raise Exception(f"Health check failed at stage {stage_idx} ({target_pct}%)")

                stage_res["health_check_passed"] = True

                if self.aborted:
                    raise Exception("Abort signal received during health checks")

                # Checkpoint: evaluate_stage_complete
                decision = await workflow.execute_activity(
                    "evaluate_stage_complete",
                    {
                        "deployment_id": deployment_id,
                        "target_version": target_version,
                        "source_version": source_version,
                        "total_servers": total_servers,
                        "updated_ids": self.state["servers_updated"],
                        "stage_idx": stage_idx,
                        "target_pct": target_pct,
                        "current_time": current_time_str,
                    },
                    start_to_close_timeout=timedelta(seconds=10),
                )
                if decision == "BLOCK":
                    self.state["status"] = "failed"
                    self.state["error_message"] = (
                        f"Stage {stage_idx} blocked post-execution by governance"
                    )
                    await record_event(
                        DeploymentEventType.POLICY_VIOLATION,
                        {"reason": self.state["error_message"], "stage_index": stage_idx},
                    )
                    return self.state
                elif decision == "ROLLBACK":
                    raise Exception(
                        f"Governance policy mandated rollback at stage {stage_idx} ({target_pct}%)"
                    )

                # Callback
                await workflow.execute_activity(
                    "on_stage_complete_callback",
                    {
                        "stage_idx": stage_idx,
                        "target_pct": target_pct,
                        "updated_count": len(newly_updated),
                    },
                    start_to_close_timeout=timedelta(seconds=10),
                )

                # Inter-stage sleep (interruptible by signal)
                if stage_idx < len(stages) - 1:
                    self.state["status"] = "paused"
                    import asyncio
                    try:
                        await workflow.wait_condition(
                            lambda: self.aborted, timeout=timedelta(seconds=stage_delay_seconds)
                        )
                        raise Exception("Abort signal received during inter-stage wait")
                    except (asyncio.TimeoutError, TimeoutError):
                        pass
                    self.state["status"] = "in_progress"

            # Finish successfully
            self.state["status"] = "completed"
            self.state["completed_at"] = str(workflow.now())
            self.state["duration_seconds"] = (
                workflow.now() - datetime.fromisoformat(self.state["started_at"])
            ).total_seconds()

            await record_event(
                DeploymentEventType.DEPLOYMENT_COMPLETED,
                {
                    "target_version": target_version,
                    "duration_seconds": self.state["duration_seconds"],
                    "servers_updated": self.state["servers_updated"],
                },
            )

            # Mark all updated servers healthy
            await workflow.execute_activity(
                "mark_servers_healthy",
                {"updated_ids": self.state["servers_updated"]},
                start_to_close_timeout=timedelta(seconds=10),
            )

        except Exception as exc:
            workflow.logger.exception("Workflow exception caught:")
            err_msg = str(exc)
            self.state["error_message"] = err_msg
            if "Abort" in err_msg or self.aborted:
                self.state["status"] = "aborted"
                await record_event(
                    DeploymentEventType.ABORT_RECEIVED,
                    {"reason": err_msg},
                )
                await record_event(
                    DeploymentEventType.ROLLBACK_INITIATED,
                    {
                        "reason": f"Aborted: {err_msg}",
                        "stage_index": self.state["current_stage_index"],
                    },
                )
                await self._rollback_workflow(
                    deployment_id,
                    target_version,
                    source_version,
                    total_servers,
                    err_msg,
                    record_event,
                    current_time_str,
                )
            else:
                is_recoverable = (
                    err_msg.startswith("Health check failed")
                    or err_msg.startswith("Governance policy mandated")
                    or (err_msg.startswith("Stage ") and ("Failed to update" in err_msg or "Under-provisioned" in err_msg))
                )

                if is_recoverable:
                    # Governance evaluate_rollback check
                    rollback_decision = await workflow.execute_activity(
                        "evaluate_rollback",
                        {
                            "deployment_id": deployment_id,
                            "target_version": target_version,
                            "source_version": source_version,
                            "total_servers": total_servers,
                            "updated_ids": self.state["servers_updated"],
                            "current_time": current_time_str,
                        },
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                    if rollback_decision == "BLOCK":
                        self.state["status"] = "failed"
                        self.state["error_message"] = "Automatic rollback blocked by governance policy."
                        await record_event(
                            DeploymentEventType.POLICY_VIOLATION,
                            {
                                "reason": "Automatic rollback blocked by governance policy.",
                                "stage_index": self.state["current_stage_index"],
                            },
                        )
                    else:
                        await record_event(
                            DeploymentEventType.ROLLBACK_INITIATED,
                            {
                                "reason": err_msg,
                                "stage_index": self.state["current_stage_index"],
                            },
                        )
                        await self._rollback_workflow(
                            deployment_id,
                            target_version,
                            source_version,
                            total_servers,
                            err_msg,
                            record_event,
                            current_time_str,
                        )
                else:
                    self.state["status"] = "failed"
                    await record_event(
                        DeploymentEventType.DEPLOYMENT_FAILED,
                        {"error": err_msg, "stage_index": self.state["current_stage_index"]},
                    )

        return self.state

    async def _rollback_workflow(
        self,
        deployment_id: str,
        target_version: str,
        source_version: str,
        total_servers: int,
        error_message: str,
        record_event: Any,
        current_time_str: str | None,
    ) -> None:
        # Pre-rollback snapshot
        snapshot_id = await workflow.execute_activity(
            "create_snapshot",
            {
                "deployment_id": deployment_id,
                "target_version": target_version,
                "source_version": source_version,
                "total_servers": total_servers,
                "updated_ids": self.state["servers_updated"],
                "metadata": {"event": "rollback_init"},
            },
            start_to_close_timeout=timedelta(seconds=10),
        )
        await record_event(
            DeploymentEventType.SNAPSHOT_CREATE,
            {
                "snapshot_id": snapshot_id,
                "reason": "Pre-rollback state snapshot",
                "servers_count": total_servers,
            },
        )

        await record_event(
            DeploymentEventType.ROLLBACK_START,
            {
                "reason": error_message or "Automatic rollback",
                "source_version": source_version,
                "servers_to_rollback": self.state["servers_updated"],
            },
        )

        result = await workflow.execute_activity(
            "rollback_updated_servers",
            {
                "deployment_id": deployment_id,
                "target_version": target_version,
                "source_version": source_version,
                "total_servers": total_servers,
                "updated_ids": self.state["servers_updated"],
                "error_message": error_message,
            },
            start_to_close_timeout=timedelta(seconds=45),
        )

        rolled_back_ids = result["rolled_back_ids"]
        success = result["success"]

        await record_event(
            DeploymentEventType.ROLLBACK_COMPLETE,
            {
                "servers_rolled_back": rolled_back_ids,
            },
        )
        if self.aborted:
            self.state["status"] = "aborted"
        elif success:
            self.state["status"] = "rolled_back"
        else:
            self.state["status"] = "failed"
