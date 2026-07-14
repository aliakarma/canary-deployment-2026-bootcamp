"""
Recovery Planning Engine to orchestrate failure mitigation and safe resume workflows.
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any, Callable, Dict, List

from cluster.models import ServerStatus
from cluster.state import ClusterState
from deploy.state import DeploymentState, DeploymentStatus
from logging_config import get_logger
from resilience.models import RecoveryPlan, RecoveryPlanStatus
from resilience.quarantine import RegionQuarantineSystem

logger = get_logger(__name__)


class RecoveryPlanningEngine:
    """Generates and executes multi-step recovery workflows for failing deployments."""

    def __init__(
        self,
        cluster: ClusterState,
        quarantine_system: RegionQuarantineSystem,
        approval_callback: Callable[[Dict[str, Any]], bool] | None = None,
    ) -> None:
        self.cluster = cluster
        self.quarantine_system = quarantine_system
        self.approval_callback = (
            approval_callback  # Simulates human verification before recovery steps
        )
        self._plans: Dict[str, RecoveryPlan] = {}

    def generate_plan(
        self,
        deployment: DeploymentState,
        strategy: str,
        target_region: str | None = None,
    ) -> RecoveryPlan:
        """Create a recovery plan tailored to the deployment failure context and strategy."""
        plan_id = f"plan-{uuid.uuid4().hex[:8]}"
        steps: List[Dict[str, Any]] = []

        if strategy == "partial_rollback":
            # Revert only degraded or failed servers in deployment updated list
            degraded_servers = [
                s.id
                for s in self.cluster.servers
                if s.id in deployment.servers_updated
                and s.status in (ServerStatus.DEGRADED, ServerStatus.FAILED)
            ]
            for idx, server_id in enumerate(degraded_servers):
                steps.append(
                    {
                        "step_index": idx,
                        "action": "rollback_server",
                        "target": server_id,
                        "description": f"Rollback degraded server {server_id} to pre-deployment version.",
                    }
                )

        elif strategy == "staged_recovery":
            # Rollback updated servers in small batches (e.g., 2 at a time) to prevent rollback storms
            updated_list = list(deployment.servers_updated)
            batch_size = 2
            step_idx = 0
            for i in range(0, len(updated_list), batch_size):
                batch = updated_list[i : i + batch_size]
                steps.append(
                    {
                        "step_index": step_idx,
                        "action": "rollback_batch",
                        "target": batch,
                        "description": f"Rollback batch of servers: {batch}.",
                    }
                )
                step_idx += 1

        elif strategy == "region_quarantine":
            # Quarantine target region, rollback updated servers inside that region
            if target_region:
                steps.append(
                    {
                        "step_index": 0,
                        "action": "quarantine_region",
                        "target": target_region,
                        "description": f"Quarantine unstable region '{target_region}' to isolate failures.",
                    }
                )
                # Revert servers in quarantined region
                servers_in_region = [
                    s.id
                    for s in self.cluster.servers
                    if s.region == target_region and s.id in deployment.servers_updated
                ]
                steps.append(
                    {
                        "step_index": 1,
                        "action": "rollback_batch",
                        "target": servers_in_region,
                        "description": f"Rollback updated servers in quarantined region: {servers_in_region}.",
                    }
                )

        elif strategy == "safe_resume":
            # Release quarantine and mark deployment paused -> back in progress
            if target_region:
                steps.append(
                    {
                        "step_index": 0,
                        "action": "release_quarantine",
                        "target": target_region,
                        "description": f"Release quarantine on region '{target_region}' after verification.",
                    }
                )
            steps.append(
                {
                    "step_index": 1 if target_region else 0,
                    "action": "resume_deployment",
                    "target": deployment.deployment_id,
                    "description": f"Resume deployment {deployment.deployment_id} rollout stages.",
                }
            )

        plan = RecoveryPlan(
            plan_id=plan_id,
            deployment_id=deployment.deployment_id,
            strategy=strategy,
            status=RecoveryPlanStatus.PENDING,
            target_region=target_region,
            steps=steps,
        )
        self._plans[plan_id] = plan
        logger.info(
            "Generated recovery plan %s [strategy: %s] with %d steps", plan_id, strategy, len(steps)
        )
        return plan

    def execute_recovery_plan(
        self, plan: RecoveryPlan, deployment: DeploymentState | None = None
    ) -> bool:
        """Execute the steps defined in the recovery plan sequentially.

        Checks manual approval callbacks if registered.
        """
        logger.info("Executing recovery plan: %s", plan.plan_id)
        plan.status = RecoveryPlanStatus.EXECUTING

        for step in plan.steps[plan.current_step_index :]:
            # Trigger manual approval hook if set
            if self.approval_callback is not None:
                approved = self.approval_callback(step)
                if not approved:
                    logger.warning(
                        "Step %d (%s) REJECTED by manual review. Halting plan.",
                        step["step_index"],
                        step["action"],
                    )
                    plan.status = RecoveryPlanStatus.FAILED
                    plan.error_message = f"Manual approval denied at step {step['step_index']}"
                    return False

            action = step["action"]
            target = step["target"]
            logger.info("Executing recovery step %d: %s on %s", step["step_index"], action, target)

            try:
                if action == "rollback_server":
                    self.cluster.rollback_server(target)
                    if deployment is not None:
                        deployment.servers_updated.discard(target)

                elif action == "rollback_batch":
                    for s_id in target:
                        self.cluster.rollback_server(s_id)
                        if deployment is not None:
                            deployment.servers_updated.discard(s_id)

                elif action == "quarantine_region":
                    self.quarantine_system.quarantine_region(
                        region=target, reason="Operational recovery plan quarantining region."
                    )

                elif action == "release_quarantine":
                    self.quarantine_system.release_region(region=target)

                elif action == "resume_deployment":
                    if deployment is not None:
                        deployment.status = DeploymentStatus.IN_PROGRESS

                plan.current_step_index += 1

            except Exception as exc:
                logger.error(
                    "Failed to execute recovery step %d: %s. Plan aborted.", step["step_index"], exc
                )
                plan.status = RecoveryPlanStatus.FAILED
                plan.error_message = f"Exception in step {step['step_index']}: {exc}"
                return False

        plan.status = RecoveryPlanStatus.COMPLETED
        plan.completed_at = datetime.datetime.now(datetime.timezone.utc)
        logger.info("Recovery plan %s COMPLETED successfully", plan.plan_id)
        return True

    def get_plan(self, plan_id: str) -> RecoveryPlan | None:
        return self._plans.get(plan_id)
