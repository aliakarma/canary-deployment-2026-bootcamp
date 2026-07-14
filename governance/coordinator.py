"""
Governance Coordinator managing policies, risk engine, and approval gates.
"""

from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING, Any, Dict

from governance.approvals import ApprovalGate
from governance.models import (
    ApprovalDecision,
    ApprovalRequest,
    GovernanceDecision,
    PolicyEvaluationResult,
    RiskScore,
)
from governance.policies import (
    AbortPolicy,
    ApprovalPolicy,
    BasePolicy,
    HealthPolicy,
    RiskPolicy,
    RollbackPolicy,
)
from governance.risk import RiskEngine
from logging_config import get_logger

if TYPE_CHECKING:
    from cluster.state import ClusterState
    from deploy.audit import AuditLogger
    from deploy.state import DeploymentState

logger = get_logger(__name__)


class GovernanceCoordinator:
    """Coordinates risk scoring, policy evaluation, and manual approvals.

    Intercepts deployment lifecycle milestones to ensure security compliance
    and system stability.
    """

    def __init__(
        self,
        policies: list[BasePolicy] | None = None,
        approval_gate: ApprovalGate | None = None,
        risk_engine: RiskEngine | None = None,
        quarantine_system: Any | None = None,
    ) -> None:
        """Initialize coordinator with policies, approval gate, and risk engine."""
        self.risk_engine = risk_engine or RiskEngine()
        self.approval_gate = approval_gate or ApprovalGate()
        self.quarantine_system = quarantine_system

        # Default standard set of governance policies
        if policies is not None:
            self.policies = policies
        else:
            self.policies = [
                RollbackPolicy(),
                HealthPolicy(),
                ApprovalPolicy(),
                AbortPolicy(),
                RiskPolicy(),
            ]
            from resilience.policies import (
                RecoveryRetryCeilingPolicy,
                RollbackStormPreventionPolicy,
                UnsafeRecoveryPreventionPolicy,
            )

            self.policies.extend(
                [
                    UnsafeRecoveryPreventionPolicy(),
                    RecoveryRetryCeilingPolicy(),
                    RollbackStormPreventionPolicy(),
                ]
            )
            if quarantine_system is not None:
                from resilience.policies import QuarantineEscalationPolicy

                self.policies.append(QuarantineEscalationPolicy(quarantine_system))
        self._last_risk_category: RiskScore | None = None

    def evaluate_start(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        current_time: datetime.datetime | None = None,
        audit_logger: AuditLogger | None = None,
    ) -> GovernanceDecision:
        """Evaluate policies at deployment initialization."""
        context = self._build_context(cluster_state, deployment_state, current_time=current_time)
        return self._evaluate_checkpoint(
            cluster_state,
            deployment_state,
            context,
            checkpoint_name="on_deployment_start",
            stage_index=-1,
            audit_logger=audit_logger,
        )

    def evaluate_stage_start(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        stage_index: int,
        target_percentage: int,
        current_time: datetime.datetime | None = None,
        audit_logger: AuditLogger | None = None,
    ) -> GovernanceDecision:
        """Evaluate policies prior to executing a rollout stage."""
        context = self._build_context(cluster_state, deployment_state, current_time=current_time)
        context["stage_index"] = stage_index
        context["target_percentage"] = target_percentage
        return self._evaluate_checkpoint(
            cluster_state,
            deployment_state,
            context,
            checkpoint_name="on_stage_start",
            stage_index=stage_index,
            audit_logger=audit_logger,
        )

    def evaluate_stage_complete(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        stage_index: int,
        target_percentage: int,
        current_time: datetime.datetime | None = None,
        audit_logger: AuditLogger | None = None,
    ) -> GovernanceDecision:
        """Evaluate policies after stage rollout but before advancing."""
        context = self._build_context(cluster_state, deployment_state, current_time=current_time)
        context["stage_index"] = stage_index
        context["target_percentage"] = target_percentage
        return self._evaluate_checkpoint(
            cluster_state,
            deployment_state,
            context,
            checkpoint_name="on_stage_complete",
            stage_index=stage_index,
            audit_logger=audit_logger,
        )

    def evaluate_rollback(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        current_time: datetime.datetime | None = None,
        audit_logger: AuditLogger | None = None,
    ) -> GovernanceDecision:
        """Evaluate policies prior to automatic rollback execution."""
        context = self._build_context(cluster_state, deployment_state, current_time=current_time)
        return self._evaluate_checkpoint(
            cluster_state,
            deployment_state,
            context,
            checkpoint_name="on_rollback",
            stage_index=deployment_state.current_stage_index,
            audit_logger=audit_logger,
        )

    # ------------------------------------------------------------------
    # Private Helper methods
    # ------------------------------------------------------------------

    def _build_context(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        current_time: datetime.datetime | None = None,
        retry_count: int = 0,
    ) -> Dict[str, Any]:
        """Calculate active metrics and prepare contextual dict."""
        score, category = self.risk_engine.calculate_risk(
            cluster_state,
            deployment_state,
            retry_count=retry_count,
        )
        return {
            "risk_score": score,
            "risk_category": category,
            "current_time": current_time or datetime.datetime.now(),
        }

    def _evaluate_checkpoint(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        context: Dict[str, Any],
        checkpoint_name: str,
        stage_index: int,
        audit_logger: AuditLogger | None = None,
    ) -> GovernanceDecision:
        """Perform evaluation loop for all active policies at a checkpoint."""
        from deploy.audit import DeploymentEvent, DeploymentEventType

        deployment_id = deployment_state.deployment_id

        if audit_logger is not None:
            events = audit_logger.get_events()
            context["recent_rollbacks"] = sum(
                1
                for e in events
                if getattr(e, "event_type", None) == DeploymentEventType.ROLLBACK_START
            )
            context["recovery_attempts"] = sum(
                1
                for e in events
                if getattr(e, "event_type", None) == DeploymentEventType.RECOVERY_PLAN_EXECUTE
            )

        score = context["risk_score"]
        category = context["risk_category"]

        # 1. Log risk score transition
        if audit_logger is not None:
            # Always log risk category transition details
            audit_logger.log(
                DeploymentEvent(
                    event_type=DeploymentEventType.RISK_SCORE_TRANSITION,
                    deployment_id=deployment_id,
                    details={
                        "checkpoint": checkpoint_name,
                        "risk_score": score,
                        "previous_category": (
                            self._last_risk_category.value if self._last_risk_category else "NONE"
                        ),
                        "new_category": category.value,
                    },
                )
            )
        self._last_risk_category = category

        overall_decision = GovernanceDecision.ALLOW
        block_reasons: list[str] = []
        rollback_reasons: list[str] = []
        approval_required_result: PolicyEvaluationResult | None = None

        # 2. Evaluate each policy rule
        for policy in self.policies:
            try:
                res = policy.evaluate(cluster_state, deployment_state, context)

                # Log policy evaluation event
                if audit_logger is not None:
                    audit_logger.log(
                        DeploymentEvent(
                            event_type=DeploymentEventType.POLICY_EVALUATION,
                            deployment_id=deployment_id,
                            details={
                                "policy_name": res.policy_name,
                                "checkpoint": checkpoint_name,
                                "passed": res.passed,
                                "decision": res.decision.value,
                                "message": res.message,
                            },
                        )
                    )

                if not res.passed:
                    # Log policy violation event
                    if audit_logger is not None:
                        audit_logger.log(
                            DeploymentEvent(
                                event_type=DeploymentEventType.POLICY_VIOLATION,
                                deployment_id=deployment_id,
                                details={
                                    "policy_name": res.policy_name,
                                    "message": res.message,
                                    "decision": res.decision.value,
                                },
                            )
                        )

                    # Update overall decision
                    if res.decision == GovernanceDecision.BLOCK:
                        overall_decision = GovernanceDecision.BLOCK
                        block_reasons.append(f"{res.policy_name}: {res.message}")
                    elif res.decision == GovernanceDecision.ROLLBACK:
                        if overall_decision != GovernanceDecision.BLOCK:
                            overall_decision = GovernanceDecision.ROLLBACK
                        rollback_reasons.append(f"{res.policy_name}: {res.message}")
                    elif policy.name == "ApprovalPolicy":
                        approval_required_result = res

            except Exception as exc:
                logger.error("Error evaluating policy %s: %s", policy.name, exc)
                overall_decision = GovernanceDecision.BLOCK
                block_reasons.append(f"System failure evaluating policy {policy.name}")

        # 3. Check for human approval requirements
        if overall_decision == GovernanceDecision.ALLOW and approval_required_result is not None:
            req_id = f"req-{uuid.uuid4().hex[:8]}"
            req = ApprovalRequest(
                request_id=req_id,
                deployment_id=deployment_id,
                stage_index=stage_index,
                reason=approval_required_result.message,
                details={
                    "risk_score": score,
                    "risk_category": category.value,
                    "checkpoint": checkpoint_name,
                },
            )

            # Log approval request
            if audit_logger is not None:
                audit_logger.log(
                    DeploymentEvent(
                        event_type=DeploymentEventType.APPROVAL_REQUEST,
                        deployment_id=deployment_id,
                        details={
                            "request_id": req_id,
                            "stage_index": stage_index,
                            "reason": req.reason,
                        },
                    )
                )

            # Evaluate manual approval gate
            decision = self.approval_gate.evaluate_request(req, category.value)

            # Log approval decision
            if audit_logger is not None:
                audit_logger.log(
                    DeploymentEvent(
                        event_type=(DeploymentEventType.APPROVAL_DECISION),
                        deployment_id=deployment_id,
                        details={
                            "request_id": req_id,
                            "decision": decision.value,
                            "status": req.status.value,
                        },
                    )
                )

            if decision == ApprovalDecision.DENIED:
                overall_decision = GovernanceDecision.BLOCK
                block_reasons.append("Manual approval was DENIED by the gatekeeper.")

        # 4. Log overall governance decision
        if audit_logger is not None:
            reason_str = ""
            if overall_decision == GovernanceDecision.BLOCK:
                reason_str = "; ".join(block_reasons)
            elif overall_decision == GovernanceDecision.ROLLBACK:
                reason_str = "; ".join(rollback_reasons)
            else:
                reason_str = "All policies passed and approvals obtained successfully."

            audit_logger.log(
                DeploymentEvent(
                    event_type=DeploymentEventType.GOVERNANCE_DECISION,
                    deployment_id=deployment_id,
                    details={
                        "checkpoint": checkpoint_name,
                        "decision": overall_decision.value,
                        "reason": reason_str,
                    },
                )
            )

        return overall_decision
