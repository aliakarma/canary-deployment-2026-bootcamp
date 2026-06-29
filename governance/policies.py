"""
Governance Policies for the Canary Deployment Simulator.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, Dict

from governance.models import GovernanceDecision, PolicyEvaluationResult, RiskScore

if TYPE_CHECKING:
    from cluster.state import ClusterState
    from deploy.state import DeploymentState


class BasePolicy:
    """Base abstract governance policy class."""

    def __init__(self, name: str) -> None:
        self.name = name

    def evaluate(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        context: Dict[str, Any],
    ) -> PolicyEvaluationResult:
        """Evaluate the policy. Must return a PolicyEvaluationResult."""
        raise NotImplementedError


class RollbackPolicy(BasePolicy):
    """Enforces safety rules around automatic and manual rollbacks."""

    def __init__(self, suspend_auto_rollback_on_critical: bool = True) -> None:
        super().__init__("RollbackPolicy")
        self.suspend_auto_rollback_on_critical = suspend_auto_rollback_on_critical

    def evaluate(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        context: Dict[str, Any],
    ) -> PolicyEvaluationResult:
        risk_category = context.get("risk_category", RiskScore.LOW)

        # If risk is critical and auto-rollback suspension is active, we block automatic rollback
        if risk_category == RiskScore.CRITICAL and self.suspend_auto_rollback_on_critical:
            return PolicyEvaluationResult(
                policy_name=self.name,
                passed=False,
                decision=GovernanceDecision.BLOCK,
                message="Automatic rollback SUSPENDED. Risk level is CRITICAL. Requires manual operator intervention.",
            )

        return PolicyEvaluationResult(
            policy_name=self.name,
            passed=True,
            decision=GovernanceDecision.ALLOW,
            message="Automatic rollback permitted under standard governance rules.",
        )


class HealthPolicy(BasePolicy):
    """Enforces strict regional health policies."""

    def __init__(self, zero_degraded_regions: set[str] | None = None) -> None:
        super().__init__("HealthPolicy")
        self.zero_degraded_regions = zero_degraded_regions or {"us-east-1"}

    def evaluate(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        context: Dict[str, Any],
    ) -> PolicyEvaluationResult:
        for server in cluster_state.servers:
            status_val = getattr(server.status, "value", str(server.status)).upper()
            if status_val in ("DEGRADED", "FAILED") and server.region in self.zero_degraded_regions:
                # If updated to target version and degraded
                if server.id in deployment_state.servers_updated:
                    return PolicyEvaluationResult(
                        policy_name=self.name,
                        passed=False,
                        decision=GovernanceDecision.ROLLBACK,
                        message=f"Strict zero-degradation policy violated: server {server.id} in critical region '{server.region}' is in state {status_val}.",
                    )

        return PolicyEvaluationResult(
            policy_name=self.name,
            passed=True,
            decision=GovernanceDecision.ALLOW,
            message="All critical region health policies satisfied.",
        )


class ApprovalPolicy(BasePolicy):
    """Determines when manual approval gates are required."""

    def __init__(self, progress_threshold: float = 75.0) -> None:
        super().__init__("ApprovalPolicy")
        self.progress_threshold = progress_threshold

    def evaluate(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        context: Dict[str, Any],
    ) -> PolicyEvaluationResult:
        risk_category = context.get("risk_category", RiskScore.LOW)
        progress = getattr(deployment_state, "progress_percentage", 0.0)

        needs_approval = False
        reason = ""

        if risk_category in (RiskScore.HIGH, RiskScore.CRITICAL):
            needs_approval = True
            reason = f"Risk score is too high ({risk_category.value})"
        elif progress >= self.progress_threshold:
            needs_approval = True
            reason = f"Deployment progress ({progress}%) exceeds stage gate threshold ({self.progress_threshold}%)"

        if needs_approval:
            return PolicyEvaluationResult(
                policy_name=self.name,
                passed=False,
                decision=GovernanceDecision.ALLOW,  # ALLOW execution path but flags approval request
                message=f"Deployment progression requires manual approval. Reason: {reason}",
            )

        return PolicyEvaluationResult(
            policy_name=self.name,
            passed=True,
            decision=GovernanceDecision.ALLOW,
            message="No manual approval required for the current stage.",
        )


class AbortPolicy(BasePolicy):
    """Enforces deployment termination policies."""

    def __init__(self) -> None:
        super().__init__("AbortPolicy")

    def evaluate(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        context: Dict[str, Any],
    ) -> PolicyEvaluationResult:
        risk_category = context.get("risk_category", RiskScore.LOW)
        if risk_category == RiskScore.CRITICAL:
            return PolicyEvaluationResult(
                policy_name=self.name,
                passed=False,
                decision=GovernanceDecision.BLOCK,
                message="Deployment aborted. Instability metrics exceeded critical boundaries.",
            )

        return PolicyEvaluationResult(
            policy_name=self.name,
            passed=True,
            decision=GovernanceDecision.ALLOW,
            message="Abort policy threshold not reached.",
        )


class RiskPolicy(BasePolicy):
    """Enforces restricted deployment windows."""

    def __init__(self) -> None:
        super().__init__("RiskPolicy")

    def evaluate(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        context: Dict[str, Any],
    ) -> PolicyEvaluationResult:
        current_time = context.get("current_time", datetime.datetime.now())

        # ISO weekday: 1=Monday, 5=Friday, 6=Saturday, 7=Sunday
        weekday = current_time.isoweekday()
        hour = current_time.hour

        blocked = False
        reason = ""

        if weekday in (6, 7):
            blocked = True
            reason = "Restricted window: deployments are blocked on weekends."
        elif weekday == 5 and hour >= 15:
            blocked = True
            reason = "Restricted window: Friday afternoon deployments are blocked."

        if blocked:
            return PolicyEvaluationResult(
                policy_name=self.name,
                passed=False,
                decision=GovernanceDecision.BLOCK,
                message=reason,
            )

        return PolicyEvaluationResult(
            policy_name=self.name,
            passed=True,
            decision=GovernanceDecision.ALLOW,
            message="Deployment window check passed.",
        )
