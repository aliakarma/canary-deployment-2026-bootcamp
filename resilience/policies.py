"""
Resilience-aware governance policies.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict

from governance.models import GovernanceDecision, PolicyEvaluationResult, RiskScore
from governance.policies import BasePolicy

if TYPE_CHECKING:
    from cluster.state import ClusterState
    from deploy.state import DeploymentState
    from resilience.quarantine import RegionQuarantineSystem


class QuarantineEscalationPolicy(BasePolicy):
    """Blocks deployments globally if too many regions are quarantined."""

    def __init__(
        self, quarantine_system: RegionQuarantineSystem, max_quarantined_regions: int = 2
    ) -> None:
        super().__init__("QuarantineEscalationPolicy")
        self.quarantine_system = quarantine_system
        self.max_quarantined_regions = max_quarantined_regions

    def evaluate(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        context: Dict[str, Any],
    ) -> PolicyEvaluationResult:
        quarantined = self.quarantine_system.get_quarantined_regions()
        if len(quarantined) >= self.max_quarantined_regions:
            return PolicyEvaluationResult(
                policy_name=self.name,
                passed=False,
                decision=GovernanceDecision.BLOCK,
                message=f"Global freeze escalated: too many regions are quarantined ({list(quarantined)}). Rollout blocked.",
            )

        return PolicyEvaluationResult(
            policy_name=self.name,
            passed=True,
            decision=GovernanceDecision.ALLOW,
            message="Quarantine count within safety limits.",
        )


class UnsafeRecoveryPreventionPolicy(BasePolicy):
    """Prevents recovery plan executions if the active risk level is CRITICAL."""

    def __init__(self) -> None:
        super().__init__("UnsafeRecoveryPreventionPolicy")

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
                message="Operational recovery plan executions are BLOCKED because risk level is CRITICAL.",
            )

        return PolicyEvaluationResult(
            policy_name=self.name,
            passed=True,
            decision=GovernanceDecision.ALLOW,
            message="Recovery execution permitted under standard risk guidelines.",
        )


class RecoveryRetryCeilingPolicy(BasePolicy):
    """Blocks rollout progression if the recovery retry count exceeds the limit.

    .. note::
        The ``recovery_attempts`` count is derived from the audit trail and is
        only populated by :class:`GovernanceCoordinator` when an
        ``audit_logger`` is supplied to the deployment. Without an audit
        logger this policy reads a count of 0 and therefore never triggers.
    """

    def __init__(self, max_recovery_attempts: int = 3) -> None:
        super().__init__("RecoveryRetryCeilingPolicy")
        self.max_recovery_attempts = max_recovery_attempts

    def evaluate(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        context: Dict[str, Any],
    ) -> PolicyEvaluationResult:
        recovery_attempts = context.get("recovery_attempts", 0)
        if recovery_attempts >= self.max_recovery_attempts:
            return PolicyEvaluationResult(
                policy_name=self.name,
                passed=False,
                decision=GovernanceDecision.BLOCK,
                message=f"Rollout blocked: recovery attempts ceiling reached ({recovery_attempts} >= {self.max_recovery_attempts}).",
            )

        return PolicyEvaluationResult(
            policy_name=self.name,
            passed=True,
            decision=GovernanceDecision.ALLOW,
            message="Recovery attempts within safety limits.",
        )


class RollbackStormPreventionPolicy(BasePolicy):
    """Blocks automatic rollbacks if a cascade of rollbacks is detected in a short time frame.

    .. note::
        The ``recent_rollbacks`` count is derived from the audit trail and is
        only populated by :class:`GovernanceCoordinator` when an
        ``audit_logger`` is supplied to the deployment. Without an audit
        logger this policy reads a count of 0 and therefore never triggers.
    """

    def __init__(self, max_recent_rollbacks: int = 3) -> None:
        super().__init__("RollbackStormPreventionPolicy")
        self.max_recent_rollbacks = max_recent_rollbacks

    def evaluate(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        context: Dict[str, Any],
    ) -> PolicyEvaluationResult:
        recent_rollbacks = context.get("recent_rollbacks", 0)
        if recent_rollbacks >= self.max_recent_rollbacks:
            return PolicyEvaluationResult(
                policy_name=self.name,
                passed=False,
                decision=GovernanceDecision.BLOCK,
                message=f"Rollback Storm Detected: Automatic rollback BLOCKED. Recent rollbacks ({recent_rollbacks}) exceed threshold.",
            )

        return PolicyEvaluationResult(
            policy_name=self.name,
            passed=True,
            decision=GovernanceDecision.ALLOW,
            message="Rollback frequency is within safety limits.",
        )
