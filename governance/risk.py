"""
Deployment Risk Scoring Engine for the Canary Deployment Simulator.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from governance.models import RiskScore

if TYPE_CHECKING:
    from cluster.state import ClusterState
    from deploy.state import DeploymentState


class RiskEngine:
    """Evaluates the risk score and category of a running canary deployment."""

    def __init__(self, critical_regions: set[str] | None = None) -> None:
        """Initialize RiskEngine with optional custom critical regions."""
        self.critical_regions = critical_regions or {"us-east-1", "us-west-2"}

    def calculate_risk(
        self,
        cluster_state: ClusterState,
        deployment_state: DeploymentState,
        retry_count: int = 0,
        previous_failures_count: int = 0,
    ) -> tuple[float, RiskScore]:
        """Calculate numerical risk score (0-100) and map to RiskScore category.

        Risk score increases based on:
          - Number of degraded and failed servers in the cluster
          - Progression percentage of the deployment
          - Involvement of critical regions
          - Retry counts in the current stage
          - Historical system failure events

        Args:
            cluster_state: The current ClusterState.
            deployment_state: The active DeploymentState.
            retry_count: Number of health-check retries in the current stage.
            previous_failures_count: Instability/failure events count.

        Returns:
            Tuple of (numerical_score, RiskScore_enum).
        """
        score = 0.0

        # 1. Degradation and Failures (Max 50 points)
        degraded_count = 0
        failed_count = 0
        for server in cluster_state.servers:
            # Check models.py for server status names: ServerStatus.DEGRADED, ServerStatus.FAILED
            # Standard enums are string-based or enum-based. We'll check the string value to be safe.
            status_val = getattr(server.status, "value", str(server.status)).upper()
            if status_val == "DEGRADED":
                degraded_count += 1
            elif status_val == "FAILED":
                failed_count += 1

        score += degraded_count * 15.0
        score += failed_count * 30.0

        # 2. Progression percentage (Max 30 points)
        # Higher stage progress has a larger blast radius
        progress = getattr(deployment_state, "progress_percentage", 0.0)
        score += (progress / 100.0) * 30.0

        # 3. Critical Region Impact (15 points)
        critical_region_affected = False
        for server_id in deployment_state.servers_updated:
            up_server = cluster_state.get_server(server_id)
            if up_server and up_server.region in self.critical_regions:
                critical_region_affected = True
                break

        if critical_region_affected:
            score += 15.0

        # 4. Retries and Instability (unbounded, will clip at end)
        score += retry_count * 10.0
        score += previous_failures_count * 10.0

        # Clip score to 0.0 - 100.0
        final_score = max(0.0, min(100.0, score))

        # Map to RiskScore
        if final_score <= 25.0:
            category = RiskScore.LOW
        elif final_score <= 50.0:
            category = RiskScore.MEDIUM
        elif final_score <= 75.0:
            category = RiskScore.HIGH
        else:
            category = RiskScore.CRITICAL

        return final_score, category
