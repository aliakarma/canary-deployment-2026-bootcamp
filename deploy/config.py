"""
Deployment configuration for the Canary Deployment Simulator.

Provides :class:`DeploymentConfig` — a dataclass encapsulating all tuneable
parameters for a canary rollout, including stage percentages, timing
controls, and health-check hooks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

# ---------------------------------------------------------------------------
# Default stage percentages for canary deployment
# ---------------------------------------------------------------------------
# Industry-standard canary progression: 10% → 25% → 50% → 75% → 100%
DEFAULT_STAGES: list[int] = [10, 25, 50, 75, 100]


@dataclass
class DeploymentConfig:
    """Configuration for a single canary deployment run.

    Attributes:
        target_version: The version string to deploy (e.g. ``"2.0.0"``).
        stages: List of cumulative percentage targets for each rollout
            stage.  For example ``[10, 25, 50, 100]`` means the first
            stage updates 10 % of servers, then 25 %, etc.  Values must
            be monotonically increasing and the last must be 100.
        stage_delay_seconds: Seconds to wait between stages for
            observation and health-check analysis.
        health_check_interval: Seconds between health checks within a
            stage (while waiting for ``stage_delay_seconds`` to elapse).
        health_check_fn: Optional callable ``(cluster_state) -> bool``
            invoked after each stage to determine whether the rollout
            should continue.  Return ``True`` to proceed, ``False`` to
            trigger rollback.  When ``None``, the engine assumes all
            stages pass.
        on_stage_complete: Optional callback invoked after each stage
            completes successfully.  Receives ``(stage_index, percentage,
            deployed_count)``.
        abort_event: Optional :class:`threading.Event` that, when set,
            signals the engine to abort the rollout immediately.  This
            integrates with Section 6 (Async ABORT Listener).
        max_retries_per_stage: Number of times a failed health check can
            be retried before the stage is declared failed.
        current_time: Optional fixed timestamp used for governance policy
            evaluation (e.g. restricted-window checks in ``RiskPolicy``).
            When ``None`` the governance coordinator falls back to the
            system clock.  Pinning this makes governance evaluation
            deterministic and independent of when the rollout runs.
    """

    target_version: str
    stages: list[int] = field(default_factory=lambda: list(DEFAULT_STAGES))
    stage_delay_seconds: float = 5.0
    health_check_interval: float = 1.0
    health_check_fn: Callable[..., bool] | None = None
    on_stage_complete: Callable[..., Any] | None = None
    abort_event: Any | None = None  # threading.Event — typed as Any to avoid import
    max_retries_per_stage: int = 1
    audit_logger: Any | None = None  # deploy.audit.AuditLogger
    governance_coordinator: Any | None = None  # governance.coordinator.GovernanceCoordinator
    quarantine_system: Any | None = None  # resilience.quarantine.RegionQuarantineSystem
    current_time: datetime | None = None  # fixed clock for deterministic governance windows

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        """Validate configuration after initialisation."""
        if not self.stages:
            raise ValueError("stages must contain at least one percentage")

        for i, pct in enumerate(self.stages):
            if not (1 <= pct <= 100):
                raise ValueError(f"Stage {i} percentage must be 1-100, got {pct}")
            if i > 0 and pct <= self.stages[i - 1]:
                raise ValueError(
                    f"Stages must be monotonically increasing: "
                    f"stage {i - 1}={self.stages[i - 1]} >= stage {i}={pct}"
                )

        if self.stages[-1] != 100:
            raise ValueError(f"Last stage must be 100%, got {self.stages[-1]}%")

        if self.stage_delay_seconds < 0:
            raise ValueError("stage_delay_seconds must be >= 0")

        if self.health_check_interval < 0:
            raise ValueError("health_check_interval must be >= 0")

        if self.max_retries_per_stage < 0:
            raise ValueError("max_retries_per_stage must be >= 0")

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------

    def __str__(self) -> str:
        stage_str = " -> ".join(f"{p}%" for p in self.stages)
        return (
            f"DeploymentConfig(version={self.target_version}, "
            f"stages=[{stage_str}], "
            f"delay={self.stage_delay_seconds}s)"
        )
