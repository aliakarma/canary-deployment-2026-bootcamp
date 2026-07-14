"""
Deployment state tracking for the Canary Deployment Simulator.

Provides :class:`DeploymentStatus` enum and :class:`DeploymentState`
for tracking the lifecycle and progress of a canary deployment.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class DeploymentStatus(Enum):
    """Lifecycle states for a canary deployment.

    State machine::

        PENDING -> IN_PROGRESS -> COMPLETED
                       |
                       +-------> PAUSED (between stages)
                       |
                       +-------> ROLLING_BACK -> ROLLED_BACK
                       |
                       +-------> ABORTED
                       |
                       +-------> FAILED
    """

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    PAUSED = "paused"
    COMPLETED = "completed"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"
    ABORTED = "aborted"
    FAILED = "failed"


@dataclass
class StageResult:
    """Record of a single rollout stage's execution.

    Attributes:
        stage_index: Zero-based index of this stage.
        target_percentage: The cumulative deployment percentage for this stage.
        servers_updated: List of server IDs updated in this stage.
        servers_total: Total servers in the cluster at stage start.
        health_check_passed: Whether the post-stage health check passed.
        started_at: When the stage began.
        completed_at: When the stage finished (or was aborted).
        duration_seconds: Wall-clock time for this stage.
        error: Optional error message if the stage failed.
    """

    stage_index: int
    target_percentage: int
    servers_updated: list[str] = field(default_factory=list)
    servers_total: int = 0
    health_check_passed: bool | None = None
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime | None = None
    duration_seconds: float = 0.0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dictionary."""
        return {
            "stage_index": self.stage_index,
            "target_percentage": self.target_percentage,
            "servers_updated": self.servers_updated,
            "servers_total": self.servers_total,
            "health_check_passed": self.health_check_passed,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": round(self.duration_seconds, 3),
            "error": self.error,
        }


@dataclass
class DeploymentState:
    """Tracks the full lifecycle of a canary deployment.

    This is the central record used by the deployment engine and
    referenced by the rollback system and logging/auditing modules.

    Attributes:
        deployment_id: Unique identifier for this deployment.
        target_version: The version being deployed.
        source_version: The version being replaced.
        status: Current lifecycle status.
        stages: Ordered list of :class:`StageResult` records.
        current_stage_index: Index of the stage currently executing.
        total_servers: Total servers in the cluster.
        servers_updated: Set of server IDs that have been updated so far.
        servers_pending: Set of server IDs still awaiting update.
        started_at: Deployment start timestamp.
        completed_at: Deployment end timestamp.
        error_message: Human-readable error description if failed/aborted.
    """

    deployment_id: str
    target_version: str
    source_version: str
    status: DeploymentStatus = DeploymentStatus.PENDING
    stages: list[StageResult] = field(default_factory=list)
    current_stage_index: int = -1
    total_servers: int = 0
    servers_updated: set[str] = field(default_factory=set)
    servers_pending: set[str] = field(default_factory=set)
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime | None = None
    error_message: str | None = None

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def progress_percentage(self) -> float:
        """Return current deployment progress as 0.0 – 100.0."""
        if self.total_servers == 0:
            return 0.0
        return (len(self.servers_updated) / self.total_servers) * 100.0

    @property
    def is_active(self) -> bool:
        """Return ``True`` if the deployment is still running."""
        return self.status in (
            DeploymentStatus.PENDING,
            DeploymentStatus.IN_PROGRESS,
            DeploymentStatus.PAUSED,
        )

    @property
    def is_terminal(self) -> bool:
        """Return ``True`` if the deployment has reached a final state."""
        return self.status in (
            DeploymentStatus.COMPLETED,
            DeploymentStatus.ROLLED_BACK,
            DeploymentStatus.ABORTED,
            DeploymentStatus.FAILED,
        )

    @property
    def duration_seconds(self) -> float:
        """Return total elapsed seconds (or time since start if still running)."""
        end = self.completed_at or datetime.now()
        return (end - self.started_at).total_seconds()

    # ------------------------------------------------------------------
    # Mutation helpers
    # ------------------------------------------------------------------

    def mark_completed(self) -> None:
        """Transition to COMPLETED state."""
        self.status = DeploymentStatus.COMPLETED
        self.completed_at = datetime.now()

    def mark_failed(self, error: str) -> None:
        """Transition to FAILED state with an error message."""
        self.status = DeploymentStatus.FAILED
        self.completed_at = datetime.now()
        self.error_message = error

    def mark_aborted(self, reason: str) -> None:
        """Transition to ABORTED state with a reason."""
        self.status = DeploymentStatus.ABORTED
        self.completed_at = datetime.now()
        self.error_message = reason

    def mark_rolling_back(self) -> None:
        """Transition to ROLLING_BACK state."""
        self.status = DeploymentStatus.ROLLING_BACK

    def mark_rolled_back(self) -> None:
        """Transition to ROLLED_BACK state."""
        self.status = DeploymentStatus.ROLLED_BACK
        self.completed_at = datetime.now()

    def to_dict(self) -> dict[str, Any]:
        """Serialise the full deployment state to a dictionary."""
        return {
            "deployment_id": self.deployment_id,
            "target_version": self.target_version,
            "source_version": self.source_version,
            "status": self.status.value,
            "current_stage_index": self.current_stage_index,
            "total_servers": self.total_servers,
            "servers_updated_count": len(self.servers_updated),
            "servers_pending_count": len(self.servers_pending),
            "servers_updated": list(self.servers_updated),
            "servers_pending": list(self.servers_pending),
            "progress_percentage": round(self.progress_percentage, 1),
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": round(self.duration_seconds, 3),
            "error_message": self.error_message,
            "stages": [s.to_dict() for s in self.stages],
        }
