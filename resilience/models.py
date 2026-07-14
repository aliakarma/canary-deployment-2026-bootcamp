"""
Data models and enums for Phase 9 Operational Resilience, Failure Recovery & Observability.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List


class QuarantineStatus(str, Enum):
    """Status of a quarantined region."""

    ACTIVE = "active"
    RELEASED = "released"


@dataclass
class QuarantineState:
    """Represents the quarantine status of a region."""

    region: str
    status: QuarantineStatus
    reason: str
    quarantined_at: datetime.datetime
    released_at: datetime.datetime | None = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "region": self.region,
            "status": self.status.value,
            "reason": self.reason,
            "quarantined_at": self.quarantined_at.isoformat(),
            "released_at": self.released_at.isoformat() if self.released_at else None,
            "metadata": self.metadata,
        }


@dataclass
class Snapshot:
    """Represents a point-in-time snapshot of the deployment infrastructure and runtime state."""

    snapshot_id: str
    timestamp: datetime.datetime
    servers: List[Dict[str, Any]]  # Serialized list of servers
    deployment_id: str | None
    deployment_status: str | None
    deployment_progress: float | None
    servers_updated: List[str]
    servers_pending: List[str]
    governance_state: Dict[str, Any] = field(default_factory=dict)
    risk_category: str | None = None
    risk_score: float | None = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "timestamp": self.timestamp.isoformat(),
            "servers": self.servers,
            "deployment_id": self.deployment_id,
            "deployment_status": self.deployment_status,
            "deployment_progress": self.deployment_progress,
            "servers_updated": self.servers_updated,
            "servers_pending": self.servers_pending,
            "governance_state": self.governance_state,
            "risk_category": self.risk_category,
            "risk_score": self.risk_score,
            "metadata": self.metadata,
        }


class RecoveryPlanStatus(str, Enum):
    """Lifecycle status of an operational recovery plan."""

    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class RecoveryPlan:
    """Represents an active operational plan to recover from a rollout failure or abort."""

    plan_id: str
    deployment_id: str
    strategy: str  # "staged_recovery", "region_quarantine", "partial_rollback", "safe_resume"
    status: RecoveryPlanStatus
    target_region: str | None
    steps: List[Dict[str, Any]]  # List of recovery steps
    current_step_index: int = 0
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    completed_at: datetime.datetime | None = None
    error_message: str | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "deployment_id": self.deployment_id,
            "strategy": self.strategy,
            "status": self.status.value,
            "target_region": self.target_region,
            "steps": self.steps,
            "current_step_index": self.current_step_index,
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
        }
