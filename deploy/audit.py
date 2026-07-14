"""
Structured audit logging and event tracking for the Canary Deployment Simulator.

Provides data models and collection mechanisms for concurrency-safe,
JSON-serializable deployment events.
"""

from __future__ import annotations

import datetime
import json
import os
import threading
import uuid
from enum import Enum
from typing import Any, Dict

from logging_config import get_logger

logger = get_logger(__name__)


class DeploymentEventType(str, Enum):
    """Supported event types for canary deployment tracking."""

    DEPLOYMENT_START = "deployment_start"
    STAGE_TRANSITION = "stage_transition"
    HEALTH_CHECK = "health_check"
    ROLLBACK_START = "rollback_start"
    ROLLBACK_COMPLETE = "rollback_complete"
    ROLLBACK_INITIATED = "rollback_initiated"
    ABORT_RECEIVED = "abort_received"
    DEPLOYMENT_COMPLETED = "deployment_completed"
    DEPLOYMENT_FAILED = "deployment_failed"

    # Phase 8 Governance Events
    POLICY_EVALUATION = "policy_evaluation"
    GOVERNANCE_DECISION = "governance_decision"
    APPROVAL_REQUEST = "approval_request"
    APPROVAL_DECISION = "approval_decision"
    RISK_SCORE_TRANSITION = "risk_score_transition"
    POLICY_VIOLATION = "policy_violation"

    # Phase 9 Resilience Events
    SNAPSHOT_CREATE = "snapshot_create"
    SNAPSHOT_RESTORE = "snapshot_restore"
    QUARANTINE_ACTIVATE = "quarantine_activate"
    QUARANTINE_RELEASE = "quarantine_release"
    RECOVERY_PLAN_EXECUTE = "recovery_plan_execute"
    RECOVERY_PLAN_COMPLETE = "recovery_plan_complete"
    EVENT_REPLAY_START = "event_replay_start"


class DeploymentEvent:
    """A single structured deployment event."""

    def __init__(
        self,
        event_type: DeploymentEventType,
        deployment_id: str,
        timestamp: datetime.datetime | None = None,
        details: Dict[str, Any] | None = None,
        event_id: str | None = None,
        correlation_id: str | None = None,
        parent_event_id: str | None = None,
    ) -> None:
        self.event_type = event_type
        self.deployment_id = deployment_id
        self.timestamp = timestamp or datetime.datetime.now(datetime.timezone.utc)
        self.details = details or {}
        self.event_id = event_id or f"evt-{uuid.uuid4().hex[:8]}"
        self.correlation_id = correlation_id or deployment_id
        self.parent_event_id = parent_event_id

    def to_dict(self) -> Dict[str, Any]:
        """Convert the event to a JSON-compatible dictionary."""
        d = {
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type.value,
            "deployment_id": self.deployment_id,
            "correlation_id": self.correlation_id,
            "details": self.details,
        }
        if self.parent_event_id is not None:
            d["parent_event_id"] = self.parent_event_id
        return d

    def __repr__(self) -> str:
        return (
            f"DeploymentEvent(type={self.event_type.value}, "
            f"id={self.deployment_id}, event_id={self.event_id}, time={self.timestamp.isoformat()})"
        )


class AuditLogger:
    """A thread-safe audit log recorder.

    Maintains a list of logged events in memory and optionally appends
    them as JSON lines to a file on disk.
    """

    def __init__(self, file_path: str | None = None) -> None:
        self.file_path = file_path
        self._events: list[DeploymentEvent] = []
        self._lock = threading.Lock()
        # Whether the parent directory has already been ensured, so we only
        # pay the ``os.makedirs`` syscall once rather than on every write.
        self._dir_ready = False

    def log(self, event: DeploymentEvent) -> None:
        """Record a deployment event. Concurrency-safe and exception-safe.

        Acquires a single lock for both in-memory append and file write
        to guarantee ordering consistency between memory and disk under
        concurrent logging pressure.

        Each event is appended (and the handle released immediately) so the
        log file is never held open — callers that rotate or delete the file
        between runs are unaffected.  The directory is created only once.

        Args:
            event: The DeploymentEvent to log.
        """
        with self._lock:
            self._events.append(event)
            if self.file_path:
                try:
                    if not self._dir_ready:
                        dirname = os.path.dirname(self.file_path)
                        if dirname:
                            os.makedirs(dirname, exist_ok=True)
                        self._dir_ready = True
                    with open(self.file_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(event.to_dict()) + "\n")
                except Exception as exc:
                    logger.error(
                        "Exception while writing to audit log file '%s': %s",
                        self.file_path,
                        exc,
                    )

    def close(self) -> None:
        """Release audit resources.

        Provided for lifecycle symmetry; events are flushed to disk on every
        :meth:`log` call, so there is no buffered state to flush here.
        """
        return None

    def get_events(self) -> list[DeploymentEvent]:
        """Retrieve a copy of all recorded events in memory.

        Returns:
            List of recorded DeploymentEvent instances.
        """
        with self._lock:
            return list(self._events)

    def clear(self) -> None:
        """Clear all in-memory events."""
        with self._lock:
            self._events.clear()
