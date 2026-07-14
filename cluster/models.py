"""
Server data model and enumerations for the Canary Deployment Simulator.

This module defines the core data structures used across the entire project:
  - ``ServerStatus`` — enumeration of possible server states
  - ``Server`` — dataclass representing a single server in the cluster
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class ServerStatus(Enum):
    """Represents the operational state of a server.

    Lifecycle::

        HEALTHY ──► UPDATING ──► HEALTHY   (successful deploy)
                        │
                        ▼
                    DEGRADED ──► FAILED     (health check failures)
                        │
                        ▼
                     HEALTHY                (after rollback)
    """

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    UPDATING = "updating"


@dataclass
class Server:
    """Represents a single server in the simulated cluster.

    Attributes:
        id: Unique identifier (e.g. ``"server-017"``).
        hostname: Fully qualified internal hostname.
        ip_address: Private IPv4 address in ``10.0.x.x`` range.
        region: Cloud region the server is deployed in.
        current_version: Currently deployed software version.
        previous_version: Version before the most recent update, or ``None``.
        status: Current operational status.
        cpu_usage: Simulated CPU utilisation (0.0 – 100.0).
        memory_usage: Simulated memory utilisation (0.0 – 100.0).
        last_health_check: Timestamp of the most recent health check.
        deployment_history: Chronological list of version change records.
    """

    id: str
    hostname: str
    ip_address: str
    region: str
    current_version: str
    previous_version: str | None = None
    status: ServerStatus = ServerStatus.HEALTHY
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    last_health_check: datetime = field(default_factory=datetime.now)
    deployment_history: list[dict[str, Any]] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Display helpers
    # ------------------------------------------------------------------

    def __str__(self) -> str:
        return (
            f"Server({self.id}, status={self.status.value}, "
            f"version={self.current_version}, region={self.region})"
        )

    def __repr__(self) -> str:
        return self.__str__()

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def is_healthy(self) -> bool:
        """Return ``True`` if the server is in a healthy state."""
        return self.status == ServerStatus.HEALTHY

    @property
    def is_updatable(self) -> bool:
        """Return ``True`` if the server can accept a new deployment."""
        return self.status in (ServerStatus.HEALTHY, ServerStatus.DEGRADED)

    def to_dict(self) -> dict[str, Any]:
        """Serialise the server to a plain dictionary."""
        return {
            "id": self.id,
            "hostname": self.hostname,
            "ip_address": self.ip_address,
            "region": self.region,
            "current_version": self.current_version,
            "previous_version": self.previous_version,
            "status": self.status.value,
            "cpu_usage": round(self.cpu_usage, 1),
            "memory_usage": round(self.memory_usage, 1),
            "last_health_check": self.last_health_check.isoformat(),
            "deployment_history": self.deployment_history,
        }
