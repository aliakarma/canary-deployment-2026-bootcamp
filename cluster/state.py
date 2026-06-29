"""
Cluster state management for the Canary Deployment Simulator.

Provides :class:`ClusterState` — a thread-safe manager that tracks servers,
handles version transitions, and records deployment history.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from datetime import datetime
from typing import Any

from cluster.models import Server, ServerStatus
from logging_config import get_logger

logger = get_logger(__name__)


class ClusterState:
    """Thread-safe manager for a cluster of servers.

    All mutation methods acquire an internal lock, making this class safe to
    use from the deployment engine and the async abort listener concurrently.

    Args:
        servers: Initial list of :class:`Server` instances.
    """

    def __init__(self, servers: list[Server]) -> None:
        self._lock = threading.Lock()
        self._servers: OrderedDict[str, Server] = OrderedDict()
        for server in servers:
            self._servers[server.id] = server
        logger.debug("ClusterState initialised with %d servers", len(self._servers))

    # ------------------------------------------------------------------
    # Read operations (thread-safe)
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        """Return the total number of servers in the cluster."""
        with self._lock:
            return len(self._servers)

    @property
    def servers(self) -> list[Server]:
        """Return a snapshot list of all servers.

        .. note::
            The returned list is a fresh copy, but the :class:`Server`
            objects inside it are the live, shared instances.  Callers
            must not mutate server fields directly; use the thread-safe
            ``update_*`` / ``rollback_server`` / ``override_version``
            methods instead, which acquire the internal lock.
        """
        with self._lock:
            return list(self._servers.values())

    def get_server(self, server_id: str) -> Server | None:
        """Retrieve a server by its ID, or ``None`` if not found.

        .. note::
            Returns the live, shared :class:`Server` instance. Treat it as
            read-only — mutate state through the thread-safe ``update_*`` /
            ``rollback_server`` / ``override_version`` methods.
        """
        with self._lock:
            return self._servers.get(server_id)

    def get_servers_by_status(self, status: ServerStatus) -> list[Server]:
        """Return all servers matching the given status."""
        with self._lock:
            return [s for s in self._servers.values() if s.status == status]

    def get_servers_by_version(self, version: str) -> list[Server]:
        """Return all servers running the given version."""
        with self._lock:
            return [s for s in self._servers.values() if s.current_version == version]

    def get_servers_by_region(self, region: str) -> list[Server]:
        """Return all servers in the given region."""
        with self._lock:
            return [s for s in self._servers.values() if s.region == region]

    def get_deployment_summary(self) -> dict[str, Any]:
        """Return a summary of the current cluster state.

        Returns:
            A dict with ``"versions"`` mapping version → count and
            ``"statuses"`` mapping status value → count.
        """
        with self._lock:
            versions: dict[str, int] = {}
            statuses: dict[str, int] = {}
            for s in self._servers.values():
                versions[s.current_version] = versions.get(s.current_version, 0) + 1
                statuses[s.status.value] = statuses.get(s.status.value, 0) + 1
            return {"versions": versions, "statuses": statuses}

    # ------------------------------------------------------------------
    # Write operations (thread-safe)
    # ------------------------------------------------------------------

    def update_server_status(self, server_id: str, new_status: ServerStatus) -> bool:
        """Transition a server's status.

        Args:
            server_id: ID of the server to update.
            new_status: The target :class:`ServerStatus`.

        Returns:
            ``True`` if the update succeeded, ``False`` if the server was
            not found.
        """
        with self._lock:
            server = self._servers.get(server_id)
            if server is None:
                logger.warning("Cannot update status: server '%s' not found", server_id)
                return False

            old_status = server.status
            server.status = new_status
            server.last_health_check = datetime.now()

            logger.info(
                "Server %s status: %s → %s",
                server_id,
                old_status.value,
                new_status.value,
            )
            return True

    def update_server_resources(
        self,
        server_id: str,
        cpu_usage: float,
        memory_usage: float,
    ) -> bool:
        """Update a server's simulated resource utilization metrics.

        Args:
            server_id: ID of the server to update.
            cpu_usage: The new CPU usage percentage.
            memory_usage: The new memory usage percentage.

        Returns:
            ``True`` if the update succeeded, ``False`` if the server was
            not found.
        """
        with self._lock:
            server = self._servers.get(server_id)
            if server is None:
                logger.warning("Cannot update resources: server '%s' not found", server_id)
                return False

            old_cpu = server.cpu_usage
            old_mem = server.memory_usage
            server.cpu_usage = max(0.0, min(100.0, cpu_usage))
            server.memory_usage = max(0.0, min(100.0, memory_usage))

            logger.debug(
                "Server %s resources: CPU %.1f%% → %.1f%%, MEM %.1f%% → %.1f%%",
                server_id,
                old_cpu,
                server.cpu_usage,
                old_mem,
                server.memory_usage,
            )
            return True

    def update_server_version(
        self,
        server_id: str,
        new_version: str,
    ) -> bool:
        """Deploy a new version to a server.

        Saves the current version as ``previous_version``, records the
        change in ``deployment_history``, and sets status to ``UPDATING``.

        Args:
            server_id: ID of the server to update.
            new_version: The version string to deploy.

        Returns:
            ``True`` if the update succeeded, ``False`` if the server was
            not found.
        """
        with self._lock:
            server = self._servers.get(server_id)
            if server is None:
                logger.warning("Cannot update version: server '%s' not found", server_id)
                return False

            old_version = server.current_version
            server.previous_version = old_version
            server.current_version = new_version
            server.status = ServerStatus.UPDATING
            server.last_health_check = datetime.now()

            server.deployment_history.append(
                {
                    "version": new_version,
                    "previous_version": old_version,
                    "timestamp": datetime.now().isoformat(),
                    "action": "deploy",
                }
            )

            logger.info(
                "Server %s version: %s → %s",
                server_id,
                old_version,
                new_version,
            )
            return True

    def override_version(self, server_id: str, new_version: str) -> bool:
        """Force a server's current version without recording deployment history.

        This is a thread-safe simulation/diagnostic helper used to model
        out-of-band configuration drift (a version applied outside the
        deployment engine).  Unlike :meth:`update_server_version`, it does
        not touch ``previous_version`` or ``deployment_history`` and does
        not change the server status.

        Args:
            server_id: ID of the server to override.
            new_version: The version string to set as current.

        Returns:
            ``True`` if the override succeeded, ``False`` if the server was
            not found.
        """
        with self._lock:
            server = self._servers.get(server_id)
            if server is None:
                logger.warning("Cannot override version: server '%s' not found", server_id)
                return False

            old_version = server.current_version
            server.current_version = new_version
            logger.info(
                "Server %s version OVERRIDDEN (drift): %s → %s",
                server_id,
                old_version,
                new_version,
            )
            return True

    def rollback_server(self, server_id: str) -> bool:
        """Roll a server back to its previous version.

        Restores ``previous_version`` as the current version and sets
        the server status to ``HEALTHY``.

        Args:
            server_id: ID of the server to roll back.

        Returns:
            ``True`` if rollback succeeded, ``False`` if the server was
            not found or has no previous version.
        """
        with self._lock:
            server = self._servers.get(server_id)
            if server is None:
                logger.warning("Cannot rollback: server '%s' not found", server_id)
                return False
            if server.previous_version is None:
                logger.warning(
                    "Cannot rollback server '%s': no previous version recorded",
                    server_id,
                )
                return False

            if (
                server.deployment_history
                and server.deployment_history[-1].get("action") == "rollback"
            ):
                logger.debug("Server %s is already rolled back (idempotent no-op)", server_id)
                return True

            rolled_back_from = server.current_version
            server.current_version = server.previous_version
            server.previous_version = rolled_back_from
            server.status = ServerStatus.HEALTHY
            server.last_health_check = datetime.now()

            server.deployment_history.append(
                {
                    "version": server.current_version,
                    "previous_version": rolled_back_from,
                    "timestamp": datetime.now().isoformat(),
                    "action": "rollback",
                }
            )

            logger.info(
                "Server %s rolled back: %s → %s",
                server_id,
                rolled_back_from,
                server.current_version,
            )
            return True
