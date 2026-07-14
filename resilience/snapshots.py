"""
Snapshot system for saving and restoring point-in-time states of the cluster and deployment engine.
"""

from __future__ import annotations

import datetime
import json
import os
import uuid
from typing import Any, Dict

from cluster.models import Server, ServerStatus
from cluster.state import ClusterState
from deploy.state import DeploymentState, DeploymentStatus, StageResult
from logging_config import get_logger
from resilience.models import Snapshot

logger = get_logger(__name__)


class ClusterSnapshotSystem:
    """Manages creation, serialization, and exception-safe restoration of cluster/deployment snapshots."""

    def __init__(self, cluster: ClusterState, file_path: str | None = None) -> None:
        self.cluster = cluster
        self.file_path = file_path
        self._snapshots: Dict[str, Snapshot] = {}

    def create_snapshot(
        self,
        deployment: DeploymentState | None = None,
        risk_category: str | None = None,
        risk_score: float | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Snapshot:
        """Capture the current state of the cluster and active deployment."""
        snapshot_id = f"snap-{uuid.uuid4().hex[:8]}"

        # Serialize servers thread-safely
        serialized_servers = []
        for server in self.cluster.servers:
            serialized_servers.append(
                {
                    "id": server.id,
                    "region": server.region,
                    "status": server.status.value,
                    "current_version": server.current_version,
                    "previous_version": server.previous_version,
                    "cpu_usage": server.cpu_usage,
                    "memory_usage": server.memory_usage,
                    "last_health_check": (
                        server.last_health_check.isoformat() if server.last_health_check else None
                    ),
                    "deployment_history": server.deployment_history.copy(),
                }
            )

        # Serialize deployment state if present
        dep_id = None
        dep_status = None
        dep_progress = None
        servers_updated = []
        servers_pending = []
        gov_state = {}

        if deployment is not None:
            dep_id = deployment.deployment_id
            dep_status = deployment.status.value
            dep_progress = deployment.progress_percentage
            servers_updated = list(deployment.servers_updated)
            servers_pending = list(deployment.servers_pending)
            gov_state = {
                "current_stage_index": deployment.current_stage_index,
                "stages": [s.to_dict() for s in deployment.stages],
                "error_message": deployment.error_message,
            }

        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            servers=serialized_servers,
            deployment_id=dep_id,
            deployment_status=dep_status,
            deployment_progress=dep_progress,
            servers_updated=servers_updated,
            servers_pending=servers_pending,
            governance_state=gov_state,
            risk_category=risk_category,
            risk_score=risk_score,
            metadata=metadata or {},
        )

        self._snapshots[snapshot_id] = snapshot

        if self.file_path:
            self.export_snapshot(snapshot, self.file_path)

        logger.info(
            "Created cluster snapshot: %s (servers: %d)", snapshot_id, len(serialized_servers)
        )
        return snapshot

    def restore_snapshot(
        self,
        snapshot: Snapshot,
        deployment: DeploymentState | None = None,
    ) -> bool:
        """Restore the cluster state (and optionally deployment state) from a snapshot.

        This method is exception-safe: if restoration fails, it rolls back to
        the state prior to the restoration call.
        """
        logger.info("Restoring cluster state from snapshot: %s", snapshot.snapshot_id)

        # 1. Take a backup of the current live cluster state
        backup_servers: list[dict[str, Any]] = []
        for s in self.cluster.servers:
            backup_servers.append(
                {
                    "id": s.id,
                    "region": s.region,
                    "status": s.status,
                    "current_version": s.current_version,
                    "previous_version": s.previous_version,
                    "cpu_usage": s.cpu_usage,
                    "memory_usage": s.memory_usage,
                    "last_health_check": s.last_health_check,
                    "deployment_history": s.deployment_history.copy(),
                }
            )

        backup_dep_state: dict[str, Any] | None = None
        if deployment is not None:
            backup_dep_state = {
                "status": deployment.status,
                "current_stage_index": deployment.current_stage_index,
                "servers_updated": set(deployment.servers_updated),
                "servers_pending": set(deployment.servers_pending),
                "error_message": deployment.error_message,
                "stages": list(deployment.stages),
            }

        try:
            # 2. Apply snapshot to live cluster state
            with self.cluster._lock:
                # Build maps of existing servers
                existing_servers = self.cluster._servers

                # Update attributes or re-create
                for snap_s in snapshot.servers:
                    server_id = snap_s["id"]
                    server = existing_servers.get(server_id)
                    if server is None:
                        # Server was deleted in the meantime, recreate it
                        server = Server(
                            id=server_id,
                            hostname=f"node-{snap_s['region']}-{server_id}.internal",
                            ip_address=f"10.0.0.{len(existing_servers)}",
                            region=snap_s["region"],
                            current_version=snap_s["current_version"],
                            status=ServerStatus(snap_s["status"]),
                        )
                        existing_servers[server_id] = server

                    server.status = ServerStatus(snap_s["status"])
                    server.current_version = snap_s["current_version"]
                    server.previous_version = snap_s["previous_version"]
                    server.cpu_usage = snap_s["cpu_usage"]
                    server.memory_usage = snap_s["memory_usage"]
                    lhc = snap_s["last_health_check"]
                    server.last_health_check = datetime.datetime.fromisoformat(lhc) if lhc else None  # type: ignore[assignment]
                    server.deployment_history = snap_s["deployment_history"].copy()

            # 3. Apply snapshot to deployment if provided and IDs match
            if deployment is not None and snapshot.deployment_id == deployment.deployment_id:
                deployment.status = DeploymentStatus(snapshot.deployment_status)
                deployment.current_stage_index = snapshot.governance_state.get(
                    "current_stage_index", -1
                )
                deployment.servers_updated = set(snapshot.servers_updated)
                deployment.servers_pending = set(snapshot.servers_pending)
                deployment.error_message = snapshot.governance_state.get("error_message")

                # Reconstruct stages
                stages = []
                for s_dict in snapshot.governance_state.get("stages", []):
                    stages.append(
                        StageResult(
                            stage_index=s_dict.get("stage_index", 0),
                            target_percentage=s_dict.get("target_percentage", 0),
                            servers_updated=s_dict.get("servers_updated", []),
                            servers_total=s_dict.get("servers_total", 0),
                            health_check_passed=s_dict.get("health_check_passed"),
                            started_at=(
                                datetime.datetime.fromisoformat(s_dict["started_at"])
                                if s_dict.get("started_at")
                                else datetime.datetime.now()
                            ),
                            completed_at=(
                                datetime.datetime.fromisoformat(s_dict["completed_at"])
                                if s_dict.get("completed_at")
                                else None
                            ),
                            duration_seconds=s_dict.get("duration_seconds", 0.0),
                            error=s_dict.get("error"),
                        )
                    )
                deployment.stages = stages

            logger.info("Snapshot restore succeeded: %s", snapshot.snapshot_id)
            return True

        except Exception as exc:
            logger.error(
                "Error during snapshot restore: %s. Initiating transaction rollback...", exc
            )

            # Rollback cluster state to backup
            with self.cluster._lock:
                for b_s in backup_servers:
                    srv = self.cluster._servers.get(b_s["id"])
                    if srv is not None:
                        srv.status = b_s["status"]
                        srv.current_version = b_s["current_version"]
                        srv.previous_version = b_s["previous_version"]
                        srv.cpu_usage = b_s["cpu_usage"]
                        srv.memory_usage = b_s["memory_usage"]
                        srv.last_health_check = b_s["last_health_check"]
                        srv.deployment_history = b_s["deployment_history"]

            # Rollback deployment state
            if deployment is not None and backup_dep_state is not None:
                deployment.status = backup_dep_state["status"]
                deployment.current_stage_index = backup_dep_state["current_stage_index"]
                deployment.servers_updated = backup_dep_state["servers_updated"]
                deployment.servers_pending = backup_dep_state["servers_pending"]
                deployment.error_message = backup_dep_state["error_message"]
                deployment.stages = backup_dep_state["stages"]

            logger.info("Transaction rollback complete. Original state preserved.")
            return False

    def export_snapshot(self, snapshot: Snapshot, filepath: str) -> None:
        """Export snapshot to a JSON file."""
        try:
            dirname = os.path.dirname(filepath)
            if dirname:
                os.makedirs(dirname, exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(snapshot.to_dict(), f, indent=4)
        except Exception as exc:
            logger.error("Failed to export snapshot to %s: %s", filepath, exc)

    def load_snapshot(self, filepath: str) -> Snapshot:
        """Load snapshot from a JSON file."""
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        snapshot = Snapshot(
            snapshot_id=data["snapshot_id"],
            timestamp=datetime.datetime.fromisoformat(data["timestamp"]),
            servers=data["servers"],
            deployment_id=data["deployment_id"],
            deployment_status=data["deployment_status"],
            deployment_progress=data["deployment_progress"],
            servers_updated=data["servers_updated"],
            servers_pending=data["servers_pending"],
            governance_state=data["governance_state"],
            risk_category=data["risk_category"],
            risk_score=data["risk_score"],
            metadata=data.get("metadata", {}),
        )
        self._snapshots[snapshot.snapshot_id] = snapshot
        return snapshot
