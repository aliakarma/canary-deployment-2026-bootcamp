"""
Rollback and consistency validation for the Canary Deployment Simulator.

Provides utilities for serializing, deserializing, validating, and executing
rollbacks on previously deployed states in a simulated server cluster.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import TYPE_CHECKING, Any

from deploy.audit import AuditLogger, DeploymentEvent, DeploymentEventType
from deploy.state import DeploymentState, DeploymentStatus, StageResult
from logging_config import get_logger

if TYPE_CHECKING:
    from cluster.state import ClusterState

logger = get_logger(__name__)


class RollbackConsistencyError(Exception):
    """Raised when rollback consistency validation fails.

    Attributes:
        errors: Dictionary mapping server ID to the reason for the mismatch.
    """

    def __init__(self, message: str, errors: dict[str, str]) -> None:
        super().__init__(message)
        self.errors = errors


def deserialize_deployment_state(d: dict[str, Any]) -> DeploymentState:
    """Reconstruct a :class:`DeploymentState` object from a dictionary.

    Args:
        d: Serialized deployment state dictionary.

    Returns:
        Reconstructed :class:`DeploymentState`.
    """
    stages: list[StageResult] = []
    for s_dict in d.get("stages", []):
        started_at_str = s_dict.get("started_at")
        started_at = datetime.fromisoformat(started_at_str) if started_at_str else datetime.now()
        completed_at_str = s_dict.get("completed_at")
        completed_at = datetime.fromisoformat(completed_at_str) if completed_at_str else None
        stage = StageResult(
            stage_index=s_dict.get("stage_index", 0),
            target_percentage=s_dict.get("target_percentage", 0),
            servers_updated=s_dict.get("servers_updated", []),
            servers_total=s_dict.get("servers_total", 0),
            health_check_passed=s_dict.get("health_check_passed"),
            started_at=started_at,
            completed_at=completed_at,
            duration_seconds=s_dict.get("duration_seconds", 0.0),
            error=s_dict.get("error"),
        )
        stages.append(stage)

    started_at_str = d.get("started_at")
    started_at = datetime.fromisoformat(started_at_str) if started_at_str else datetime.now()
    completed_at_str = d.get("completed_at")
    completed_at = datetime.fromisoformat(completed_at_str) if completed_at_str else None

    state = DeploymentState(
        deployment_id=d.get("deployment_id", ""),
        target_version=d.get("target_version", ""),
        source_version=d.get("source_version", ""),
        status=DeploymentStatus(d.get("status", "pending")),
        stages=stages,
        current_stage_index=d.get("current_stage_index", -1),
        total_servers=d.get("total_servers", 0),
        servers_updated=set(d.get("servers_updated", [])),
        servers_pending=set(d.get("servers_pending", [])),
        started_at=started_at,
        completed_at=completed_at,
        error_message=d.get("error_message"),
    )
    return state


def save_deployment_state(state: DeploymentState, filepath: str) -> None:
    """Save the deployment state to a JSON file.

    Args:
        state: The DeploymentState to save.
        filepath: Path to the destination file.
    """
    logger.info("Saving deployment state %s to %s ...", state.deployment_id, filepath)
    try:
        # Ensure target directory exists to prevent FileNotFoundError
        if filepath:
            dirname = os.path.dirname(filepath)
            if dirname:
                os.makedirs(dirname, exist_ok=True)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(state.to_dict(), f, indent=4)
        logger.debug("Successfully saved deployment state %s", state.deployment_id)
    except Exception as exc:
        logger.error("Failed to save deployment state: %s", exc)
        raise


def load_deployment_state(filepath: str) -> DeploymentState:
    """Load and deserialize a deployment state from a JSON file.

    Args:
        filepath: Path to the source JSON file.

    Returns:
        The deserialized :class:`DeploymentState`.
    """
    logger.info("Loading deployment state from %s ...", filepath)
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        state = deserialize_deployment_state(data)
        logger.debug("Successfully loaded deployment state %s", state.deployment_id)
        return state
    except Exception as exc:
        logger.error("Failed to load deployment state: %s", exc)
        raise


def validate_rollback_consistency(
    cluster_state: ClusterState,
    deployment_state: DeploymentState,
) -> dict[str, str]:
    """Validate that the cluster state is consistent with the deployment target.

    Verifies that:
      - All servers listed as updated in the deployment actually exist in the cluster.
      - The servers run the target version they were updated to (detecting manual config drift).

    Args:
        cluster_state: The current ClusterState.
        deployment_state: The target DeploymentState to validate against.

    Returns:
        A dictionary mapping server ID to mismatch reasons. Empty if consistent.
    """
    logger.info(
        "Validating rollback consistency for deployment %s ...", deployment_state.deployment_id
    )
    errors: dict[str, str] = {}

    for server_id in deployment_state.servers_updated:
        server = cluster_state.get_server(server_id)
        if server is None:
            errors[server_id] = "Server not found in cluster state"
            continue

        if server.current_version != deployment_state.target_version:
            errors[server_id] = (
                f"Version mismatch: expected target version '{deployment_state.target_version}', "
                f"but found '{server.current_version}'"
            )

    if errors:
        logger.warning(
            "Rollback consistency validation FAILED: %d/%d updated servers are inconsistent",
            len(errors),
            len(deployment_state.servers_updated),
        )
    else:
        logger.info("Rollback consistency validation PASSED")

    return errors


def rollback(
    cluster_state: ClusterState,
    deployment_state: DeploymentState,
    force: bool = False,
    audit_logger: AuditLogger | None = None,
) -> list[str]:
    """Rollback updated servers in the cluster to their pre-deployment versions.

    Args:
        cluster_state: The ClusterState to update.
        deployment_state: The DeploymentState tracker containing updated server lists.
        force: If True, bypass consistency errors and roll back matching nodes anyway.
        audit_logger: Optional AuditLogger.

    Returns:
        List of successfully rolled back server IDs.

    Raises:
        RollbackConsistencyError: If inconsistencies are found and force is False.
    """
    logger.info(
        "Initiating rollback for deployment %s (target version: %s) ...",
        deployment_state.deployment_id,
        deployment_state.target_version,
    )

    errors = validate_rollback_consistency(cluster_state, deployment_state)
    if errors and not force:
        err_msg = f"Cannot rollback deployment {deployment_state.deployment_id}: consistency check failed."
        raise RollbackConsistencyError(err_msg, errors)

    if audit_logger is not None:
        audit_logger.log(
            DeploymentEvent(
                event_type=DeploymentEventType.ROLLBACK_START,
                deployment_id=deployment_state.deployment_id,
                details={
                    "reason": (
                        "Manual rollback execution"
                        if not force
                        else "Forced manual rollback execution"
                    ),
                    "source_version": deployment_state.source_version,
                    "servers_to_rollback": list(deployment_state.servers_updated),
                },
            )
        )

    deployment_state.mark_rolling_back()
    rolled_back_ids: list[str] = []

    try:
        # Revert each updated server
        for server_id in list(deployment_state.servers_updated):
            # Skip if server is missing and we are in force mode
            server = cluster_state.get_server(server_id)
            if server is None:
                logger.warning("Force rollback: skipping missing server %s", server_id)
                continue

            # Check if rollback can be executed
            if server.current_version == deployment_state.source_version:
                rolled_back_ids.append(server_id)
                continue

            success = cluster_state.rollback_server(server_id)
            if success:
                rolled_back_ids.append(server_id)
            else:
                logger.error("Failed to rollback server %s in cluster state", server_id)

        deployment_state.mark_rolled_back()

        if audit_logger is not None:
            audit_logger.log(
                DeploymentEvent(
                    event_type=DeploymentEventType.ROLLBACK_COMPLETE,
                    deployment_id=deployment_state.deployment_id,
                    details={
                        "servers_rolled_back": rolled_back_ids,
                    },
                )
            )
    except Exception as exc:
        logger.exception("Unexpected error during rollback loop: %s", exc)
        deployment_state.mark_failed(f"Rollback failed: {exc}")
        raise

    logger.info(
        "Rollback for deployment %s COMPLETED. Reverted %d/%d servers to version %s",
        deployment_state.deployment_id,
        len(rolled_back_ids),
        len(deployment_state.servers_updated),
        deployment_state.source_version,
    )

    return rolled_back_ids
