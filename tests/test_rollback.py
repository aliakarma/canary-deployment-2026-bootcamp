"""
Unit tests for the rollback system.

Covers: JSON serialization/deserialization, state file saving/loading,
consistency validation (drift detection), manual rollback execution, and
DeploymentEngine integration.
"""

from __future__ import annotations

import os
import tempfile

import pytest

from cluster.generator import generate_cluster
from cluster.models import ServerStatus
from cluster.state import ClusterState
from deploy.config import DeploymentConfig
from deploy.engine import DeploymentEngine
from deploy.rollback import (
    RollbackConsistencyError,
    deserialize_deployment_state,
    load_deployment_state,
    rollback,
    save_deployment_state,
    validate_rollback_consistency,
)
from deploy.state import DeploymentState, DeploymentStatus


class TestRollbackSystem:
    """Comprehensive tests for Phase 5 Rollback System."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=10, seed=42))

    @pytest.fixture
    def completed_deployment(self, cluster_state: ClusterState) -> DeploymentState:
        """Run a simple 2-stage deployment to completion."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        return engine.deploy(config)

    # ------------------------------------------------------------------
    # Serialization / Deserialization
    # ------------------------------------------------------------------

    def test_serialization_deserialization_roundtrip(
        self, completed_deployment: DeploymentState
    ) -> None:
        """to_dict() and deserialize_deployment_state roundtrip yields equivalent objects."""
        d = completed_deployment.to_dict()
        restored = deserialize_deployment_state(d)

        assert restored.deployment_id == completed_deployment.deployment_id
        assert restored.target_version == completed_deployment.target_version
        assert restored.source_version == completed_deployment.source_version
        assert restored.status == completed_deployment.status
        assert restored.total_servers == completed_deployment.total_servers
        assert restored.servers_updated == completed_deployment.servers_updated
        assert restored.servers_pending == completed_deployment.servers_pending
        assert restored.started_at == completed_deployment.started_at
        assert restored.completed_at == completed_deployment.completed_at
        assert restored.error_message == completed_deployment.error_message
        assert len(restored.stages) == len(completed_deployment.stages)

        # Check sub-elements
        for i in range(len(completed_deployment.stages)):
            orig_stage = completed_deployment.stages[i]
            rest_stage = restored.stages[i]
            assert rest_stage.stage_index == orig_stage.stage_index
            assert rest_stage.target_percentage == orig_stage.target_percentage
            assert rest_stage.servers_updated == orig_stage.servers_updated
            assert rest_stage.servers_total == orig_stage.servers_total
            assert rest_stage.health_check_passed == orig_stage.health_check_passed
            assert rest_stage.started_at == orig_stage.started_at
            assert rest_stage.completed_at == orig_stage.completed_at
            assert rest_stage.duration_seconds == round(orig_stage.duration_seconds, 3)

    def test_save_and_load_state_file(self, completed_deployment: DeploymentState) -> None:
        """Verify state can be serialized to disk and loaded back."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "state.json")
            save_deployment_state(completed_deployment, filepath)
            assert os.path.exists(filepath)

            restored = load_deployment_state(filepath)
            assert restored.deployment_id == completed_deployment.deployment_id
            assert restored.servers_updated == completed_deployment.servers_updated

    def test_save_state_creates_directories(self, completed_deployment: DeploymentState) -> None:
        """Verify that save_deployment_state creates nested directories if they do not exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "nested_dir", "deep_dir", "state.json")
            save_deployment_state(completed_deployment, filepath)
            assert os.path.exists(filepath)

            restored = load_deployment_state(filepath)
            assert restored.deployment_id == completed_deployment.deployment_id

    # ------------------------------------------------------------------
    # Consistency Validation
    # ------------------------------------------------------------------

    def test_validate_consistency_passing(
        self, cluster_state: ClusterState, completed_deployment: DeploymentState
    ) -> None:
        """Consistent cluster state returns empty validation report."""
        errors = validate_rollback_consistency(cluster_state, completed_deployment)
        assert not errors

    def test_validate_consistency_server_missing(
        self, cluster_state: ClusterState, completed_deployment: DeploymentState
    ) -> None:
        """Validation detects when an updated server has been deleted from the cluster state."""
        # Mutate the cluster dictionary to delete a server
        server_id = list(completed_deployment.servers_updated)[0]
        with cluster_state._lock:
            del cluster_state._servers[server_id]

        errors = validate_rollback_consistency(cluster_state, completed_deployment)
        assert server_id in errors
        assert "not found" in errors[server_id]

    def test_validate_consistency_version_drift(
        self, cluster_state: ClusterState, completed_deployment: DeploymentState
    ) -> None:
        """Validation detects if a server runs a version other than the target version."""
        server_id = list(completed_deployment.servers_updated)[0]
        # Simulate drift by reverting that node to v1.0.0 manually
        cluster_state.get_server(server_id).current_version = "1.0.0"  # type: ignore[union-attr]

        errors = validate_rollback_consistency(cluster_state, completed_deployment)
        assert server_id in errors
        assert "Version mismatch" in errors[server_id]

    # ------------------------------------------------------------------
    # Rollback Execution
    # ------------------------------------------------------------------

    def test_rollback_success(
        self, cluster_state: ClusterState, completed_deployment: DeploymentState
    ) -> None:
        """Successful rollback restores all nodes to source version and updates deployment status."""
        original_updated_count = len(completed_deployment.servers_updated)
        rolled_back = rollback(cluster_state, completed_deployment)

        assert len(rolled_back) == original_updated_count
        assert completed_deployment.status == DeploymentStatus.ROLLED_BACK

        # Ensure all servers are back to v1.0.0 and healthy
        for sid in rolled_back:
            server = cluster_state.get_server(sid)
            assert server is not None
            assert server.current_version == "1.0.0"
            assert server.status == ServerStatus.HEALTHY

    def test_rollback_fails_on_inconsistency(
        self, cluster_state: ClusterState, completed_deployment: DeploymentState
    ) -> None:
        """Rollback raises RollbackConsistencyError if drift is detected."""
        server_id = list(completed_deployment.servers_updated)[0]
        # Introduce manual drift
        cluster_state.get_server(server_id).current_version = "3.0.0"  # type: ignore[union-attr]

        with pytest.raises(RollbackConsistencyError) as exc_info:
            rollback(cluster_state, completed_deployment)

        assert "consistency check failed" in str(exc_info.value)
        assert server_id in exc_info.value.errors

        # Status of deployment remains unchanged (completed)
        assert completed_deployment.status == DeploymentStatus.COMPLETED

    def test_rollback_force_mode(
        self, cluster_state: ClusterState, completed_deployment: DeploymentState
    ) -> None:
        """Verify rollback runs anyway if force=True, bypassing inconsistencies."""
        server_id_drift = list(completed_deployment.servers_updated)[0]
        server_id_missing = list(completed_deployment.servers_updated)[1]

        # drift
        cluster_state.get_server(server_id_drift).current_version = "3.0.0"  # type: ignore[union-attr]
        # missing
        with cluster_state._lock:
            del cluster_state._servers[server_id_missing]

        # Should execute without throwing, bypassing errors
        rolled_back = rollback(cluster_state, completed_deployment, force=True)

        assert server_id_drift in rolled_back
        assert server_id_missing not in rolled_back
        assert completed_deployment.status == DeploymentStatus.ROLLED_BACK

        # The drifted server was successfully rolled back to its previous version
        assert cluster_state.get_server(server_id_drift).current_version == "1.0.0"  # type: ignore[union-attr]

    # ------------------------------------------------------------------
    # Engine Integration
    # ------------------------------------------------------------------

    def test_engine_rollback_integration(
        self, cluster_state: ClusterState, completed_deployment: DeploymentState
    ) -> None:
        """Verify engine.rollback() delegates and reverts target deployment."""
        engine = DeploymentEngine(cluster_state)
        rolled_back = engine.rollback(completed_deployment)

        assert len(rolled_back) == 10
        assert completed_deployment.status == DeploymentStatus.ROLLED_BACK

    def test_deserialize_deployment_state_missing_keys(self) -> None:
        """Verify that deserialize_deployment_state gracefully defaults missing keys to prevent crashes."""
        # Empty dictionary or minimal values
        minimal_dict = {
            "deployment_id": "dep-999",
            "target_version": "2.0.0",
            "source_version": "1.0.0",
            "status": "completed",
            "started_at": "2026-06-17T22:15:42",
        }
        restored = deserialize_deployment_state(minimal_dict)
        assert restored.deployment_id == "dep-999"
        assert restored.target_version == "2.0.0"
        assert restored.status == DeploymentStatus.COMPLETED
        assert restored.current_stage_index == -1
        assert len(restored.stages) == 0
        assert restored.completed_at is None
