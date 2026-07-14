"""
Unit tests for the cluster simulation module.

Covers: models, generator, state management, and inspector.
"""

from __future__ import annotations

import pytest

from cluster.generator import DEFAULT_MAX_SERVERS, DEFAULT_MIN_SERVERS, generate_cluster
from cluster.inspector import inspect_cluster
from cluster.models import Server, ServerStatus
from cluster.state import ClusterState

# ======================================================================
# Models
# ======================================================================


class TestServerModel:
    """Tests for the Server dataclass and ServerStatus enum."""

    def test_server_status_values(self) -> None:
        """All expected status values exist."""
        assert ServerStatus.HEALTHY.value == "healthy"
        assert ServerStatus.DEGRADED.value == "degraded"
        assert ServerStatus.FAILED.value == "failed"
        assert ServerStatus.UPDATING.value == "updating"

    def test_server_defaults(self) -> None:
        """A freshly created server has sensible defaults."""
        server = Server(
            id="test-001",
            hostname="node-test-001.internal",
            ip_address="10.0.0.1",
            region="us-east-1",
            current_version="1.0.0",
        )
        assert server.status == ServerStatus.HEALTHY
        assert server.previous_version is None
        assert server.deployment_history == []
        assert server.is_healthy is True
        assert server.is_updatable is True

    def test_server_is_healthy_property(self) -> None:
        """``is_healthy`` reflects the current status."""
        server = Server(
            id="test-001",
            hostname="h",
            ip_address="10.0.0.1",
            region="us-east-1",
            current_version="1.0.0",
            status=ServerStatus.FAILED,
        )
        assert server.is_healthy is False

    def test_server_is_updatable(self) -> None:
        """``is_updatable`` allows HEALTHY and DEGRADED servers."""
        server = Server(
            id="test-001",
            hostname="h",
            ip_address="10.0.0.1",
            region="us-east-1",
            current_version="1.0.0",
        )
        assert server.is_updatable is True

        server.status = ServerStatus.DEGRADED
        assert server.is_updatable is True

        server.status = ServerStatus.UPDATING
        assert server.is_updatable is False

        server.status = ServerStatus.FAILED
        assert server.is_updatable is False

    def test_server_to_dict(self) -> None:
        """``to_dict()`` returns a serialisable dictionary."""
        server = Server(
            id="test-001",
            hostname="node-test-001.internal",
            ip_address="10.0.0.1",
            region="us-east-1",
            current_version="1.0.0",
        )
        d = server.to_dict()
        assert d["id"] == "test-001"
        assert d["status"] == "healthy"
        assert d["current_version"] == "1.0.0"
        assert isinstance(d["last_health_check"], str)

    def test_server_str_repr(self) -> None:
        """String representation includes key fields."""
        server = Server(
            id="test-001",
            hostname="h",
            ip_address="10.0.0.1",
            region="us-east-1",
            current_version="1.0.0",
        )
        s = str(server)
        assert "test-001" in s
        assert "healthy" in s
        assert "1.0.0" in s


# ======================================================================
# Generator
# ======================================================================


class TestClusterGenerator:
    """Tests for cluster generation."""

    def test_generate_cluster_default_size(self) -> None:
        """Without an explicit size, cluster has 20–50 servers."""
        servers = generate_cluster()
        assert DEFAULT_MIN_SERVERS <= len(servers) <= DEFAULT_MAX_SERVERS

    def test_generate_cluster_fixed_size(self) -> None:
        """Explicit size produces exactly that many servers."""
        servers = generate_cluster(size=25)
        assert len(servers) == 25

    def test_generate_cluster_size_one(self) -> None:
        """Edge case: a single-server cluster."""
        servers = generate_cluster(size=1)
        assert len(servers) == 1

    def test_generate_cluster_invalid_size(self) -> None:
        """Size < 1 raises ValueError."""
        with pytest.raises(ValueError, match="must be ≥ 1"):
            generate_cluster(size=0)

    def test_server_initial_state(self) -> None:
        """All generated servers start HEALTHY at v1.0.0."""
        servers = generate_cluster(size=30, seed=42)
        for server in servers:
            assert server.status == ServerStatus.HEALTHY
            assert server.current_version == "1.0.0"
            assert server.previous_version is None

    def test_unique_ids(self) -> None:
        """Every server has a unique ID."""
        servers = generate_cluster(size=50, seed=42)
        ids = [s.id for s in servers]
        assert len(ids) == len(set(ids))

    def test_region_distribution(self) -> None:
        """Servers are distributed across multiple regions."""
        servers = generate_cluster(size=40, seed=42)
        regions = {s.region for s in servers}
        assert len(regions) >= 2

    def test_reproducible_with_seed(self) -> None:
        """Same seed produces identical clusters."""
        cluster_a = generate_cluster(size=20, seed=123)
        cluster_b = generate_cluster(size=20, seed=123)
        for a, b in zip(cluster_a, cluster_b):
            assert a.id == b.id
            assert a.hostname == b.hostname
            assert a.region == b.region
            assert a.cpu_usage == b.cpu_usage

    def test_deployment_history_initial(self) -> None:
        """Each server starts with one 'initial_deployment' entry."""
        servers = generate_cluster(size=5, seed=1)
        for s in servers:
            assert len(s.deployment_history) == 1
            assert s.deployment_history[0]["action"] == "initial_deployment"


# ======================================================================
# State Management
# ======================================================================


class TestClusterState:
    """Tests for ClusterState manager."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        """Provide a fresh ClusterState with 10 servers."""
        servers = generate_cluster(size=10, seed=42)
        return ClusterState(servers)

    def test_size(self, cluster_state: ClusterState) -> None:
        """Size property returns correct count."""
        assert cluster_state.size == 10

    def test_get_server_found(self, cluster_state: ClusterState) -> None:
        """Retrieving an existing server returns it."""
        server = cluster_state.get_server("server-001")
        assert server is not None
        assert server.id == "server-001"

    def test_get_server_not_found(self, cluster_state: ClusterState) -> None:
        """Retrieving a non-existent server returns None."""
        assert cluster_state.get_server("server-999") is None

    def test_get_servers_by_status(self, cluster_state: ClusterState) -> None:
        """Filtering by status returns correct subset."""
        healthy = cluster_state.get_servers_by_status(ServerStatus.HEALTHY)
        assert len(healthy) == 10  # all start healthy

        # Change one server's status
        cluster_state.update_server_status("server-001", ServerStatus.FAILED)
        healthy = cluster_state.get_servers_by_status(ServerStatus.HEALTHY)
        failed = cluster_state.get_servers_by_status(ServerStatus.FAILED)
        assert len(healthy) == 9
        assert len(failed) == 1

    def test_get_servers_by_version(self, cluster_state: ClusterState) -> None:
        """Filtering by version returns correct subset."""
        v1 = cluster_state.get_servers_by_version("1.0.0")
        assert len(v1) == 10

        cluster_state.update_server_version("server-001", "2.0.0")
        v1 = cluster_state.get_servers_by_version("1.0.0")
        v2 = cluster_state.get_servers_by_version("2.0.0")
        assert len(v1) == 9
        assert len(v2) == 1

    def test_update_server_status(self, cluster_state: ClusterState) -> None:
        """Status transitions work correctly."""
        assert cluster_state.update_server_status("server-001", ServerStatus.UPDATING) is True
        server = cluster_state.get_server("server-001")
        assert server is not None
        assert server.status == ServerStatus.UPDATING

    def test_update_server_status_not_found(self, cluster_state: ClusterState) -> None:
        """Updating a non-existent server returns False."""
        assert cluster_state.update_server_status("server-999", ServerStatus.FAILED) is False

    def test_update_server_version(self, cluster_state: ClusterState) -> None:
        """Version update saves previous version and appends history."""
        assert cluster_state.update_server_version("server-001", "2.0.0") is True
        server = cluster_state.get_server("server-001")
        assert server is not None
        assert server.current_version == "2.0.0"
        assert server.previous_version == "1.0.0"
        assert server.status == ServerStatus.UPDATING
        # Initial deploy + new deploy = 2 entries
        assert len(server.deployment_history) == 2
        assert server.deployment_history[-1]["action"] == "deploy"

    def test_update_server_version_not_found(self, cluster_state: ClusterState) -> None:
        """Updating version on a non-existent server returns False."""
        assert cluster_state.update_server_version("server-999", "2.0.0") is False

    def test_rollback_server(self, cluster_state: ClusterState) -> None:
        """Rollback restores previous version and sets HEALTHY status."""
        cluster_state.update_server_version("server-001", "2.0.0")
        assert cluster_state.rollback_server("server-001") is True

        server = cluster_state.get_server("server-001")
        assert server is not None
        assert server.current_version == "1.0.0"
        assert server.previous_version == "2.0.0"
        assert server.status == ServerStatus.HEALTHY
        assert server.deployment_history[-1]["action"] == "rollback"

    def test_rollback_no_previous_version(self, cluster_state: ClusterState) -> None:
        """Rollback fails when there is no previous version."""
        # server-001 starts with previous_version=None
        assert cluster_state.rollback_server("server-001") is False

    def test_rollback_not_found(self, cluster_state: ClusterState) -> None:
        """Rollback on a non-existent server returns False."""
        assert cluster_state.rollback_server("server-999") is False

    def test_deployment_summary(self, cluster_state: ClusterState) -> None:
        """Summary correctly counts versions and statuses."""
        summary = cluster_state.get_deployment_summary()
        assert summary["versions"]["1.0.0"] == 10
        assert summary["statuses"]["healthy"] == 10

        cluster_state.update_server_version("server-001", "2.0.0")
        summary = cluster_state.get_deployment_summary()
        assert summary["versions"]["1.0.0"] == 9
        assert summary["versions"]["2.0.0"] == 1
        assert summary["statuses"]["updating"] == 1

    def test_get_servers_by_region(self, cluster_state: ClusterState) -> None:
        """Filtering by region works."""
        all_servers = cluster_state.servers
        regions = {s.region for s in all_servers}
        for region in regions:
            by_region = cluster_state.get_servers_by_region(region)
            expected = [s for s in all_servers if s.region == region]
            assert len(by_region) == len(expected)

    def test_servers_snapshot(self, cluster_state: ClusterState) -> None:
        """``servers`` property returns a list copy, not the internal store."""
        snapshot = cluster_state.servers
        assert isinstance(snapshot, list)
        assert len(snapshot) == 10


# ======================================================================
# Inspector
# ======================================================================


class TestClusterInspector:
    """Tests for the cluster inspection utility."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        servers = generate_cluster(size=20, seed=42)
        return ClusterState(servers)

    def test_inspect_cluster_returns_string(self, cluster_state: ClusterState) -> None:
        """Inspector returns a non-empty formatted string."""
        report = inspect_cluster(cluster_state)
        assert isinstance(report, str)
        assert len(report) > 0

    def test_inspect_cluster_contains_key_sections(self, cluster_state: ClusterState) -> None:
        """Report contains expected section headers."""
        report = inspect_cluster(cluster_state)
        assert "CLUSTER STATUS REPORT" in report
        assert "Version Distribution" in report
        assert "Region Breakdown" in report

    def test_inspect_cluster_verbose(self, cluster_state: ClusterState) -> None:
        """Verbose mode includes per-server details."""
        report = inspect_cluster(cluster_state, verbose=True)
        assert "server-001" in report

    def test_inspect_cluster_shows_server_count(self, cluster_state: ClusterState) -> None:
        """Report shows the total server count."""
        report = inspect_cluster(cluster_state)
        assert "20" in report

    def test_inspect_cluster_mixed_versions(self, cluster_state: ClusterState) -> None:
        """Report correctly shows multiple versions after updates."""
        cluster_state.update_server_version("server-001", "2.0.0")
        cluster_state.update_server_status("server-001", ServerStatus.HEALTHY)
        report = inspect_cluster(cluster_state)
        assert "1.0.0" in report
        assert "2.0.0" in report
