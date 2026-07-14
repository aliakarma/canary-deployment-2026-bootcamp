"""
Unit tests for the health analysis module.

Covers: HealthThresholds validation, metric simulation, evaluate_server_health,
inject_failures logic, cluster-wide analyze, and full deployment integration
with health checks and failure injection.
"""

from __future__ import annotations

import pytest

from cluster.generator import generate_cluster
from cluster.models import Server, ServerStatus
from cluster.state import ClusterState
from deploy.config import DeploymentConfig
from deploy.engine import DeploymentEngine
from deploy.state import DeploymentStatus
from health.analyzer import analyze, create_health_check_fn
from health.failure_injection import inject_failures
from health.metrics import evaluate_server_health, simulate_server_metrics
from health.thresholds import HealthThresholds

# ======================================================================
# HealthThresholds Tests
# ======================================================================


class TestHealthThresholds:
    """Tests for HealthThresholds initialization and validation."""

    def test_default_thresholds(self) -> None:
        """Verify default values are correct and realistic."""
        t = HealthThresholds()
        assert t.max_server_cpu_usage == 85.0
        assert t.max_server_memory_usage == 90.0
        assert t.max_degraded_server_percentage == 10.0
        assert t.max_failed_server_percentage == 0.0
        assert t.max_cluster_cpu_usage_avg == 75.0
        assert t.max_cluster_memory_usage_avg == 80.0
        assert t.max_server_error_rate == 5.0
        assert t.max_server_latency_ms == 500.0

    def test_negative_values_rejected(self) -> None:
        """Verify negative thresholds trigger ValueError."""
        with pytest.raises(ValueError, match="must be non-negative"):
            HealthThresholds(max_server_cpu_usage=-1.0)

    def test_percentage_exceeding_100_rejected(self) -> None:
        """Verify percentages above 100 trigger ValueError."""
        with pytest.raises(ValueError, match="cannot exceed 100"):
            HealthThresholds(max_server_cpu_usage=105.0)

    def test_latency_above_100_accepted(self) -> None:
        """Verify latency is not constrained to <= 100."""
        t = HealthThresholds(max_server_latency_ms=1200.0)
        assert t.max_server_latency_ms == 1200.0

    def test_invalid_types_rejected(self) -> None:
        """Verify non-numeric values trigger TypeError."""
        with pytest.raises(TypeError, match="must be numeric"):
            HealthThresholds(max_server_cpu_usage="high")  # type: ignore[arg-type]


# ======================================================================
# Metrics Simulation & Server Evaluation Tests
# ======================================================================


class TestMetricsAndServerEvaluation:
    """Tests for simulated metrics and individual server health checks."""

    @pytest.fixture
    def server(self) -> Server:
        return Server(
            id="server-001",
            hostname="node-use1-001.internal",
            ip_address="10.0.0.1",
            region="us-east-1",
            current_version="1.0.0",
        )

    def test_simulate_metrics_healthy_status(self, server: Server) -> None:
        """Healthy server produces low error rate and latency."""
        server.status = ServerStatus.HEALTHY
        err, lat = simulate_server_metrics(server, seed=42)
        assert 0.0 <= err <= 1.5
        assert 40.0 <= lat <= 150.0

    def test_simulate_metrics_degraded_status(self, server: Server) -> None:
        """Degraded server produces elevated metrics."""
        server.status = ServerStatus.DEGRADED
        err, lat = simulate_server_metrics(server, seed=42)
        assert 5.0 <= err <= 20.0
        assert 300.0 <= lat <= 850.0

    def test_simulate_metrics_failed_status(self, server: Server) -> None:
        """Failed server produces maximum error rate and latency."""
        server.status = ServerStatus.FAILED
        err, lat = simulate_server_metrics(server, seed=42)
        assert err == 100.0
        assert lat == 5000.0

    def test_evaluate_health_passing(self, server: Server) -> None:
        """Healthy server with default resource usage passes."""
        server.cpu_usage = 45.0
        server.memory_usage = 60.0
        report = evaluate_server_health(server, HealthThresholds())
        assert report.passed is True
        assert not report.failed_metrics

    def test_evaluate_health_failing_cpu(self, server: Server) -> None:
        """Server fails check when CPU usage exceeds threshold."""
        server.cpu_usage = 90.0
        report = evaluate_server_health(server, HealthThresholds(max_server_cpu_usage=85.0))
        assert report.passed is False
        assert "cpu_usage" in report.failed_metrics

    def test_evaluate_health_failing_memory(self, server: Server) -> None:
        """Server fails check when memory usage exceeds threshold."""
        server.memory_usage = 95.0
        report = evaluate_server_health(server, HealthThresholds(max_server_memory_usage=90.0))
        assert report.passed is False
        assert "memory_usage" in report.failed_metrics

    def test_custom_server_id_stable_hashing(self) -> None:
        """Verify that server metric simulation works deterministically with custom server ID formats."""
        custom_server1 = Server(
            id="web-prod-k8s-pod-abc",
            hostname="web-prod-k8s-pod-abc.internal",
            ip_address="10.0.0.99",
            region="us-east-1",
            current_version="1.0.0",
        )
        custom_server2 = Server(
            id="web-prod-k8s-pod-abc",
            hostname="web-prod-k8s-pod-abc.internal",
            ip_address="10.0.0.99",
            region="us-east-1",
            current_version="1.0.0",
        )
        # Verify simulated metrics are identical and deterministic
        err1, lat1 = simulate_server_metrics(custom_server1, seed=42)
        err2, lat2 = simulate_server_metrics(custom_server2, seed=42)
        assert err1 == err2
        assert lat1 == lat2


# ======================================================================
# Failure Injection Tests
# ======================================================================


class TestFailureInjection:
    """Tests for the inject_failures utility."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=10, seed=100))

    def test_inject_failures_degrade(self, cluster_state: ClusterState) -> None:
        """Injecting degrade failures updates status and resource usage."""
        affected = inject_failures(
            cluster_state,
            failure_rate=0.3,
            failure_type="degrade",
            seed=42,
        )
        assert len(affected) == 3  # ceil(10 * 0.3) = 3
        for sid in affected:
            server = cluster_state.get_server(sid)
            assert server is not None
            assert server.status == ServerStatus.DEGRADED
            assert server.cpu_usage > 85.0
            assert server.memory_usage > 80.0

    def test_inject_failures_fail(self, cluster_state: ClusterState) -> None:
        """Injecting fail failures updates status and resource usage."""
        affected = inject_failures(
            cluster_state,
            failure_rate=0.2,
            failure_type="fail",
            seed=42,
        )
        assert len(affected) == 2  # ceil(10 * 0.2) = 2
        for sid in affected:
            server = cluster_state.get_server(sid)
            assert server is not None
            assert server.status == ServerStatus.FAILED
            assert server.cpu_usage > 95.0
            assert server.memory_usage > 90.0

    def test_inject_failures_resource_spike_only(self, cluster_state: ClusterState) -> None:
        """Injecting resource spikes leaves status healthy but spikes metrics."""
        affected = inject_failures(
            cluster_state,
            failure_rate=0.2,
            failure_type="resource_spike",
            seed=42,
        )
        assert len(affected) == 2
        for sid in affected:
            server = cluster_state.get_server(sid)
            assert server is not None
            assert server.status == ServerStatus.HEALTHY
            assert server.cpu_usage > 85.0
            assert server.memory_usage > 90.0

    def test_inject_failures_targeting_version(self, cluster_state: ClusterState) -> None:
        """Injecting failures targeting a specific version only affects those servers."""
        # Set some servers to v2.0.0
        cluster_state.update_server_version("server-001", "2.0.0")
        cluster_state.update_server_version("server-002", "2.0.0")

        affected = inject_failures(
            cluster_state,
            target_version="2.0.0",
            failure_rate=1.0,
            failure_type="degrade",
            seed=42,
        )
        assert sorted(affected) == ["server-001", "server-002"]


# ======================================================================
# Cluster Analyzer Tests
# ======================================================================


class TestClusterAnalyzer:
    """Tests for cluster-wide health report analysis."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=10, seed=200))

    def test_analyze_healthy_cluster(self, cluster_state: ClusterState) -> None:
        """Healthy cluster passes checks with good averages."""
        report = analyze(cluster_state, HealthThresholds())
        assert report.overall_passed is True
        assert report.degraded_count == 0
        assert report.failed_count == 0
        assert report.unhealthy_count == 0
        assert report.avg_cpu_usage < 70.0
        assert report.avg_memory_usage < 70.0
        assert not report.failed_checks

    def test_analyze_cluster_failing_due_to_degraded_pct(self, cluster_state: ClusterState) -> None:
        """Cluster fails when percentage of degraded servers exceeds threshold."""
        # Degrade 2 servers out of 10 (20%)
        inject_failures(cluster_state, failure_rate=0.2, failure_type="degrade", seed=42)

        thresholds = HealthThresholds(max_degraded_server_percentage=10.0)
        report = analyze(cluster_state, thresholds)
        assert report.overall_passed is False
        assert any("Degraded server percentage" in check for check in report.failed_checks)

    def test_analyze_cluster_failing_due_to_failed_server(
        self, cluster_state: ClusterState
    ) -> None:
        """Cluster fails if any server is in FAILED status (threshold=0.0)."""
        # Fail 1 server
        inject_failures(cluster_state, failure_rate=0.1, failure_type="fail", seed=42)

        report = analyze(cluster_state, HealthThresholds(max_failed_server_percentage=0.0))
        assert report.overall_passed is False
        assert any("Failed server percentage" in check for check in report.failed_checks)

    def test_analyze_cluster_failing_due_to_avg_cpu(self, cluster_state: ClusterState) -> None:
        """Cluster fails if average CPU usage exceeds threshold."""
        # Spike resources on 8 servers
        inject_failures(cluster_state, failure_rate=0.8, failure_type="resource_spike", seed=42)

        report = analyze(cluster_state, HealthThresholds(max_cluster_cpu_usage_avg=60.0))
        assert report.overall_passed is False
        assert any("Average cluster CPU usage" in check for check in report.failed_checks)


# ======================================================================
# Integration Tests
# ======================================================================


class TestDeploymentHealthCheckIntegration:
    """End-to-end integration tests between deployment engine and health analyzer."""

    def test_successful_deployment_with_health_checks(self) -> None:
        """Deployment passes when health checks pass throughout stages."""
        cluster_state = ClusterState(generate_cluster(size=5, seed=300))
        thresholds = HealthThresholds()
        health_check = create_health_check_fn(thresholds)

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[20, 60, 100],
            stage_delay_seconds=0.0,
            health_check_fn=health_check,
        )

        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.COMPLETED
        assert len(result.servers_updated) == 5
        for s in cluster_state.servers:
            assert s.current_version == "2.0.0"
            assert s.status == ServerStatus.HEALTHY

    def test_deployment_rollback_on_injected_failure(self) -> None:
        """Deployment aborts and rolls back when failures are injected mid-rollout."""
        cluster_state = ClusterState(generate_cluster(size=10, seed=400))
        thresholds = HealthThresholds(max_degraded_server_percentage=0.0)

        # We will create a custom health check function that injects failure
        # after the first stage has completed (when some nodes are on v2.0.0).
        call_count = 0
        real_health_fn = create_health_check_fn(thresholds)

        def test_health_check_hook(cs: ClusterState) -> bool:
            nonlocal call_count
            if call_count == 1:
                # Inject failure into nodes running v2.0.0
                inject_failures(
                    cs,
                    target_version="2.0.0",
                    failure_rate=1.0,
                    failure_type="degrade",
                    seed=42,
                )
            call_count += 1
            return real_health_fn(cs)

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[20, 50, 100],
            stage_delay_seconds=0.0,
            health_check_interval=0.0,
            health_check_fn=test_health_check_hook,
            max_retries_per_stage=0,
        )

        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        # Deployment should fail and trigger a rollback
        assert result.status == DeploymentStatus.ROLLED_BACK
        assert result.current_stage_index == 1  # Fails at stage 1 (50%) due to injected failure
        assert "Health check failed" in (result.error_message or "")

        # Verify all servers are successfully rolled back to v1.0.0 and marked healthy
        for s in cluster_state.servers:
            assert s.current_version == "1.0.0"
            assert s.status == ServerStatus.HEALTHY
