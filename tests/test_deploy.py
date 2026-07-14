"""
Unit tests for the deployment coordination engine.

Covers: DeploymentConfig validation, DeploymentState lifecycle,
StageResult tracking, DeploymentEngine staged rollout, percentage-based
updates, rollback on health-check failure, abort handling, and
configurable timing.
"""

from __future__ import annotations

import threading
import time

import pytest

from cluster.generator import generate_cluster
from cluster.models import ServerStatus
from cluster.state import ClusterState
from deploy.config import DEFAULT_STAGES, DeploymentConfig
from deploy.engine import DeploymentEngine
from deploy.state import DeploymentState, DeploymentStatus, StageResult

# ======================================================================
# DeploymentConfig
# ======================================================================


class TestDeploymentConfig:
    """Tests for DeploymentConfig validation and defaults."""

    def test_default_stages(self) -> None:
        """Default stages are [10, 25, 50, 75, 100]."""
        cfg = DeploymentConfig(target_version="2.0.0")
        assert cfg.stages == DEFAULT_STAGES
        assert cfg.stages[-1] == 100

    def test_custom_stages(self) -> None:
        """Custom stages are accepted if valid."""
        cfg = DeploymentConfig(
            target_version="2.0.0",
            stages=[20, 50, 100],
        )
        assert cfg.stages == [20, 50, 100]

    def test_stages_must_end_at_100(self) -> None:
        """Last stage must be 100%."""
        with pytest.raises(ValueError, match="Last stage must be 100%"):
            DeploymentConfig(target_version="2.0.0", stages=[10, 50])

    def test_stages_must_be_monotonic(self) -> None:
        """Stages must be monotonically increasing."""
        with pytest.raises(ValueError, match="monotonically increasing"):
            DeploymentConfig(target_version="2.0.0", stages=[50, 25, 100])

    def test_stages_cannot_be_empty(self) -> None:
        """At least one stage is required."""
        with pytest.raises(ValueError, match="at least one"):
            DeploymentConfig(target_version="2.0.0", stages=[])

    def test_stage_percentage_bounds(self) -> None:
        """Each stage must be 1-100."""
        with pytest.raises(ValueError, match="1-100"):
            DeploymentConfig(target_version="2.0.0", stages=[0, 100])

    def test_negative_delay_rejected(self) -> None:
        """Negative stage delay is rejected."""
        with pytest.raises(ValueError, match="stage_delay_seconds"):
            DeploymentConfig(target_version="2.0.0", stage_delay_seconds=-1.0)

    def test_negative_health_check_interval_rejected(self) -> None:
        """Negative health check interval is rejected."""
        with pytest.raises(ValueError, match="health_check_interval"):
            DeploymentConfig(target_version="2.0.0", health_check_interval=-1.0)

    def test_negative_retries_rejected(self) -> None:
        """Negative retries count is rejected."""
        with pytest.raises(ValueError, match="max_retries_per_stage"):
            DeploymentConfig(target_version="2.0.0", max_retries_per_stage=-1)

    def test_str_representation(self) -> None:
        """String representation includes key fields."""
        cfg = DeploymentConfig(target_version="2.0.0", stages=[10, 50, 100])
        s = str(cfg)
        assert "2.0.0" in s
        assert "10%" in s
        assert "50%" in s
        assert "100%" in s

    def test_single_stage_100(self) -> None:
        """A single-stage [100] deployment is valid (big-bang)."""
        cfg = DeploymentConfig(target_version="2.0.0", stages=[100])
        assert cfg.stages == [100]

    def test_zero_delay(self) -> None:
        """Zero delay is valid (no wait between stages)."""
        cfg = DeploymentConfig(target_version="2.0.0", stage_delay_seconds=0.0)
        assert cfg.stage_delay_seconds == 0.0


# ======================================================================
# DeploymentState
# ======================================================================


class TestDeploymentState:
    """Tests for DeploymentState lifecycle tracking."""

    def _make_state(self) -> DeploymentState:
        return DeploymentState(
            deployment_id="test-001",
            target_version="2.0.0",
            source_version="1.0.0",
            total_servers=10,
            servers_pending={"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"},
        )

    def test_initial_status(self) -> None:
        """New deployment starts in PENDING state."""
        ds = self._make_state()
        assert ds.status == DeploymentStatus.PENDING
        assert ds.is_active is True
        assert ds.is_terminal is False

    def test_progress_percentage(self) -> None:
        """Progress is calculated from updated vs total servers."""
        ds = self._make_state()
        assert ds.progress_percentage == 0.0

        ds.servers_updated.add("s1")
        assert ds.progress_percentage == 10.0

        ds.servers_updated.update({"s2", "s3", "s4", "s5"})
        assert ds.progress_percentage == 50.0

    def test_mark_completed(self) -> None:
        """mark_completed sets terminal state."""
        ds = self._make_state()
        ds.mark_completed()
        assert ds.status == DeploymentStatus.COMPLETED
        assert ds.is_terminal is True
        assert ds.completed_at is not None

    def test_mark_failed(self) -> None:
        """mark_failed sets terminal state with error message."""
        ds = self._make_state()
        ds.mark_failed("something broke")
        assert ds.status == DeploymentStatus.FAILED
        assert ds.is_terminal is True
        assert ds.error_message == "something broke"

    def test_mark_aborted(self) -> None:
        """mark_aborted sets terminal state with reason."""
        ds = self._make_state()
        ds.mark_aborted("user requested")
        assert ds.status == DeploymentStatus.ABORTED
        assert ds.is_terminal is True

    def test_mark_rolling_back_and_rolled_back(self) -> None:
        """Rollback lifecycle transitions work."""
        ds = self._make_state()
        ds.mark_rolling_back()
        assert ds.status == DeploymentStatus.ROLLING_BACK
        assert ds.is_terminal is False
        assert ds.is_active is False

        ds.mark_rolled_back()
        assert ds.status == DeploymentStatus.ROLLED_BACK
        assert ds.is_terminal is True

    def test_to_dict(self) -> None:
        """Serialisation includes all key fields."""
        ds = self._make_state()
        d = ds.to_dict()
        assert d["deployment_id"] == "test-001"
        assert d["target_version"] == "2.0.0"
        assert d["source_version"] == "1.0.0"
        assert d["status"] == "pending"
        assert d["total_servers"] == 10

    def test_duration_seconds(self) -> None:
        """Duration is calculated correctly."""
        ds = self._make_state()
        # Duration should be a small positive number
        assert ds.duration_seconds >= 0


# ======================================================================
# StageResult
# ======================================================================


class TestStageResult:
    """Tests for StageResult tracking."""

    def test_basic_fields(self) -> None:
        """StageResult stores stage metadata correctly."""
        sr = StageResult(
            stage_index=0,
            target_percentage=10,
            servers_total=50,
        )
        assert sr.stage_index == 0
        assert sr.target_percentage == 10
        assert sr.servers_total == 50
        assert sr.health_check_passed is None
        assert sr.error is None

    def test_to_dict(self) -> None:
        """Serialisation works."""
        sr = StageResult(stage_index=1, target_percentage=25, servers_total=30)
        d = sr.to_dict()
        assert d["stage_index"] == 1
        assert d["target_percentage"] == 25


# ======================================================================
# DeploymentEngine — Core Rollout
# ======================================================================


class TestDeploymentEngine:
    """Tests for the deployment engine's staged rollout logic."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        """20-server cluster for deterministic testing."""
        return ClusterState(generate_cluster(size=20, seed=42))

    def test_full_deployment_completes(self, cluster_state: ClusterState) -> None:
        """A simple deployment with no health checks completes successfully."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.COMPLETED
        assert len(result.servers_updated) == 20
        assert result.is_terminal is True
        assert result.progress_percentage == 100.0

    def test_all_servers_updated_to_target_version(self, cluster_state: ClusterState) -> None:
        """After deployment, all servers run the target version."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        engine.deploy(config)

        for server in cluster_state.servers:
            assert server.current_version == "2.0.0"

    def test_all_servers_healthy_after_deployment(self, cluster_state: ClusterState) -> None:
        """After successful deployment, all updated servers are HEALTHY."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        engine.deploy(config)

        for server in cluster_state.servers:
            assert server.status == ServerStatus.HEALTHY

    def test_source_version_detected(self, cluster_state: ClusterState) -> None:
        """Engine correctly detects the current (source) version."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert result.source_version == "1.0.0"

    def test_deployment_id_generated(self, cluster_state: ClusterState) -> None:
        """Each deployment gets a unique ID."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert result.deployment_id is not None
        assert len(result.deployment_id) > 0

    def test_current_deployment_property(self, cluster_state: ClusterState) -> None:
        """current_deployment is set during and after deploy()."""
        engine = DeploymentEngine(cluster_state)
        assert engine.current_deployment is None

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
        )
        result = engine.deploy(config)
        assert engine.current_deployment is result


# ======================================================================
# DeploymentEngine — Percentage-Based Updates
# ======================================================================


class TestPercentageBasedUpdates:
    """Tests for percentage-based server selection logic."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=20, seed=42))

    def test_stage_updates_correct_percentage(self, cluster_state: ClusterState) -> None:
        """Each stage updates the correct cumulative percentage of servers."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[10, 50, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        # Stage 0: 10% of 20 = 2 servers
        assert len(result.stages[0].servers_updated) == 2
        # Stage 1: 50% of 20 = 10, minus 2 already done = 8 more
        assert len(result.stages[1].servers_updated) == 8
        # Stage 2: 100% of 20 = 20, minus 10 already done = 10 more
        assert len(result.stages[2].servers_updated) == 10

    def test_stage_percentages_are_cumulative(self, cluster_state: ClusterState) -> None:
        """Cumulative server count matches each stage's target percentage."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[25, 75, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        cumulative = 0
        for stage in result.stages:
            cumulative += len(stage.servers_updated)
            expected = (stage.target_percentage * 20) // 100
            # Allow ceiling rounding
            assert cumulative >= expected or cumulative == expected

        assert cumulative == 20

    def test_cross_region_distribution(self, cluster_state: ClusterState) -> None:
        """Server selection distributes across regions."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[25, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        # First stage (25% = 5 servers) should span multiple regions
        first_stage_ids = result.stages[0].servers_updated
        regions_hit = set()
        for sid in first_stage_ids:
            server = cluster_state.get_server(sid)
            assert server is not None
            regions_hit.add(server.region)

        # With 4 regions and 5 servers, we should hit multiple regions
        assert len(regions_hit) >= 2


# ======================================================================
# DeploymentEngine — Deployment State Tracking
# ======================================================================


class TestDeploymentStateTracking:
    """Tests for deployment state tracking across stages."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=10, seed=42))

    def test_stages_recorded(self, cluster_state: ClusterState) -> None:
        """Each stage produces a StageResult record."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[30, 60, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert len(result.stages) == 3
        for i, stage in enumerate(result.stages):
            assert stage.stage_index == i
            assert stage.completed_at is not None
            assert stage.duration_seconds >= 0

    def test_stage_result_server_lists(self, cluster_state: ClusterState) -> None:
        """Each stage records which servers were updated."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        all_updated = []
        for stage in result.stages:
            all_updated.extend(stage.servers_updated)

        # No duplicates across stages
        assert len(all_updated) == len(set(all_updated))
        # All servers accounted for
        assert len(all_updated) == 10

    def test_servers_pending_empties(self, cluster_state: ClusterState) -> None:
        """After full deployment, servers_pending is empty."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert len(result.servers_pending) == 0

    def test_to_dict_after_deployment(self, cluster_state: ClusterState) -> None:
        """Deployment state serialises correctly after completion."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)
        d = result.to_dict()

        assert d["status"] == "completed"
        assert d["progress_percentage"] == 100.0
        assert len(d["stages"]) == 2
        assert d["duration_seconds"] >= 0


# ======================================================================
# DeploymentEngine — Rollout Sequencing
# ======================================================================


class TestRolloutSequencing:
    """Tests for rollout sequencing and ordering logic."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=20, seed=42))

    def test_stages_execute_in_order(self, cluster_state: ClusterState) -> None:
        """Stages execute in ascending percentage order."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[10, 25, 50, 75, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert len(result.stages) == 5
        for i in range(len(result.stages) - 1):
            assert result.stages[i].target_percentage < result.stages[i + 1].target_percentage

    def test_no_server_updated_twice(self, cluster_state: ClusterState) -> None:
        """No server is updated more than once across all stages."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[10, 25, 50, 75, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        all_updated: list[str] = []
        for stage in result.stages:
            all_updated.extend(stage.servers_updated)

        assert len(all_updated) == len(set(all_updated))


# ======================================================================
# DeploymentEngine — Health Check Integration
# ======================================================================


class TestHealthCheckIntegration:
    """Tests for health-check callback integration."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=10, seed=42))

    def test_health_check_passing(self, cluster_state: ClusterState) -> None:
        """Deployment succeeds when health check always passes."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
            health_check_fn=lambda cs: True,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.COMPLETED
        for stage in result.stages:
            assert stage.health_check_passed is True

    def test_health_check_failure_triggers_rollback(self, cluster_state: ClusterState) -> None:
        """Deployment rolls back when health check fails."""
        call_count = 0

        def failing_health_check(cs: ClusterState) -> bool:
            nonlocal call_count
            call_count += 1
            return False  # Always fail

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
            health_check_interval=0.0,
            health_check_fn=failing_health_check,
            max_retries_per_stage=0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.ROLLED_BACK
        assert "Health check failed" in (result.error_message or "")

    def test_health_check_failure_after_first_stage(self, cluster_state: ClusterState) -> None:
        """Servers are rolled back when health check fails after stage 1."""
        stage_count = [0]

        def fail_on_second_stage(cs: ClusterState) -> bool:
            stage_count[0] += 1
            return stage_count[0] <= 1  # Pass first stage, fail second

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[30, 60, 100],
            stage_delay_seconds=0.0,
            health_check_interval=0.0,
            health_check_fn=fail_on_second_stage,
            max_retries_per_stage=0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.ROLLED_BACK
        # After rollback, previously updated servers should be back to v1.0.0
        for server in cluster_state.servers:
            assert server.current_version == "1.0.0"

    def test_health_check_retry_then_pass(self, cluster_state: ClusterState) -> None:
        """Health check retries and eventually passes."""
        attempt = [0]

        def pass_on_retry(cs: ClusterState) -> bool:
            attempt[0] += 1
            return attempt[0] >= 2  # Fail first, pass on retry

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
            health_check_interval=0.0,
            health_check_fn=pass_on_retry,
            max_retries_per_stage=2,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.COMPLETED

    def test_health_check_exception_treated_as_failure(self, cluster_state: ClusterState) -> None:
        """Health check exceptions are treated as failures."""

        def exploding_check(cs: ClusterState) -> bool:
            raise RuntimeError("health check crashed")

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
            health_check_interval=0.0,
            health_check_fn=exploding_check,
            max_retries_per_stage=0,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.ROLLED_BACK


# ======================================================================
# DeploymentEngine — Configurable Timing
# ======================================================================


class TestConfigurableTiming:
    """Tests for configurable timing controls."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=10, seed=42))

    def test_zero_delay_fast_deployment(self, cluster_state: ClusterState) -> None:
        """Zero delay means no waiting between stages."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster_state)

        start = time.time()
        result = engine.deploy(config)
        elapsed = time.time() - start

        assert result.status == DeploymentStatus.COMPLETED
        # Should complete very quickly (< 1 second)
        assert elapsed < 2.0

    def test_nonzero_delay_adds_wait(self, cluster_state: ClusterState) -> None:
        """Non-zero delay causes actual waiting between stages."""
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.5,
        )
        engine = DeploymentEngine(cluster_state)

        start = time.time()
        result = engine.deploy(config)
        elapsed = time.time() - start

        assert result.status == DeploymentStatus.COMPLETED
        # Should take at least 0.5 seconds (one inter-stage delay)
        assert elapsed >= 0.4

    def test_on_stage_complete_callback(self, cluster_state: ClusterState) -> None:
        """on_stage_complete callback is invoked after each stage."""
        callbacks: list[tuple[int, int, int]] = []

        def on_complete(stage_idx: int, pct: int, count: int) -> None:
            callbacks.append((stage_idx, pct, count))

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[30, 60, 100],
            stage_delay_seconds=0.0,
            on_stage_complete=on_complete,
        )
        engine = DeploymentEngine(cluster_state)
        engine.deploy(config)

        assert len(callbacks) == 3
        assert callbacks[0][0] == 0  # stage_idx
        assert callbacks[0][1] == 30  # percentage
        assert callbacks[1][0] == 1
        assert callbacks[2][0] == 2


# ======================================================================
# DeploymentEngine — Abort Handling
# ======================================================================


class TestAbortHandling:
    """Tests for abort event integration."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=10, seed=42))

    def test_abort_before_start(self, cluster_state: ClusterState) -> None:
        """Pre-set abort event stops deployment immediately."""
        abort_event = threading.Event()
        abort_event.set()  # Already signalled

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
            abort_event=abort_event,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.ABORTED

    def test_abort_during_inter_stage_wait(self, cluster_state: ClusterState) -> None:
        """Abort signal during inter-stage wait stops deployment."""
        abort_event = threading.Event()

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=5.0,  # Long delay — abort will interrupt
            abort_event=abort_event,
        )
        engine = DeploymentEngine(cluster_state)

        # Signal abort after a brief delay
        def signal_abort() -> None:
            time.sleep(0.3)
            abort_event.set()

        threading.Thread(target=signal_abort, daemon=True).start()

        start = time.time()
        result = engine.deploy(config)
        elapsed = time.time() - start

        assert result.status == DeploymentStatus.ABORTED
        # Should not have waited the full 5 seconds
        assert elapsed < 3.0

    def test_abort_rolls_back_updated_servers(self, cluster_state: ClusterState) -> None:
        """Aborted deployment rolls back servers that were already updated."""
        abort_event = threading.Event()

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=5.0,
            abort_event=abort_event,
        )
        engine = DeploymentEngine(cluster_state)

        # Signal abort after first stage completes
        def signal_abort() -> None:
            time.sleep(0.3)
            abort_event.set()

        threading.Thread(target=signal_abort, daemon=True).start()
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.ABORTED
        # All servers should be back to v1.0.0 after rollback
        for server in cluster_state.servers:
            assert server.current_version == "1.0.0"

    def test_abort_mid_stage_rollout(self, cluster_state: ClusterState) -> None:
        """Abort signal set mid-stage sets deployment status to ABORTED (not ROLLED_BACK)."""
        abort_event = threading.Event()

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
            abort_event=abort_event,
        )
        engine = DeploymentEngine(cluster_state)

        # Trigger abort while engine is updating nodes in stage
        # We hook into update_server_version to set the abort event mid-way
        original_update = cluster_state.update_server_version
        call_count = 0

        def mock_update_version(server_id: str, new_version: str) -> bool:
            nonlocal call_count
            if call_count == 2:
                abort_event.set()
            call_count += 1
            return original_update(server_id, new_version)

        cluster_state.update_server_version = mock_update_version
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.ABORTED
        assert "Abort signal received mid-stage" in result.error_message


# ======================================================================
# DeploymentEngine — Edge Cases
# ======================================================================


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_single_server_cluster(self) -> None:
        """Deployment works with a single server."""
        cs = ClusterState(generate_cluster(size=1, seed=42))
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cs)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.COMPLETED
        assert len(result.servers_updated) == 1

    def test_large_cluster(self) -> None:
        """Deployment works with a large cluster."""
        cs = ClusterState(generate_cluster(size=50, seed=42))
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[10, 25, 50, 75, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cs)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.COMPLETED
        assert len(result.servers_updated) == 50

    def test_single_stage_big_bang(self) -> None:
        """Single [100] stage updates all servers at once."""
        cs = ClusterState(generate_cluster(size=20, seed=42))
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cs)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.COMPLETED
        assert len(result.stages) == 1
        assert len(result.stages[0].servers_updated) == 20

    def test_under_provisioned_update_triggers_rollback(self) -> None:
        """Verify that a stage fails and triggers rollback if fewer servers are updated than required."""
        cs = ClusterState(generate_cluster(size=10, seed=42))
        # Set 6 servers to FAILED status so they are not updatable
        for i in range(1, 7):
            cs.update_server_status(f"server-00{i}", ServerStatus.FAILED)

        # We require updating 50% (5 servers), but only 4 are updatable (healthy)
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cs)
        result = engine.deploy(config)

        # Rollout should fail with ROLLED_BACK state due to under-provisioned update
        assert result.status == DeploymentStatus.ROLLED_BACK
        assert "Under-provisioned update" in result.error_message

    def test_deployment_state_serialisation_roundtrip(self) -> None:
        """DeploymentState.to_dict() produces valid JSON-serialisable data."""
        import json

        cs = ClusterState(generate_cluster(size=10, seed=42))
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cs)
        result = engine.deploy(config)
        d = result.to_dict()

        # Should be JSON-serialisable
        json_str = json.dumps(d, default=str)
        assert isinstance(json_str, str)
        assert len(json_str) > 0


# ======================================================================
# DeploymentEngine — Rollback Event Semantics
# ======================================================================


class TestRollbackEventSemantics:
    """Tests verifying correct semantic separation of ROLLBACK_INITIATED
    vs DEPLOYMENT_FAILED event types."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=10, seed=42))

    def test_rollback_emits_rollback_initiated_not_failed(
        self, cluster_state: ClusterState
    ) -> None:
        """When a health check fails and triggers rollback, the engine should emit
        ROLLBACK_INITIATED (not DEPLOYMENT_FAILED), because rollback is a recovery
        flow, not a terminal failure."""
        from deploy.audit import AuditLogger, DeploymentEventType

        audit = AuditLogger()
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
            health_check_interval=0.0,
            health_check_fn=lambda cs: False,  # Always fail
            max_retries_per_stage=0,
            audit_logger=audit,
        )
        engine = DeploymentEngine(cluster_state)
        result = engine.deploy(config)

        assert result.status == DeploymentStatus.ROLLED_BACK

        events = audit.get_events()
        event_types = [e.event_type for e in events]

        # ROLLBACK_INITIATED should be present (recovery flow)
        assert DeploymentEventType.ROLLBACK_INITIATED in event_types

        # DEPLOYMENT_FAILED should NOT be present (this was a recoverable rollback)
        assert DeploymentEventType.DEPLOYMENT_FAILED not in event_types

        # Verify rollback lifecycle events are present
        assert DeploymentEventType.ROLLBACK_START in event_types
        assert DeploymentEventType.ROLLBACK_COMPLETE in event_types

    def test_unrecoverable_error_emits_deployment_failed(self, cluster_state: ClusterState) -> None:
        """When an unexpected exception occurs during deployment, the engine should
        emit DEPLOYMENT_FAILED (not ROLLBACK_INITIATED), because this is a true
        unrecoverable failure."""
        from deploy.audit import AuditLogger, DeploymentEventType

        audit = AuditLogger()

        # Inject an exception into the health check that propagates unexpectedly
        # We'll monkeypatch the engine to raise during stage execution
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
            audit_logger=audit,
        )
        engine = DeploymentEngine(cluster_state)

        # Monkeypatch _execute_stage to raise an unexpected exception

        def explosive_execute(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise RuntimeError("Catastrophic unexpected failure")

        engine._execute_stage = explosive_execute  # type: ignore[assignment]

        result = engine.deploy(config)

        assert result.status == DeploymentStatus.FAILED

        events = audit.get_events()
        event_types = [e.event_type for e in events]

        # DEPLOYMENT_FAILED should be present (true unrecoverable error)
        assert DeploymentEventType.DEPLOYMENT_FAILED in event_types

        # ROLLBACK_INITIATED should NOT be present (no rollback was attempted)
        assert DeploymentEventType.ROLLBACK_INITIATED not in event_types
