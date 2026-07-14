"""
Unit and integration tests for Phase 9 Operational Resilience, Failure Recovery & Observability.
"""

from __future__ import annotations

import pytest

from cluster.generator import generate_cluster
from cluster.models import ServerStatus
from cluster.state import ClusterState
from deploy.config import DeploymentConfig
from deploy.engine import DeploymentEngine
from governance.models import GovernanceDecision, RiskScore
from resilience.models import (
    RecoveryPlanStatus,
)
from resilience.observability import OperationalObservabilityLayer
from resilience.policies import (
    UnsafeRecoveryPreventionPolicy,
)
from resilience.quarantine import RegionQuarantineSystem
from resilience.recovery import RecoveryPlanningEngine
from resilience.replay import EventReplayEngine
from resilience.snapshots import ClusterSnapshotSystem


class TestResilienceEngine:
    """Comprehensive test suite verifying resilience, recovery, and observability."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=10, seed=42))

    # ------------------------------------------------------------------
    # 1. Snapshot Tests
    # ------------------------------------------------------------------

    def test_snapshot_create_and_restore(self, cluster_state: ClusterState) -> None:
        """Verify that cluster snapshots capture and restore point-in-time states accurately."""
        system = ClusterSnapshotSystem(cluster_state)

        # Modify some servers in live state
        servers = cluster_state.servers
        cluster_state.update_server_status(servers[0].id, ServerStatus.DEGRADED)
        cluster_state.update_server_version(servers[1].id, "1.5.0")

        # Capture snapshot
        snapshot = system.create_snapshot()

        # Perform further changes
        cluster_state.update_server_status(servers[0].id, ServerStatus.FAILED)
        cluster_state.update_server_version(servers[1].id, "2.0.0")

        # Restore snapshot
        success = system.restore_snapshot(snapshot)
        assert success is True

        # Verify restoration matches snapshot state
        assert cluster_state.get_server(servers[0].id).status == ServerStatus.DEGRADED
        assert cluster_state.get_server(servers[1].id).current_version == "1.5.0"

    def test_snapshot_restoration_exception_safety(self, cluster_state: ClusterState) -> None:
        """Verify snapshot restoration transaction rollback if a corruption occurs mid-restoration."""
        system = ClusterSnapshotSystem(cluster_state)

        # Modify some live states
        servers = cluster_state.servers
        cluster_state.update_server_status(servers[0].id, ServerStatus.DEGRADED)

        # Capture snapshot
        snapshot = system.create_snapshot()

        # Change state
        cluster_state.update_server_status(servers[0].id, ServerStatus.HEALTHY)

        # Inject corrupt/malformed server list inside snapshot to trigger restoration failure
        snapshot.servers = [
            {
                "id": servers[0].id,
                "status": "INVALID_STATUS",
                "current_version": "1.0.0",
                "previous_version": None,
                "region": "us-east-1",
                "cpu_usage": 0.0,
                "memory_usage": 0.0,
                "last_health_check": None,
                "deployment_history": [],
            }
        ]

        # Attempt restore (should fail and execute transaction rollback)
        success = system.restore_snapshot(snapshot)
        assert success is False

        # Live cluster must remain in the post-snapshot backup state (HEALTHY status preserved)
        assert cluster_state.get_server(servers[0].id).status == ServerStatus.HEALTHY

    # ------------------------------------------------------------------
    # 2. Region Quarantine Tests
    # ------------------------------------------------------------------

    def test_quarantine_auto_activation(self, cluster_state: ClusterState) -> None:
        """Verify regions are automatically quarantined when degradation thresholds are exceeded."""
        quarantine = RegionQuarantineSystem(cluster_state)

        # Force us-east-1 servers to fail
        us_east_servers = [s for s in cluster_state.servers if s.region == "us-east-1"]
        assert len(us_east_servers) > 0

        for s in us_east_servers:
            cluster_state.update_server_status(s.id, ServerStatus.FAILED)

        # Run auto-quarantine check
        quarantined = quarantine.check_and_auto_quarantine(threshold_percentage=30.0)
        assert "us-east-1" in quarantined
        assert quarantine.is_quarantined("us-east-1") is True

    def test_quarantine_routing_freeze(self, cluster_state: ClusterState) -> None:
        """Verify the deployment engine skips quarantined regions during server selection."""
        quarantine = RegionQuarantineSystem(cluster_state)
        quarantine.quarantine_region("us-east-1", "Manual isolation")

        config = DeploymentConfig(
            target_version="2.0.0", stages=[10, 100], quarantine_system=quarantine
        )

        engine = DeploymentEngine(cluster_state)
        deployment = engine._init_deployment(config)
        engine._current_config = config

        # Select servers for the stage (e.g. 5 servers needed)
        selected = engine._select_servers_for_stage(deployment, count=5)

        # Quarantined us-east-1 servers must NOT be selected
        for s_id in selected:
            s = cluster_state.get_server(s_id)
            assert s.region != "us-east-1"

    # ------------------------------------------------------------------
    # 3. Recovery Planning Tests
    # ------------------------------------------------------------------

    def test_recovery_plan_execution(self, cluster_state: ClusterState) -> None:
        """Verify recovery plan generation, step execution, and approval gates."""
        quarantine = RegionQuarantineSystem(cluster_state)
        recovery = RecoveryPlanningEngine(cluster_state, quarantine)

        config = DeploymentConfig(target_version="2.0.0", stages=[100])
        engine = DeploymentEngine(cluster_state)
        deployment = engine._init_deployment(config)

        # Simulate update to 2 servers
        servers = cluster_state.servers
        cluster_state.update_server_version(servers[0].id, "2.0.0")
        deployment.servers_updated.add(servers[0].id)
        cluster_state.update_server_version(servers[1].id, "2.0.0")
        deployment.servers_updated.add(servers[1].id)

        # Generate recovery plan with partial_rollback strategy
        plan = recovery.generate_plan(deployment, "partial_rollback")
        assert len(plan.steps) == 0  # No degraded servers initially

        # Mark one server degraded to trigger partial rollback step
        cluster_state.update_server_status(servers[0].id, ServerStatus.DEGRADED)
        plan = recovery.generate_plan(deployment, "partial_rollback")
        assert len(plan.steps) == 1
        assert plan.steps[0]["target"] == servers[0].id

        # Verify executing recovery plan reverts the degraded node
        success = recovery.execute_recovery_plan(plan, deployment)
        assert success is True
        assert cluster_state.get_server(servers[0].id).current_version == "1.0.0"
        assert cluster_state.get_server(servers[1].id).current_version == "2.0.0"

    def test_recovery_plan_approval_denied(self, cluster_state: ClusterState) -> None:
        """Verify that a denied recovery step approval halts plan execution."""
        quarantine = RegionQuarantineSystem(cluster_state)
        # Callback returns False (denying step approval)
        recovery = RecoveryPlanningEngine(
            cluster_state, quarantine, approval_callback=lambda step: False
        )

        config = DeploymentConfig(target_version="2.0.0", stages=[100])
        engine = DeploymentEngine(cluster_state)
        deployment = engine._init_deployment(config)

        servers = cluster_state.servers
        cluster_state.update_server_version(servers[0].id, "2.0.0")
        cluster_state.update_server_status(servers[0].id, ServerStatus.FAILED)
        deployment.servers_updated.add(servers[0].id)

        plan = recovery.generate_plan(deployment, "partial_rollback")

        # Execution should return False and mark status as FAILED
        success = recovery.execute_recovery_plan(plan, deployment)
        assert success is False
        assert plan.status == RecoveryPlanStatus.FAILED

    # ------------------------------------------------------------------
    # 4. Event Replay Tests
    # ------------------------------------------------------------------

    def test_event_replay_engine(self) -> None:
        """Verify EventReplayEngine timeline, causality graph, and state reconstructions."""
        events = [
            {
                "event_id": "evt-1",
                "timestamp": "2026-06-17T12:00:00Z",
                "event_type": "deployment_start",
                "deployment_id": "dep-1",
                "details": {"target_version": "2.0.0", "source_version": "1.0.0"},
            },
            {
                "event_id": "evt-2",
                "parent_event_id": "evt-1",
                "timestamp": "2026-06-17T12:01:00Z",
                "event_type": "stage_transition",
                "deployment_id": "dep-1",
                "details": {"stage_index": 0, "servers_updated": ["server-001"]},
            },
            {
                "event_id": "evt-3",
                "parent_event_id": "evt-2",
                "timestamp": "2026-06-17T12:02:00Z",
                "event_type": "deployment_completed",
                "deployment_id": "dep-1",
                "details": {},
            },
        ]

        replay = EventReplayEngine()

        # Verify lineage check passes
        is_valid, errors = replay.verify_event_lineage(events)
        assert is_valid is True
        assert len(errors) == 0

        # Verify causality graph
        graph = replay.build_causality_graph(events)
        assert graph["evt-1"] == ["evt-2"]
        assert graph["evt-2"] == ["evt-3"]

        # Verify state reconstruction at step 2
        state_at_step = replay.reconstruct_state_at_step(events, "evt-2")
        assert state_at_step["deployment_status"] == "in_progress"
        assert state_at_step["servers"]["server-001"]["version"] == "2.0.0"

    def test_event_replay_lineage_corruption(self) -> None:
        """Verify EventReplayEngine detects lineage corruption (missing parents or cycles)."""
        events = [
            {
                "event_id": "evt-1",
                "parent_event_id": "evt-999",  # missing parent
                "timestamp": "2026-06-17T12:00:00Z",
                "event_type": "deployment_start",
                "deployment_id": "dep-1",
            },
            {
                "event_id": "evt-2",
                "parent_event_id": "evt-2",  # cycle/self-parent
                "timestamp": "2026-06-17T12:01:00Z",
                "event_type": "stage_transition",
                "deployment_id": "dep-1",
            },
        ]
        replay = EventReplayEngine()
        is_valid, errors = replay.verify_event_lineage(events)
        assert is_valid is False
        assert len(errors) > 0

    # ------------------------------------------------------------------
    # 5. Resilience Policies Tests
    # ------------------------------------------------------------------

    def test_unsafe_recovery_prevention(self, cluster_state: ClusterState) -> None:
        """Verify UnsafeRecoveryPreventionPolicy blocks recovery plans at critical risk scores."""
        policy = UnsafeRecoveryPreventionPolicy()
        config = DeploymentConfig(target_version="2.0.0", stages=[100])
        engine = DeploymentEngine(cluster_state)
        deployment = engine._init_deployment(config)

        # Evaluate under low risk
        res1 = policy.evaluate(cluster_state, deployment, {"risk_category": RiskScore.LOW})
        assert res1.passed is True

        # Evaluate under critical risk (should pass=False, decision=BLOCK)
        res2 = policy.evaluate(cluster_state, deployment, {"risk_category": RiskScore.CRITICAL})
        assert res2.passed is False
        assert res2.decision == GovernanceDecision.BLOCK

    # ------------------------------------------------------------------
    # 6. Observability Metrics Tests
    # ------------------------------------------------------------------

    def test_observability_aggregation(self) -> None:
        """Verify metrics aggregation maps completed durations and rollback frequencies correctly."""
        events = [
            {"event_id": "evt-1", "event_type": "deployment_start", "deployment_id": "dep-1"},
            {
                "event_id": "evt-2",
                "event_type": "deployment_completed",
                "deployment_id": "dep-1",
                "details": {"duration_seconds": 15.0},
            },
            {"event_id": "evt-3", "event_type": "deployment_start", "deployment_id": "dep-2"},
            {
                "event_id": "evt-4",
                "event_type": "rollback_initiated",
                "deployment_id": "dep-2",
                "details": {"reason": "health check failed at stage 0 (ap-southeast-1)"},
            },
            {
                "event_id": "evt-5",
                "event_type": "rollback_start",
                "deployment_id": "dep-2",
                "details": {"reason": "health check failed at stage 0 (ap-southeast-1)"},
            },
            {
                "event_id": "evt-6",
                "event_type": "rollback_complete",
                "deployment_id": "dep-2",
                "details": {"servers_rolled_back": ["server-001"]},
            },
        ]

        obs = OperationalObservabilityLayer()
        metrics = obs.aggregate_metrics(events)

        assert metrics["total_deployments"] == 2
        assert metrics["completions"] == 1
        assert metrics["failures"] == 0  # rollback_initiated is NOT a failure
        assert metrics["rollbacks_initiated"] == 1
        assert metrics["rollbacks_completed"] == 1
        assert metrics["rollback_ratio"] == 0.5
        assert metrics["average_completed_duration_seconds"] == 15.0

    def test_observability_distinguishes_failed_vs_rollback(self) -> None:
        """Verify that rollback recoveries do NOT inflate the failure counter.

        deployment_failed should ONLY be counted for true unrecoverable failures.
        rollback_initiated should be tracked separately.
        """
        events = [
            # Deployment 1: successful rollback recovery (NOT a failure)
            {"event_id": "e1", "event_type": "deployment_start", "deployment_id": "dep-1"},
            {
                "event_id": "e2",
                "event_type": "rollback_initiated",
                "deployment_id": "dep-1",
                "details": {"reason": "Health check failed"},
            },
            {
                "event_id": "e3",
                "event_type": "rollback_start",
                "deployment_id": "dep-1",
                "details": {},
            },
            {
                "event_id": "e4",
                "event_type": "rollback_complete",
                "deployment_id": "dep-1",
                "details": {"servers_rolled_back": ["s1"]},
            },
            # Deployment 2: true unrecoverable failure
            {"event_id": "e5", "event_type": "deployment_start", "deployment_id": "dep-2"},
            {
                "event_id": "e6",
                "event_type": "deployment_failed",
                "deployment_id": "dep-2",
                "details": {"error": "Unexpected error: crash"},
            },
            # Deployment 3: successful
            {"event_id": "e7", "event_type": "deployment_start", "deployment_id": "dep-3"},
            {
                "event_id": "e8",
                "event_type": "deployment_completed",
                "deployment_id": "dep-3",
                "details": {"duration_seconds": 5.0},
            },
        ]

        obs = OperationalObservabilityLayer()
        metrics = obs.aggregate_metrics(events)

        assert metrics["total_deployments"] == 3
        assert metrics["completions"] == 1
        assert metrics["failures"] == 1  # Only dep-2 is a hard failure
        assert metrics["rollbacks_initiated"] == 1  # dep-1 rolled back (recovery, not failure)
        assert metrics["rollbacks_completed"] == 1

    # ------------------------------------------------------------------
    # 7. Replay Rollback Initiated State Reconstruction
    # ------------------------------------------------------------------

    def test_replay_rollback_initiated_state_reconstruction(self) -> None:
        """Verify rollback_initiated sets deployment_status to 'rolling_back' during replay."""
        events = [
            {
                "event_id": "evt-1",
                "timestamp": "2026-06-17T12:00:00Z",
                "event_type": "deployment_start",
                "deployment_id": "dep-1",
                "details": {"target_version": "2.0.0", "source_version": "1.0.0"},
            },
            {
                "event_id": "evt-2",
                "parent_event_id": "evt-1",
                "timestamp": "2026-06-17T12:01:00Z",
                "event_type": "stage_transition",
                "deployment_id": "dep-1",
                "details": {"stage_index": 0, "servers_updated": ["server-001"]},
            },
            {
                "event_id": "evt-3",
                "parent_event_id": "evt-2",
                "timestamp": "2026-06-17T12:02:00Z",
                "event_type": "rollback_initiated",
                "deployment_id": "dep-1",
                "details": {"reason": "Health check failed at stage 0"},
            },
            {
                "event_id": "evt-4",
                "parent_event_id": "evt-3",
                "timestamp": "2026-06-17T12:03:00Z",
                "event_type": "rollback_complete",
                "deployment_id": "dep-1",
                "details": {"servers_rolled_back": ["server-001"]},
            },
        ]

        replay = EventReplayEngine()

        # At rollback_initiated, status should be "rolling_back"
        state_at_ri = replay.reconstruct_state_at_step(events, "evt-3")
        assert state_at_ri["deployment_status"] == "rolling_back"

        # At rollback_complete, status should be "rolled_back"
        state_at_rc = replay.reconstruct_state_at_step(events, "evt-4")
        assert state_at_rc["deployment_status"] == "rolled_back"

        # Verify lineage is valid
        is_valid, errors = replay.verify_event_lineage(events)
        assert is_valid is True
        assert len(errors) == 0
