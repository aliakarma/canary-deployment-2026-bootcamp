"""
Unit tests for the Phase 8 Governance Policy Engine & Advanced Operational Control.
"""

from __future__ import annotations

import datetime

import pytest

from cluster.generator import generate_cluster
from cluster.models import ServerStatus
from cluster.state import ClusterState
from deploy.audit import AuditLogger, DeploymentEventType
from deploy.config import DeploymentConfig
from deploy.engine import DeploymentEngine
from deploy.state import DeploymentStatus
from governance.approvals import ApprovalGate
from governance.coordinator import GovernanceCoordinator
from governance.models import (
    ApprovalDecision,
    ApprovalRequest,
    GovernanceDecision,
    RiskScore,
)
from governance.policies import (
    HealthPolicy,
    RiskPolicy,
    RollbackPolicy,
)
from governance.risk import RiskEngine


class TestGovernanceEngine:
    """Comprehensive test suite for Phase 8 Governance."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=10, seed=42))

    # ------------------------------------------------------------------
    # 1. Risk Score calculations
    # ------------------------------------------------------------------

    def test_risk_score_low(self, cluster_state: ClusterState) -> None:
        """Verify standard healthy starting deployment calculates LOW risk."""
        engine = RiskEngine()
        config = DeploymentConfig(target_version="2.0.0", stages=[10, 100])
        dep_engine = DeploymentEngine(cluster_state)
        dep_state = dep_engine._init_deployment(config)

        score, category = engine.calculate_risk(cluster_state, dep_state)
        assert score == 0.0
        assert category == RiskScore.LOW

    def test_risk_score_critical(self, cluster_state: ClusterState) -> None:
        """Verify cluster with high server failures raises risk to CRITICAL."""
        engine = RiskEngine()
        config = DeploymentConfig(target_version="2.0.0", stages=[10, 100])
        dep_engine = DeploymentEngine(cluster_state)
        dep_state = dep_engine._init_deployment(config)

        # Inject failures: degraded and failed servers
        servers = cluster_state.servers
        cluster_state.update_server_status(servers[0].id, ServerStatus.DEGRADED)
        cluster_state.update_server_status(servers[1].id, ServerStatus.FAILED)
        cluster_state.update_server_status(servers[2].id, ServerStatus.FAILED)

        score, category = engine.calculate_risk(cluster_state, dep_state)
        # degraded * 15 (15) + failed * 30 * 2 (60) = 75
        assert score >= 75.0
        # If we also deploy to us-east-1 critical region (say server 0 is us-east-1 and in updated)
        dep_state.servers_updated.add(cluster_state.servers[0].id)
        score, category = engine.calculate_risk(cluster_state, dep_state)
        assert score > 75.0
        assert category == RiskScore.CRITICAL

    # ------------------------------------------------------------------
    # 2. Approval Gate
    # ------------------------------------------------------------------

    def test_approval_gate_bypassed(self) -> None:
        """Verify approval gate is bypassed for low risk scores."""
        gate = ApprovalGate()
        req = ApprovalRequest(
            request_id="req-1",
            deployment_id="dep-1",
            stage_index=0,
            reason="Routine check",
        )
        decision = gate.evaluate_request(req, "LOW")
        assert decision == ApprovalDecision.BYPASSED

    def test_approval_gate_callback(self) -> None:
        """Verify custom callbacks decide approvals or denials."""
        # Setup callback that approves
        gate_approve = ApprovalGate(callback=lambda r: True)
        req1 = ApprovalRequest("req-1", "dep-1", 0, "Needs human validation")
        assert gate_approve.evaluate_request(req1, "HIGH") == ApprovalDecision.APPROVED

        # Setup callback that denies
        gate_deny = ApprovalGate(callback=lambda r: False)
        req2 = ApprovalRequest("req-2", "dep-1", 0, "Needs human validation")
        assert gate_deny.evaluate_request(req2, "HIGH") == ApprovalDecision.DENIED

    # ------------------------------------------------------------------
    # 3. Policy Boundaries
    # ------------------------------------------------------------------

    def test_policy_rollback_governance(self, cluster_state: ClusterState) -> None:
        """Verify RollbackPolicy suspends auto-rollback on CRITICAL risk levels."""
        policy = RollbackPolicy()
        config = DeploymentConfig(target_version="2.0.0", stages=[10, 100])
        dep_engine = DeploymentEngine(cluster_state)
        dep_state = dep_engine._init_deployment(config)

        # LOW risk passes
        res1 = policy.evaluate(cluster_state, dep_state, {"risk_category": RiskScore.LOW})
        assert res1.passed is True
        assert res1.decision == GovernanceDecision.ALLOW

        # CRITICAL risk blocks rollback
        res2 = policy.evaluate(cluster_state, dep_state, {"risk_category": RiskScore.CRITICAL})
        assert res2.passed is False
        assert res2.decision == GovernanceDecision.BLOCK

    def test_policy_health_critical_region(self, cluster_state: ClusterState) -> None:
        """Verify HealthPolicy triggers ROLLBACK if critical region us-east-1 gets degraded."""
        policy = HealthPolicy(zero_degraded_regions={"us-east-1"})
        config = DeploymentConfig(target_version="2.0.0", stages=[100])
        dep_engine = DeploymentEngine(cluster_state)
        dep_state = dep_engine._init_deployment(config)

        # Re-initialize updated servers
        us_east_server = [s for s in cluster_state.servers if s.region == "us-east-1"][0]
        dep_state.servers_updated.add(us_east_server.id)

        # Healthy passes
        res1 = policy.evaluate(cluster_state, dep_state, {})
        assert res1.passed is True

        # Degraded us-east-1 server violates health policy
        us_east_server.status = ServerStatus.DEGRADED
        res2 = policy.evaluate(cluster_state, dep_state, {})
        assert res2.passed is False
        assert res2.decision == GovernanceDecision.ROLLBACK
        assert "zero-degradation" in res2.message

    def test_policy_deployment_window_friday(self, cluster_state: ClusterState) -> None:
        """Verify RiskPolicy blocks Friday afternoon and weekend rollouts."""
        policy = RiskPolicy()
        config = DeploymentConfig(target_version="2.0.0", stages=[100])
        dep_engine = DeploymentEngine(cluster_state)
        dep_state = dep_engine._init_deployment(config)

        # Wednesday morning passes
        wednesday = datetime.datetime(2026, 6, 17, 10, 0, 0)  # Wednesday
        res1 = policy.evaluate(cluster_state, dep_state, {"current_time": wednesday})
        assert res1.passed is True

        # Friday 16:00 is blocked
        friday_afternoon = datetime.datetime(2026, 6, 19, 16, 0, 0)
        res2 = policy.evaluate(cluster_state, dep_state, {"current_time": friday_afternoon})
        assert res2.passed is False
        assert res2.decision == GovernanceDecision.BLOCK

        # Sunday morning is blocked
        sunday = datetime.datetime(2026, 6, 21, 9, 0, 0)
        res3 = policy.evaluate(cluster_state, dep_state, {"current_time": sunday})
        assert res3.passed is False
        assert res3.decision == GovernanceDecision.BLOCK

    # ------------------------------------------------------------------
    # 4. Integration checkpoints & Enforcement
    # ------------------------------------------------------------------

    def test_governance_block_start(self, cluster_state: ClusterState) -> None:
        """Verify rollout fails immediately at start checkpoint if window is blocked."""
        audit = AuditLogger()
        friday_afternoon = datetime.datetime(2026, 6, 19, 17, 0, 0)

        # Coordinator with a RiskPolicy that will block
        coordinator = GovernanceCoordinator()
        # Mocking time to Friday afternoon in coordinator context calculations
        coordinator._build_context = lambda cs, ds, current_time=None, retry_count=0: {
            "risk_score": 0.0,
            "risk_category": RiskScore.LOW,
            "current_time": friday_afternoon,
        }

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[10, 100],
            stage_delay_seconds=0.0,
            audit_logger=audit,
            governance_coordinator=coordinator,
        )
        engine = DeploymentEngine(cluster_state)
        res = engine.deploy(config)

        assert res.status == DeploymentStatus.FAILED
        assert "blocked by governance start policy" in res.error_message

        # Assert policy_violation event is logged
        events = audit.get_events()
        event_types = [e.event_type for e in events]
        assert DeploymentEventType.POLICY_VIOLATION in event_types
        assert DeploymentEventType.GOVERNANCE_DECISION in event_types

    def test_governance_stage_start_denied_approval(self, cluster_state: ClusterState) -> None:
        """Verify rollout blocks when manual stage approval is denied."""
        audit = AuditLogger()

        # ApprovalGate that denies all requests
        gate = ApprovalGate(callback=lambda r: False)
        # ApprovalPolicy enforces gate at 75% progress
        coordinator = GovernanceCoordinator(approval_gate=gate)

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 80, 100],
            stage_delay_seconds=0.0,
            audit_logger=audit,
            governance_coordinator=coordinator,
            # Pin to a weekday morning so the default RiskPolicy's restricted
            # window does not block the run regardless of when tests execute.
            current_time=datetime.datetime(2026, 6, 17, 10, 0),
        )
        engine = DeploymentEngine(cluster_state)
        res = engine.deploy(config)

        # Should pass stage 0 (50%), but fail stage 1 (80% which triggers approval gate check)
        assert res.status == DeploymentStatus.FAILED
        assert "Stage 1 blocked post-execution by governance" in res.error_message

        events = audit.get_events()
        event_types = [e.event_type for e in events]
        assert DeploymentEventType.APPROVAL_REQUEST in event_types
        assert DeploymentEventType.APPROVAL_DECISION in event_types

        # Verify that approval decision details register "DENIED"
        app_dec_event = [
            e for e in events if e.event_type == DeploymentEventType.APPROVAL_DECISION
        ][0]
        assert app_dec_event.details["decision"] == "DENIED"

    def test_governance_blocked_auto_rollback(self, cluster_state: ClusterState) -> None:
        """Verify auto-rollback gets blocked under CRITICAL risk conditions, preserving partial state."""
        audit = AuditLogger()

        # Config triggers immediate failure on first health check
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
            health_check_fn=lambda cs: False,
            max_retries_per_stage=0,
            audit_logger=audit,
            # Pin to a weekday morning so the default RiskPolicy's restricted
            # window does not block the run regardless of when tests execute.
            current_time=datetime.datetime(2026, 6, 17, 10, 0),
        )

        # Setup coordinator where rollback policy blocks rollback
        # We can simulate this by forcing risk score to CRITICAL on the rollback evaluation
        coordinator = GovernanceCoordinator()

        # Force the rollback checkpoint to BLOCK, simulating a CRITICAL-risk
        # auto-rollback suspension. Mirrors the coordinator's public signature
        # (which accepts current_time / audit_logger keyword arguments).
        coordinator.evaluate_rollback = (
            lambda cs, ds, current_time=None, audit_logger=None: GovernanceDecision.BLOCK
        )
        config.governance_coordinator = coordinator

        engine = DeploymentEngine(cluster_state)

        # Deploy should raise GovernanceViolationError or mark state appropriately
        # Because the rollback threw GovernanceViolationError, engine traps it and marks deployment FAILED
        res = engine.deploy(config)

        assert res.status == DeploymentStatus.FAILED
        assert "rollback blocked by governance policy" in res.error_message.lower()

        # Verify that updated servers were NOT reverted (unsafe rollback prevented!)
        # There should be updated servers running target_version
        updated_servers = [s for s in cluster_state.servers if s.current_version == "2.0.0"]
        assert len(updated_servers) > 0

        # Assert correct audit logs
        events = audit.get_events()
        event_types = [e.event_type for e in events]
        # Rollback shouldn't have ROLLBACK_START because it was blocked before it began
        assert DeploymentEventType.ROLLBACK_START not in event_types
        # Policy violation must be logged
        assert DeploymentEventType.POLICY_VIOLATION in event_types

    # ------------------------------------------------------------------
    # 5. Correlated Event Tracing
    # ------------------------------------------------------------------

    def test_correlated_event_tracing_chain(self, cluster_state: ClusterState) -> None:
        """Verify that sequential event logs maintain correct causal parent-child UUID linkages."""
        audit = AuditLogger()
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
            audit_logger=audit,
        )
        engine = DeploymentEngine(cluster_state)
        engine.deploy(config)

        events = audit.get_events()
        assert len(events) >= 5

        # Root event deployment_start should have no parent_event_id
        assert events[0].event_type == DeploymentEventType.DEPLOYMENT_START
        assert events[0].parent_event_id is None

        # Verify linking chain
        for idx in range(1, len(events)):
            current_ev = events[idx]
            previous_ev = events[idx - 1]

            assert current_ev.parent_event_id == previous_ev.event_id
            assert current_ev.correlation_id == events[0].deployment_id
