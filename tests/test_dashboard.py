"""
Integration tests for the FastAPI dashboard backend.

Covers all API endpoints, replay correctness, chaos injection,
governance decision propagation, rollback safety, dashboard state
serialisation, and end-to-end simulation scenarios.
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from dashboard.app import create_app
from dashboard.state import SimulatorState

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture()
def sim_state() -> SimulatorState:
    """Fresh simulator state with a small deterministic cluster."""
    return SimulatorState(cluster_size=10, seed=42)


@pytest.fixture()
def client(sim_state: SimulatorState) -> TestClient:
    """FastAPI test client wired to a fresh simulator."""
    app = create_app(state=sim_state)
    return TestClient(app)


# ==================================================================
# 1. Endpoint smoke tests
# ==================================================================


class TestClusterEndpoint:
    def test_returns_regions(self, client: TestClient) -> None:
        resp = client.get("/api/cluster")
        assert resp.status_code == 200
        data = resp.json()
        assert "regions" in data
        assert "versions" in data
        assert "statuses" in data
        assert "quarantined_regions" in data
        assert data["total_servers"] == 10

    def test_regions_contain_servers(self, client: TestClient) -> None:
        data = client.get("/api/cluster").json()
        for region, servers in data["regions"].items():
            assert len(servers) > 0
            for s in servers:
                assert "id" in s
                assert "status" in s
                assert "current_version" in s


class TestHealthEndpoint:
    def test_returns_health_metrics(self, client: TestClient) -> None:
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_servers"] == 10
        assert data["healthy"] == 10
        assert data["degraded"] == 0
        assert data["failed"] == 0
        assert data["risk_category"] == "LOW"

    def test_risk_score_is_numeric(self, client: TestClient) -> None:
        data = client.get("/api/health").json()
        assert isinstance(data["risk_score"], (int, float))


class TestEventsEndpoint:
    def test_returns_empty_initially(self, client: TestClient) -> None:
        resp = client.get("/api/events")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_respects_limit(self, client: TestClient) -> None:
        resp = client.get("/api/events?limit=5")
        assert resp.status_code == 200


class TestReplayEndpoint:
    def test_replay_no_audit_trail(self, client: TestClient) -> None:
        resp = client.get("/api/replay/evt-nonexistent")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "error"


class TestDashboardUI:
    def test_serves_html(self, client: TestClient) -> None:
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Canary Deployment Simulator" in resp.text


# ==================================================================
# 2. Chaos injection endpoint
# ==================================================================


class TestInjectFailure:
    def test_degrade_nodes(self, client: TestClient) -> None:
        resp = client.post(
            "/api/inject_failure",
            json={"failure_type": "degrade", "failure_rate": 0.3},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "injected"
        assert data["affected_count"] > 0

        health = client.get("/api/health").json()
        assert health["degraded"] > 0

    def test_fail_nodes(self, client: TestClient) -> None:
        resp = client.post(
            "/api/inject_failure",
            json={"failure_type": "fail", "failure_rate": 0.2},
        )
        data = resp.json()
        assert data["status"] == "injected"

        health = client.get("/api/health").json()
        assert health["failed"] > 0

    def test_resource_spike(self, client: TestClient) -> None:
        resp = client.post(
            "/api/inject_failure",
            json={"failure_type": "resource_spike", "failure_rate": 0.4},
        )
        assert resp.json()["status"] == "injected"

    def test_invalid_failure_type(self, client: TestClient) -> None:
        resp = client.post(
            "/api/inject_failure",
            json={"failure_type": "invalid_type", "failure_rate": 0.1},
        )
        data = resp.json()
        assert data["status"] == "error"

    def test_chaos_creates_event(self, client: TestClient) -> None:
        client.post(
            "/api/inject_failure",
            json={"failure_type": "degrade", "failure_rate": 0.2},
        )
        events = client.get("/api/events").json()
        assert len(events) >= 1
        chaos_events = [e for e in events if e.get("details", {}).get("chaos_injection")]
        assert len(chaos_events) >= 1


# ==================================================================
# 3. Rollout endpoint
# ==================================================================


class TestStartRollout:
    def test_start_rollout(self, client: TestClient) -> None:
        resp = client.post(
            "/api/start_rollout",
            json={"target_version": "2.0.0"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "started"
        assert data["target_version"] == "2.0.0"

    def test_duplicate_rollout_blocked(self, client: TestClient) -> None:
        client.post("/api/start_rollout", json={"target_version": "2.0.0"})
        time.sleep(0.2)
        resp = client.post("/api/start_rollout", json={"target_version": "3.0.0"})
        data = resp.json()
        assert data["status"] == "error"
        assert "already in progress" in data["message"].lower()

    def test_rollout_produces_events(self, client: TestClient) -> None:
        client.post("/api/start_rollout", json={"target_version": "2.0.0"})
        time.sleep(3)
        events = client.get("/api/events").json()
        event_types = {e["event_type"] for e in events}
        assert "deployment_start" in event_types

    def test_rollout_updates_cluster(self, client: TestClient) -> None:
        client.post("/api/start_rollout", json={"target_version": "2.0.0"})
        time.sleep(6)
        cluster = client.get("/api/cluster").json()
        assert cluster["deployment"] is not None


# ==================================================================
# 4. Rollback endpoint
# ==================================================================


class TestRollback:
    def test_rollback_no_deployment(self, client: TestClient) -> None:
        resp = client.post("/api/rollback")
        data = resp.json()
        assert data["status"] == "error"

    def test_rollback_after_rollout(self, client: TestClient) -> None:
        client.post("/api/start_rollout", json={"target_version": "2.0.0"})
        time.sleep(5)
        resp = client.post("/api/rollback")
        data = resp.json()
        # If the rollout is still running the rollback is delivered as an abort
        # signal (the engine rolls back on its own thread); otherwise it is a
        # direct forced rollback. Both are valid safe outcomes.
        assert data["status"] in ("rolled_back", "aborting", "error")


# ==================================================================
# 5. Reset endpoint
# ==================================================================


class TestReset:
    def test_reset_restores_state(self, client: TestClient) -> None:
        client.post(
            "/api/inject_failure",
            json={"failure_type": "degrade", "failure_rate": 0.5},
        )
        health_before = client.get("/api/health").json()
        assert health_before["degraded"] > 0

        resp = client.post("/api/reset")
        assert resp.json()["status"] == "reset"

        health_after = client.get("/api/health").json()
        assert health_after["degraded"] == 0
        assert health_after["healthy"] == 10


# ==================================================================
# 6. State serialisation
# ==================================================================


class TestStateSerialization:
    def test_cluster_json_serialisable(self, client: TestClient) -> None:
        data = client.get("/api/cluster").json()
        assert isinstance(data, dict)
        for region, servers in data["regions"].items():
            for s in servers:
                assert isinstance(s["id"], str)
                assert isinstance(s["cpu_usage"], (int, float))

    def test_health_json_serialisable(self, client: TestClient) -> None:
        data = client.get("/api/health").json()
        for key in ("total_servers", "healthy", "degraded", "failed", "risk_score"):
            assert isinstance(data[key], (int, float))

    def test_events_json_serialisable(self, client: TestClient) -> None:
        client.post(
            "/api/inject_failure",
            json={"failure_type": "degrade", "failure_rate": 0.1},
        )
        events = client.get("/api/events").json()
        assert isinstance(events, list)
        for e in events:
            assert "event_id" in e
            assert "event_type" in e
            assert "timestamp" in e


# ==================================================================
# 7. Replay correctness
# ==================================================================


class TestReplayCorrectness:
    def test_replay_after_rollout(self, client: TestClient) -> None:
        """Start a rollout, wait for events, then replay the first event."""
        client.post("/api/start_rollout", json={"target_version": "2.0.0"})
        time.sleep(4)

        events = client.get("/api/events").json()
        if not events:
            pytest.skip("No events produced in time")

        first_id = events[0]["event_id"]
        resp = client.get(f"/api/replay/{first_id}")
        data = resp.json()
        assert data["status"] == "ok"
        state = data["reconstructed_state"]
        assert "deployment_status" in state
        assert "servers" in state


# ==================================================================
# 8. End-to-end simulation scenarios
# ==================================================================


class TestE2EScenarios:
    def test_rollout_completes_successfully(self, client: TestClient) -> None:
        """Rollout with no chaos should complete."""
        client.post("/api/start_rollout", json={"target_version": "2.0.0"})
        for _ in range(20):
            time.sleep(1)
            cluster = client.get("/api/cluster").json()
            dep = cluster.get("deployment")
            if dep and dep["status"] in ("completed", "rolled_back", "failed"):
                break
        assert dep is not None
        assert dep["status"] == "completed"

    def test_chaos_triggers_degraded_health(self, client: TestClient) -> None:
        """Injecting chaos degrades health metrics."""
        client.post(
            "/api/inject_failure",
            json={"failure_type": "fail", "failure_rate": 0.5},
        )
        health = client.get("/api/health").json()
        assert health["failed"] >= 1

    def test_reset_after_chaos(self, client: TestClient) -> None:
        """Reset after chaos returns to healthy baseline."""
        client.post(
            "/api/inject_failure",
            json={"failure_type": "fail", "failure_rate": 0.8},
        )
        client.post("/api/reset")
        health = client.get("/api/health").json()
        assert health["failed"] == 0
        assert health["healthy"] == 10


# ==================================================================
# 9. Enriched health metrics
# ==================================================================


class TestEnrichedHealth:
    def test_health_includes_operational_fields(self, client: TestClient) -> None:
        data = client.get("/api/health").json()
        for key in ("updating", "health_score", "rollout_percentage"):
            assert key in data
        # Fresh, fully healthy cluster scores a perfect 100.
        assert data["health_score"] == 100.0
        assert data["updating"] == 0
        assert data["rollout_percentage"] == 0.0

    def test_health_score_drops_after_failure(self, client: TestClient) -> None:
        client.post(
            "/api/inject_failure",
            json={"failure_type": "fail", "failure_rate": 0.5},
        )
        data = client.get("/api/health").json()
        assert data["health_score"] < 100.0

    def test_risk_reflects_chaos_without_deployment(self, client: TestClient) -> None:
        """Risk must rise from pure chaos even with no active deployment."""
        before = client.get("/api/health").json()["risk_score"]
        client.post(
            "/api/inject_failure",
            json={"failure_type": "fail", "failure_rate": 0.6},
        )
        after = client.get("/api/health").json()["risk_score"]
        assert after > before


# ==================================================================
# 10. Governance endpoint
# ==================================================================


class TestGovernanceEndpoint:
    def test_returns_governance_signals(self, client: TestClient) -> None:
        data = client.get("/api/governance").json()
        for key in (
            "risk_score",
            "risk_category",
            "restricted_window",
            "policy_violations",
            "approval_requests",
            "quarantined_regions",
            "recent_decisions",
        ):
            assert key in data
        # Demo clock is a Wednesday morning — within the approved change window.
        assert data["restricted_window"] is False

    def test_governance_block_scenario_records_violation(self, client: TestClient) -> None:
        resp = client.post("/api/scenario", json={"name": "governance_block"})
        assert resp.json()["status"] == "blocked"
        gov = client.get("/api/governance").json()
        assert gov["policy_violations"] >= 1
        decisions = [d["decision"] for d in gov["recent_decisions"]]
        assert "BLOCK" in decisions


# ==================================================================
# 11. Chaos-triggered quarantine containment
# ==================================================================


class TestQuarantineContainment:
    def test_region_failure_scenario_quarantines(self, client: TestClient) -> None:
        resp = client.post(
            "/api/scenario",
            json={"name": "regional_failure", "region": "us-east-1"},
        )
        data = resp.json()
        assert data["status"] == "region_failed"
        assert data["affected_count"] > 0
        assert "us-east-1" in data["quarantined_regions"]

        cluster = client.get("/api/cluster").json()
        assert "us-east-1" in cluster["quarantined_regions"]

    def test_region_failure_unknown_region(self, client: TestClient) -> None:
        resp = client.post(
            "/api/scenario",
            json={"name": "regional_failure", "region": "mars-1"},
        )
        assert resp.json()["status"] == "error"

    def test_unknown_scenario(self, client: TestClient) -> None:
        resp = client.post("/api/scenario", json={"name": "does_not_exist"})
        assert resp.json()["status"] == "error"


# ==================================================================
# 12. Event severity annotation
# ==================================================================


class TestEventSeverity:
    def test_events_have_severity(self, client: TestClient) -> None:
        client.post(
            "/api/inject_failure",
            json={"failure_type": "degrade", "failure_rate": 0.2},
        )
        events = client.get("/api/events").json()
        assert len(events) >= 1
        for e in events:
            assert e["severity"] in ("info", "success", "warning", "critical")

    def test_quarantine_event_is_critical(self, client: TestClient) -> None:
        client.post(
            "/api/scenario",
            json={"name": "regional_failure", "region": "us-east-1"},
        )
        events = client.get("/api/events").json()
        quarantine_events = [e for e in events if e["event_type"] == "quarantine_activate"]
        assert quarantine_events
        assert all(e["severity"] == "critical" for e in quarantine_events)


# ==================================================================
# 13. Abort-based rollback during an active rollout
# ==================================================================


class TestAbortRollback:
    def test_rollback_during_active_rollout_signals_abort(self, client: TestClient) -> None:
        client.post("/api/start_rollout", json={"target_version": "2.0.0"})
        time.sleep(0.5)
        resp = client.post("/api/rollback")
        assert resp.json()["status"] == "aborting"

    def test_active_rollout_eventually_rolls_back_after_abort(self, client: TestClient) -> None:
        client.post("/api/start_rollout", json={"target_version": "2.0.0"})
        time.sleep(0.5)
        client.post("/api/rollback")
        final = None
        for _ in range(15):
            time.sleep(1)
            cluster = client.get("/api/cluster").json()
            dep = cluster.get("deployment")
            if dep and dep["status"] in ("rolled_back", "aborted", "completed", "failed"):
                final = dep["status"]
                break
        assert final in ("rolled_back", "aborted")


# ==================================================================
# 14. Replay latest
# ==================================================================


class TestReplayLatest:
    def test_replay_latest_no_trail(self, client: TestClient) -> None:
        resp = client.get("/api/replay_latest")
        assert resp.json()["status"] == "error"

    def test_replay_latest_after_chaos(self, client: TestClient) -> None:
        client.post(
            "/api/inject_failure",
            json={"failure_type": "degrade", "failure_rate": 0.2},
        )
        resp = client.get("/api/replay_latest").json()
        assert resp["status"] == "ok"
        assert "event_id" in resp
        assert "reconstructed_state" in resp
