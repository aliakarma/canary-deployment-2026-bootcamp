"""
Thread-safe shared simulator state for the dashboard backend.

Manages the lifecycle of cluster, deployment engine, audit logger,
governance, quarantine, and replay subsystems. All mutations are
serialised through a single lock so that concurrent API requests
and the background rollout thread never race.
"""

from __future__ import annotations

import datetime
import os
import threading
from typing import Any, Dict, List

from cluster.generator import generate_cluster
from cluster.models import ServerStatus
from cluster.state import ClusterState
from deploy.audit import AuditLogger, DeploymentEvent, DeploymentEventType
from deploy.config import DeploymentConfig
from deploy.engine import DeploymentEngine
from deploy.state import DeploymentState
from governance import GovernanceCoordinator
from governance.risk import RiskEngine
from health import HealthThresholds, create_health_check_fn, inject_failures
from logging_config import get_logger
from resilience.observability import OperationalObservabilityLayer
from resilience.quarantine import RegionQuarantineSystem
from resilience.replay import EventReplayEngine

logger = get_logger(__name__)

# Fixed business-hours clock used so governance restricted-window policies
# behave deterministically regardless of when the dashboard is demoed.
DEMO_CLOCK = datetime.datetime(2026, 6, 17, 10, 0)  # Wednesday morning (allowed)
WEEKEND_CLOCK = datetime.datetime(2026, 6, 21, 15, 0)  # Sunday afternoon (blocked)

AUDIT_FILE = os.path.join("logs", "dashboard_audit_trail.jsonl")

# Auto-quarantine when (degraded + failed) servers in a region exceed this share.
QUARANTINE_THRESHOLD = 30.0

# Relative health weights used to compute a single cluster-wide health score.
_HEALTH_WEIGHTS = {
    ServerStatus.HEALTHY: 1.0,
    ServerStatus.UPDATING: 0.75,
    ServerStatus.DEGRADED: 0.4,
    ServerStatus.FAILED: 0.0,
}


class SimulatorState:
    """Thread-safe container for all simulator subsystems.

    Designed to be instantiated once and shared across the FastAPI app.
    Every public method acquires ``_lock`` before touching internal state.
    """

    def __init__(self, cluster_size: int = 30, seed: int = 2026) -> None:
        self._lock = threading.Lock()
        self._cluster_size = cluster_size
        self._seed = seed

        self._cluster: ClusterState | None = None
        self._engine: DeploymentEngine | None = None
        self._audit_logger: AuditLogger | None = None
        self._quarantine: RegionQuarantineSystem | None = None
        self._replay: EventReplayEngine = EventReplayEngine()
        self._observability: OperationalObservabilityLayer = OperationalObservabilityLayer()
        self._risk_engine: RiskEngine = RiskEngine()
        self._rollout_thread: threading.Thread | None = None
        self._abort_event: threading.Event | None = None
        self._events: List[Dict[str, Any]] = []

        self._init_subsystems()

    def _init_subsystems(self) -> None:
        """Initialise cluster, engine, audit, and quarantine subsystems."""
        if os.path.exists(AUDIT_FILE):
            try:
                os.remove(AUDIT_FILE)
            except OSError:
                pass

        servers = generate_cluster(size=self._cluster_size, seed=self._seed)
        self._cluster = ClusterState(servers)
        self._engine = DeploymentEngine(self._cluster)
        self._audit_logger = AuditLogger(file_path=AUDIT_FILE)
        self._quarantine = RegionQuarantineSystem(self._cluster)
        self._abort_event = None
        logger.info("Dashboard simulator state initialised (%d servers)", self._cluster_size)

    # ------------------------------------------------------------------
    # Cluster state
    # ------------------------------------------------------------------

    def get_cluster_data(self) -> Dict[str, Any]:
        """Return a serialisable snapshot of the cluster."""
        with self._lock:
            assert self._cluster is not None
            assert self._quarantine is not None

            servers = self._cluster.servers
            by_region: Dict[str, List[Dict[str, Any]]] = {}
            for s in servers:
                by_region.setdefault(s.region, []).append(s.to_dict())

            summary = self._cluster.get_deployment_summary()
            quarantined = list(self._quarantine.get_quarantined_regions())

            deployment = self._engine.current_deployment if self._engine else None
            dep_data: Dict[str, Any] | None = None
            if deployment is not None:
                dep_data = deployment.to_dict()

            return {
                "total_servers": self._cluster.size,
                "regions": by_region,
                "versions": summary["versions"],
                "statuses": summary["statuses"],
                "quarantined_regions": quarantined,
                "rollout_active": self._is_rollout_active(),
                "deployment": dep_data,
            }

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def get_health_data(self) -> Dict[str, Any]:
        """Return aggregate health metrics, a cluster health score, and risk."""
        with self._lock:
            assert self._cluster is not None

            servers = self._cluster.servers
            total = len(servers)
            degraded = sum(1 for s in servers if s.status == ServerStatus.DEGRADED)
            failed = sum(1 for s in servers if s.status == ServerStatus.FAILED)
            healthy = sum(1 for s in servers if s.status == ServerStatus.HEALTHY)
            updating = sum(1 for s in servers if s.status == ServerStatus.UPDATING)

            weighted = sum(_HEALTH_WEIGHTS.get(s.status, 0.0) for s in servers)
            health_score = round((weighted / total) * 100.0, 1) if total else 100.0

            risk_score, risk_category = self._calculate_risk_locked()

            deployment = self._engine.current_deployment if self._engine else None
            rollout_pct = round(deployment.progress_percentage, 1) if deployment else 0.0

            return {
                "total_servers": total,
                "healthy": healthy,
                "degraded": degraded,
                "failed": failed,
                "updating": updating,
                "health_score": health_score,
                "rollout_percentage": rollout_pct,
                "risk_score": round(risk_score, 1),
                "risk_category": risk_category,
            }

    def _calculate_risk_locked(self) -> tuple[float, str]:
        """Compute risk score/category. Assumes ``_lock`` is held.

        When no deployment has run yet, a transient zero-progress deployment is
        used so that pure chaos injection (degraded/failed nodes) still drives a
        visible, non-zero risk reading.
        """
        assert self._cluster is not None
        assert self._engine is not None

        deployment = self._engine.current_deployment
        if deployment is None:
            deployment = DeploymentState(
                deployment_id="cluster-baseline",
                target_version="-",
                source_version="-",
                total_servers=self._cluster.size,
            )
        score, category = self._risk_engine.calculate_risk(self._cluster, deployment)
        return score, category.value

    # ------------------------------------------------------------------
    # Governance
    # ------------------------------------------------------------------

    def get_governance_data(self) -> Dict[str, Any]:
        """Return live governance signals for the operational console."""
        with self._lock:
            assert self._audit_logger is not None
            assert self._quarantine is not None

            risk_score, risk_category = self._calculate_risk_locked()

            event_dicts = [e.to_dict() for e in self._audit_logger.get_events()]
            metrics = self._observability.aggregate_metrics(event_dicts)

            recent_decisions = [
                {
                    "decision": e["details"].get("decision"),
                    "checkpoint": e["details"].get("checkpoint"),
                    "reason": e["details"].get("reason"),
                    "timestamp": e.get("timestamp"),
                }
                for e in event_dicts
                if e.get("event_type") == "governance_decision"
            ][-6:]

            recent_violations = [
                {
                    "policy": e["details"].get("policy_name"),
                    "message": e["details"].get("message"),
                    "timestamp": e.get("timestamp"),
                }
                for e in event_dicts
                if e.get("event_type") == "policy_violation"
            ][-6:]

            quarantines = [
                q.to_dict()
                for q in self._quarantine.get_all_quarantine_states()
                if q.status.value == "active"
            ]

            restricted, restricted_reason = self._restricted_window(DEMO_CLOCK)

            deployment = self._engine.current_deployment if self._engine else None

            return {
                "risk_score": round(risk_score, 1),
                "risk_category": risk_category,
                "deployment_status": deployment.status.value if deployment else None,
                "restricted_window": restricted,
                "restricted_reason": restricted_reason,
                "policy_violations": metrics["policy_violations"],
                "approval_requests": metrics["approval_requests"],
                "approvals_approved": metrics["approvals_approved"],
                "approvals_denied": metrics["approvals_denied"],
                "quarantined_regions": [q["region"] for q in quarantines],
                "quarantines": quarantines,
                "recent_decisions": recent_decisions,
                "recent_violations": recent_violations,
            }

    @staticmethod
    def _restricted_window(clock: datetime.datetime) -> tuple[bool, str]:
        """Mirror ``RiskPolicy`` restricted-window logic for display purposes."""
        weekday = clock.isoweekday()
        if weekday in (6, 7):
            return True, "Weekend change freeze in effect."
        if weekday == 5 and clock.hour >= 15:
            return True, "Friday afternoon change freeze in effect."
        return False, "Within approved change window."

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def get_events_data(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Return the most recent audit events, annotated with a severity."""
        with self._lock:
            assert self._audit_logger is not None
            events = self._audit_logger.get_events()
            result = []
            for e in events[-limit:]:
                d = e.to_dict()
                d["severity"] = self._event_severity(d)
                result.append(d)
            return result

    @staticmethod
    def _event_severity(event: Dict[str, Any]) -> str:
        """Classify an event into info | success | warning | critical."""
        etype = event.get("event_type", "")
        details = event.get("details", {}) or {}

        critical = {
            "deployment_failed",
            "policy_violation",
            "rollback_initiated",
            "rollback_start",
            "abort_received",
            "quarantine_activate",
        }
        success = {"deployment_completed", "rollback_complete", "quarantine_release"}

        if etype in critical:
            return "critical"
        if etype in success:
            return "success"
        if etype == "health_check" and details.get("status") == "fail":
            return "warning"
        if etype == "governance_decision" and details.get("decision") in ("BLOCK", "ROLLBACK"):
            return "critical"
        if etype == "risk_score_transition" and details.get("new_category") in ("HIGH", "CRITICAL"):
            return "warning"
        if etype in ("approval_request", "recovery_plan_execute"):
            return "warning"
        return "info"

    # ------------------------------------------------------------------
    # Rollout
    # ------------------------------------------------------------------

    def _is_rollout_active(self) -> bool:
        """Return ``True`` if the background rollout thread is still running."""
        return self._rollout_thread is not None and self._rollout_thread.is_alive()

    def start_rollout(
        self,
        target_version: str = "2.0.0",
        stages: List[int] | None = None,
    ) -> Dict[str, Any]:
        """Kick off a progressive rollout on a background thread."""
        with self._lock:
            assert self._cluster is not None
            assert self._engine is not None
            assert self._audit_logger is not None
            assert self._quarantine is not None

            if self._is_rollout_active():
                return {"status": "error", "message": "Rollout already in progress"}

            thresholds = HealthThresholds()
            base_health_fn = create_health_check_fn(thresholds)
            quarantine = self._quarantine
            audit_logger = self._audit_logger

            def health_check_with_quarantine(cs: ClusterState) -> bool:
                is_healthy = base_health_fn(cs)
                newly = quarantine.check_and_auto_quarantine(
                    threshold_percentage=QUARANTINE_THRESHOLD
                )
                for region in newly:
                    audit_logger.log(
                        DeploymentEvent(
                            event_type=DeploymentEventType.QUARANTINE_ACTIVATE,
                            deployment_id="rollout",
                            details={
                                "region": region,
                                "reason": f"Auto-quarantine during rollout: {region} unstable",
                            },
                        )
                    )
                return is_healthy

            abort_event = threading.Event()
            self._abort_event = abort_event

            config = DeploymentConfig(
                target_version=target_version,
                stages=stages or [10, 25, 50, 75, 100],
                stage_delay_seconds=2.0,
                health_check_fn=health_check_with_quarantine,
                abort_event=abort_event,
                audit_logger=self._audit_logger,
                governance_coordinator=GovernanceCoordinator(
                    quarantine_system=self._quarantine,
                ),
                quarantine_system=self._quarantine,
                current_time=DEMO_CLOCK,
            )

            engine = self._engine

            def _run_deployment() -> None:
                # A daemon worker must never surface an unhandled exception:
                # catch everything and record it on the audit trail instead.
                try:
                    engine.deploy(config)
                except Exception as exc:  # noqa: BLE001 - defensive worker boundary
                    logger.exception("Rollout worker crashed: %s", exc)

            self._rollout_thread = threading.Thread(
                target=_run_deployment,
                daemon=True,
                name="DashboardRolloutThread",
            )
            self._rollout_thread.start()
            return {"status": "started", "target_version": target_version}

    # ------------------------------------------------------------------
    # Rollback
    # ------------------------------------------------------------------

    def trigger_rollback(self) -> Dict[str, Any]:
        """Roll back the current deployment.

        If a rollout is still running, signalling its abort event is the safe
        path: the engine performs its own rollback on the rollout thread rather
        than a second thread mutating the same deployment concurrently.
        """
        with self._lock:
            assert self._engine is not None
            assert self._audit_logger is not None

            if self._is_rollout_active() and self._abort_event is not None:
                self._abort_event.set()
                return {
                    "status": "aborting",
                    "message": "Abort signalled — engine is rolling back the active rollout",
                }

            deployment = self._engine.current_deployment
            if deployment is None:
                return {"status": "error", "message": "No deployment to rollback"}

            try:
                rolled_back = self._engine.rollback(
                    deployment, force=True, audit_logger=self._audit_logger
                )
                return {
                    "status": "rolled_back",
                    "servers_reverted": len(rolled_back),
                }
            except Exception as exc:
                return {"status": "error", "message": str(exc)}

    # ------------------------------------------------------------------
    # Chaos injection
    # ------------------------------------------------------------------

    def inject_failure(
        self,
        failure_type: str = "degrade",
        failure_rate: float = 0.3,
        target_version: str | None = None,
    ) -> Dict[str, Any]:
        """Inject controlled chaos into the cluster."""
        with self._lock:
            assert self._cluster is not None

            # Auto-target the active deployment target version if none is specified
            if target_version is None and self._engine and self._engine.current_deployment:
                target_version = self._engine.current_deployment.target_version

            try:
                affected = inject_failures(
                    self._cluster,
                    target_version=target_version,
                    failure_rate=failure_rate,
                    failure_type=failure_type,
                    seed=42,
                )
                if self._audit_logger is not None:
                    dep = self._engine.current_deployment if self._engine else None
                    dep_id = dep.deployment_id if dep else "manual"
                    self._audit_logger.log(
                        DeploymentEvent(
                            event_type=DeploymentEventType.HEALTH_CHECK,
                            deployment_id=dep_id,
                            details={
                                "chaos_injection": True,
                                "failure_type": failure_type,
                                "failure_rate": failure_rate,
                                "affected_servers": affected,
                            },
                        )
                    )

                # Surfacing containment immediately makes the "blast-radius
                # reduction" story visible even without an active rollout.
                quarantined = self._auto_quarantine_locked("Chaos injection containment")

                return {
                    "status": "injected",
                    "failure_type": failure_type,
                    "affected_count": len(affected),
                    "affected_servers": affected,
                    "quarantined_regions": quarantined,
                }
            except ValueError as exc:
                return {"status": "error", "message": str(exc)}

    def _auto_quarantine_locked(self, reason_prefix: str) -> List[str]:
        """Run auto-quarantine and emit audit events. Assumes ``_lock`` held."""
        assert self._quarantine is not None
        newly = self._quarantine.check_and_auto_quarantine(
            threshold_percentage=QUARANTINE_THRESHOLD
        )
        if newly and self._audit_logger is not None:
            for region in newly:
                self._audit_logger.log(
                    DeploymentEvent(
                        event_type=DeploymentEventType.QUARANTINE_ACTIVATE,
                        deployment_id="manual",
                        details={
                            "region": region,
                            "reason": f"{reason_prefix}: region '{region}' isolated",
                        },
                    )
                )
        return newly

    # ------------------------------------------------------------------
    # Demo scenarios (one-click, minimal typing during presentations)
    # ------------------------------------------------------------------

    def run_scenario(self, name: str, region: str | None = None) -> Dict[str, Any]:
        """Dispatch a preconfigured demonstration scenario."""
        if name == "successful_rollout":
            return self.start_rollout()
        if name == "regional_failure":
            return self._scenario_regional_failure(region or "us-east-1")
        if name == "governance_block":
            return self._scenario_governance_block()
        return {"status": "error", "message": f"Unknown scenario '{name}'"}

    def _scenario_regional_failure(self, region: str) -> Dict[str, Any]:
        """Fail every node in a region and quarantine it to contain the blast."""
        with self._lock:
            assert self._cluster is not None
            assert self._audit_logger is not None

            affected = [s.id for s in self._cluster.servers if s.region == region]
            if not affected:
                return {"status": "error", "message": f"Unknown region '{region}'"}

            for s_id in affected:
                self._cluster.update_server_status(s_id, ServerStatus.FAILED)
                self._cluster.update_server_resources(s_id, 99.0, 97.0)

            dep = self._engine.current_deployment if self._engine else None
            self._audit_logger.log(
                DeploymentEvent(
                    event_type=DeploymentEventType.HEALTH_CHECK,
                    deployment_id=dep.deployment_id if dep else "manual",
                    details={
                        "chaos_injection": True,
                        "failure_type": "region_outage",
                        "region": region,
                        "affected_servers": affected,
                        "reason": f"Simulated regional outage in {region}",
                    },
                )
            )
            quarantined = self._auto_quarantine_locked("Regional outage containment")

            return {
                "status": "region_failed",
                "region": region,
                "affected_count": len(affected),
                "quarantined_regions": quarantined,
            }

    def _scenario_governance_block(self) -> Dict[str, Any]:
        """Run a rollout pinned to a weekend so governance blocks it at start."""
        with self._lock:
            assert self._engine is not None
            assert self._audit_logger is not None

            if self._is_rollout_active():
                return {
                    "status": "error",
                    "message": "Finish or roll back the active rollout first",
                }

            config = DeploymentConfig(
                target_version="2.0.0",
                stages=[10, 50, 100],
                stage_delay_seconds=0.1,
                health_check_fn=create_health_check_fn(HealthThresholds()),
                governance_coordinator=GovernanceCoordinator(),
                audit_logger=self._audit_logger,
                current_time=WEEKEND_CLOCK,
            )
            result = self._engine.deploy(config)
            return {
                "status": "blocked" if result.status.value == "failed" else result.status.value,
                "message": result.error_message or "Deployment evaluated by governance",
            }

    # ------------------------------------------------------------------
    # Replay
    # ------------------------------------------------------------------

    def replay_event(self, event_id: str) -> Dict[str, Any]:
        """Reconstruct cluster state at a specific audit event."""
        with self._lock:
            return self._replay_locked(event_id)

    def replay_latest(self) -> Dict[str, Any]:
        """Reconstruct cluster state at the most recent audit event."""
        with self._lock:
            if not os.path.exists(AUDIT_FILE):
                return {"status": "error", "message": "No audit trail file found"}
            events = self._replay.load_audit_trail(AUDIT_FILE)
            if not events:
                return {"status": "error", "message": "Audit trail is empty"}
            last_id = events[-1].get("event_id")
            if not last_id:
                return {"status": "error", "message": "Latest event has no id"}
            result = self._replay_locked(last_id)
            result["event_id"] = last_id
            return result

    def _replay_locked(self, event_id: str) -> Dict[str, Any]:
        """Reconstruct state at ``event_id``. Assumes ``_lock`` is held."""
        if not os.path.exists(AUDIT_FILE):
            return {"status": "error", "message": "No audit trail file found"}

        events = self._replay.load_audit_trail(AUDIT_FILE)
        if not events:
            return {"status": "error", "message": "Audit trail is empty"}

        try:
            target_ev = next((e for e in events if e.get("event_id") == event_id), None)
            dep_id = target_ev.get("deployment_id") if target_ev else None
            state = self._replay.reconstruct_state_at_step(events, event_id, deployment_id=dep_id)
            return {"status": "ok", "reconstructed_state": state}
        except ValueError as exc:
            return {"status": "error", "message": str(exc)}

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------

    def reset(self) -> Dict[str, Any]:
        """Reset all simulator state to fresh defaults."""
        with self._lock:
            if self._is_rollout_active():
                return {"status": "error", "message": "Cannot reset while rollout is active"}
            if self._audit_logger is not None:
                self._audit_logger.close()
            self._init_subsystems()
            return {"status": "reset"}

    def shutdown(self) -> None:
        """Gracefully abort and join any running rollout thread."""
        with self._lock:
            if self._abort_event is not None:
                self._abort_event.set()

        if self._rollout_thread is not None and self._rollout_thread.is_alive():
            self._rollout_thread.join(timeout=5.0)
