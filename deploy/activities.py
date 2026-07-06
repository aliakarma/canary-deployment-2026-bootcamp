from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Set

from temporalio import activity

from cluster.models import ServerStatus
from cluster.state import ClusterState
from deploy.audit import DeploymentEvent, DeploymentEventType
from deploy.state import DeploymentState, DeploymentStatus, StageResult
from governance import GovernanceDecision


class CanaryDeploymentActivities:
    """Class containing all Temporal activities for the canary deployment.

    Instantiated with references to local components (ClusterState, Quarantine, Governance, etc.)
    so activities can interact directly with the simulation in a thread-safe manner.
    """

    def __init__(
        self,
        cluster: ClusterState,
        quarantine_system: Any | None = None,
        governance_coordinator: Any | None = None,
        audit_logger: Any | None = None,
        health_check_fn: Any | None = None,
        on_stage_complete: Any | None = None,
        engine: Any = None,
    ) -> None:
        self.cluster = cluster
        self.quarantine_system = quarantine_system
        self.governance_coordinator = governance_coordinator
        self.audit_logger = audit_logger
        self.health_check_fn = health_check_fn
        self.on_stage_complete = on_stage_complete
        self.engine = engine

    # ------------------------------------------------------------------
    # Helper to construct active deployment state for legacy coordinator APIs
    # ------------------------------------------------------------------
    def _reconstruct_temp_deployment_state(
        self,
        deployment_id: str,
        target_version: str,
        source_version: str,
        total_servers: int,
        updated_ids: List[str],
        status_name: str = "in_progress",
    ) -> DeploymentState:
        """Reconstruct a DeploymentState object to pass to governance check APIs."""
        all_ids = {s.id for s in self.cluster.servers}
        updated_set = set(updated_ids)
        pending_set = all_ids - updated_set
        ds = DeploymentState(
            deployment_id=deployment_id,
            target_version=target_version,
            source_version=source_version,
            total_servers=total_servers,
            servers_updated=updated_set,
            servers_pending=pending_set,
        )
        ds.status = DeploymentStatus(status_name)
        return ds

    # ------------------------------------------------------------------
    # Activities
    # ------------------------------------------------------------------

    @activity.defn
    async def evaluate_start(self, args: Dict[str, Any]) -> str:
        """Evaluate starting governance policy."""
        if self.governance_coordinator is None:
            return "ALLOW"

        deployment_id = args["deployment_id"]
        target_version = args["target_version"]
        source_version = args["source_version"]
        total_servers = args["total_servers"]
        current_time_str = args.get("current_time")

        current_time = (
            datetime.fromisoformat(current_time_str) if current_time_str else datetime.now()
        )

        # Temporary DeploymentState for evaluation
        ds = self._reconstruct_temp_deployment_state(
            deployment_id, target_version, source_version, total_servers, []
        )

        decision = self.governance_coordinator.evaluate_start(
            self.cluster, ds, current_time=current_time, audit_logger=self.audit_logger
        )
        return decision.name

    @activity.defn
    async def evaluate_stage_start(self, args: Dict[str, Any]) -> str:
        """Evaluate pre-stage start policy."""
        if self.governance_coordinator is None:
            return "ALLOW"

        deployment_id = args["deployment_id"]
        target_version = args["target_version"]
        source_version = args["source_version"]
        total_servers = args["total_servers"]
        updated_ids = args["updated_ids"]
        stage_idx = args["stage_idx"]
        target_pct = args["target_pct"]
        current_time_str = args.get("current_time")

        current_time = (
            datetime.fromisoformat(current_time_str) if current_time_str else datetime.now()
        )

        ds = self._reconstruct_temp_deployment_state(
            deployment_id, target_version, source_version, total_servers, updated_ids
        )

        decision = self.governance_coordinator.evaluate_stage_start(
            self.cluster,
            ds,
            stage_idx,
            target_pct,
            current_time=current_time,
            audit_logger=self.audit_logger,
        )
        return decision.name

    @activity.defn
    async def create_snapshot(self, args: Dict[str, Any]) -> str:
        """Create a point-in-time state snapshot."""
        if self.audit_logger is None:
            return "no_audit_logger"

        from resilience.snapshots import ClusterSnapshotSystem

        deployment_id = args["deployment_id"]
        target_version = args["target_version"]
        source_version = args["source_version"]
        total_servers = args["total_servers"]
        updated_ids = args["updated_ids"]
        metadata = args.get("metadata", {})

        ds = self._reconstruct_temp_deployment_state(
            deployment_id, target_version, source_version, total_servers, updated_ids
        )

        snapshot_system = ClusterSnapshotSystem(self.cluster)
        snapshot = snapshot_system.create_snapshot(ds, metadata=metadata)
        return snapshot.snapshot_id

    @activity.defn
    async def execute_stage(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Deploy target version to servers for the active stage."""
        try:
            deployment_id = args["deployment_id"]
            target_version = args["target_version"]
            source_version = args["source_version"]
            total_servers = args["total_servers"]
            updated_ids = args["updated_ids"]
            stage_idx = args["stage_idx"]
            target_pct = args["target_pct"]

            if self.engine is not None:
                ds = self._reconstruct_temp_deployment_state(
                    deployment_id, target_version, source_version, total_servers, updated_ids
                )
                from deploy.config import DeploymentConfig
                config = getattr(self.engine, "_current_config", None)
                if config is None:
                    config = DeploymentConfig(
                        target_version=target_version,
                        stages=args.get("stages", [target_pct]),
                        quarantine_system=self.quarantine_system,
                        governance_coordinator=self.governance_coordinator,
                        health_check_fn=self.health_check_fn,
                        on_stage_complete=self.on_stage_complete,
                    )
                stage_res = self.engine._execute_stage(ds, config, stage_idx, target_pct)
                newly_updated = list(ds.servers_updated - set(updated_ids))
                return {"newly_updated_ids": newly_updated, "error": stage_res.error}

            # Reconstruct state
            ds = self._reconstruct_temp_deployment_state(
                deployment_id, target_version, source_version, total_servers, updated_ids
            )

            # Get quarantined regions
            quarantined = set()
            if self.quarantine_system is not None:
                quarantined = self.quarantine_system.get_quarantined_regions()

            eligible_servers = [s for s in self.cluster.servers if s.region not in quarantined]
            eligible_total = len(eligible_servers)

            eligible_updated = len(
                [
                    s
                    for s in self.cluster.servers
                    if s.id in ds.servers_updated and s.region not in quarantined
                ]
            )

            target_count = math.ceil(eligible_total * target_pct / 100)
            servers_needed = target_count - eligible_updated

            if servers_needed <= 0:
                return {"newly_updated_ids": [], "error": None}

            # Select servers to update
            pending_servers = [
                s
                for s in self.cluster.servers
                if s.id in ds.servers_pending and s.is_updatable and s.region not in quarantined
            ]
            pending_servers.sort(key=lambda s: (s.region, s.id))

            by_region: Dict[str, List[str]] = {}
            for s in pending_servers:
                by_region.setdefault(s.region, []).append(s.id)

            selected: List[str] = []
            regions = list(by_region.keys())
            region_idx = 0

            while len(selected) < servers_needed and any(by_region.values()):
                region = regions[region_idx % len(regions)]
                if by_region[region]:
                    selected.append(by_region[region].pop(0))
                region_idx += 1
                regions = [r for r in regions if by_region.get(r)]
                if not regions:
                    break

            selected = selected[:servers_needed]
            newly_updated = []
            error = None

            for s_id in selected:
                success = self.cluster.update_server_version(s_id, target_version)
                if success:
                    newly_updated.append(s_id)
                else:
                    error = f"Failed to update server {s_id}"

            if not error and len(newly_updated) < servers_needed:
                error = (
                    f"Under-provisioned update: target required updating {servers_needed} servers, "
                    f"but only {len(newly_updated)} were successfully updated."
                )

            return {"newly_updated_ids": newly_updated, "error": error}
        except Exception as exc:
            return {"newly_updated_ids": [], "error": str(exc)}

    @activity.defn
    async def run_health_check(self, args: Dict[str, Any]) -> bool:
        """Run health check callback."""
        if self.engine is not None:
            from deploy.config import DeploymentConfig
            config = getattr(self.engine, "_current_config", None)
            if config is None:
                config = DeploymentConfig(
                    target_version="2.0.0",
                    health_check_fn=self.health_check_fn,
                )
            return self.engine._run_health_check(config, args["stage_idx"], args["target_pct"])

        if self.health_check_fn is None:
            return True
        try:
            return bool(self.health_check_fn(self.cluster))
        except Exception:
            return False

    @activity.defn
    async def auto_quarantine(self) -> List[str]:
        """Check and quarantine unstable regions."""
        if self.quarantine_system is None:
            return []
        # Quarantine threshold is 30%
        return list(self.quarantine_system.check_and_auto_quarantine(threshold_percentage=30.0))

    @activity.defn
    async def evaluate_stage_complete(self, args: Dict[str, Any]) -> str:
        """Evaluate governance policy after stage execution."""
        if self.governance_coordinator is None:
            return "ALLOW"

        deployment_id = args["deployment_id"]
        target_version = args["target_version"]
        source_version = args["source_version"]
        total_servers = args["total_servers"]
        updated_ids = args["updated_ids"]
        stage_idx = args["stage_idx"]
        target_pct = args["target_pct"]
        current_time_str = args.get("current_time")

        current_time = (
            datetime.fromisoformat(current_time_str) if current_time_str else datetime.now()
        )

        ds = self._reconstruct_temp_deployment_state(
            deployment_id, target_version, source_version, total_servers, updated_ids
        )

        decision = self.governance_coordinator.evaluate_stage_complete(
            self.cluster,
            ds,
            stage_idx,
            target_pct,
            current_time=current_time,
            audit_logger=self.audit_logger,
        )
        return decision.name

    @activity.defn
    async def on_stage_complete_callback(self, args: Dict[str, Any]) -> None:
        """Trigger on_stage_complete callback if configured."""
        if self.on_stage_complete is not None:
            stage_idx = args["stage_idx"]
            target_pct = args["target_pct"]
            updated_count = args["updated_count"]
            try:
                self.on_stage_complete(stage_idx, target_pct, updated_count)
            except Exception:
                pass

    @activity.defn
    async def evaluate_rollback(self, args: Dict[str, Any]) -> str:
        """Evaluate rollback governance policy."""
        if self.governance_coordinator is None:
            return "ALLOW"

        deployment_id = args["deployment_id"]
        target_version = args["target_version"]
        source_version = args["source_version"]
        total_servers = args["total_servers"]
        updated_ids = args["updated_ids"]
        current_time_str = args.get("current_time")

        current_time = (
            datetime.fromisoformat(current_time_str) if current_time_str else datetime.now()
        )

        ds = self._reconstruct_temp_deployment_state(
            deployment_id, target_version, source_version, total_servers, updated_ids, "rolling_back"
        )

        decision = self.governance_coordinator.evaluate_rollback(
            self.cluster, ds, current_time=current_time, audit_logger=self.audit_logger
        )
        return decision.name

    @activity.defn
    async def rollback_updated_servers(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Roll back updated servers, using recovery system if configured."""
        deployment_id = args["deployment_id"]
        target_version = args["target_version"]
        source_version = args["source_version"]
        total_servers = args["total_servers"]
        updated_ids = args["updated_ids"]
        error_message = args.get("error_message")

        ds = self._reconstruct_temp_deployment_state(
            deployment_id, target_version, source_version, total_servers, updated_ids, "rolling_back"
        )
        ds.error_message = error_message

        run_recovery = False
        rolled_back_ids = []
        all_success = True

        if self.quarantine_system is not None:
            from resilience.recovery import RecoveryPlanningEngine
            recovery_engine = RecoveryPlanningEngine(self.cluster, self.quarantine_system)
            quarantined = self.quarantine_system.get_quarantined_regions()
            strategy = "region_quarantine" if quarantined else "staged_recovery"
            target_region = list(quarantined)[0] if quarantined else None

            plan = recovery_engine.generate_plan(ds, strategy, target_region)
            # Log plan events manually inside activity using audit_logger
            self._record_event_sync(
                DeploymentEventType.RECOVERY_PLAN_EXECUTE.name,
                deployment_id,
                {
                    "plan_id": plan.plan_id,
                    "strategy": strategy,
                    "target_region": target_region,
                    "steps_count": len(plan.steps),
                },
            )

            success = recovery_engine.execute_recovery_plan(plan, ds)

            # Check which ones are now reverted
            for s_id in updated_ids:
                srv = self.cluster.get_server(s_id)
                if srv and srv.current_version == source_version:
                    rolled_back_ids.append(s_id)

            self._record_event_sync(
                DeploymentEventType.RECOVERY_PLAN_COMPLETE.name,
                deployment_id,
                {
                    "plan_id": plan.plan_id,
                    "status": "completed" if success else "failed",
                    "steps_completed": plan.current_step_index,
                },
            )
            if success:
                run_recovery = True
            else:
                all_success = False

        if not run_recovery:
            for s_id in list(updated_ids):
                success = self.cluster.rollback_server(s_id)
                if success:
                    rolled_back_ids.append(s_id)
                else:
                    all_success = False

        return {"rolled_back_ids": rolled_back_ids, "success": all_success}

    @activity.defn
    async def record_event(self, args: Dict[str, Any]) -> str:
        """Write event to standard audit logger."""
        event_type_name = args["event_type_name"]
        deployment_id = args["deployment_id"]
        details = args.get("details")
        parent_event_id = args.get("parent_event_id")

        event_type = DeploymentEventType[event_type_name]
        return self._record_event_sync(event_type.name, deployment_id, details, parent_event_id)

    def _record_event_sync(
        self,
        event_type_name: str,
        deployment_id: str,
        details: Dict[str, Any] | None = None,
        parent_event_id: str | None = None,
    ) -> str:
        """Internal synchronous logging helper."""
        if self.audit_logger is not None:
            event = DeploymentEvent(
                event_type=DeploymentEventType[event_type_name],
                deployment_id=deployment_id,
                details=details,
                parent_event_id=parent_event_id,
            )
            self.audit_logger.log(event)
            return event.event_id
        return ""

    @activity.defn
    async def mark_servers_healthy(self, args: Dict[str, Any]) -> None:
        """Mark updated servers as healthy after successful rollout."""
        updated_ids = args["updated_ids"]
        for s_id in updated_ids:
            self.cluster.update_server_status(s_id, ServerStatus.HEALTHY)
