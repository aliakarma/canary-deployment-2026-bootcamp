"""
Deployment Coordination Engine for the Canary Deployment Simulator.

Orchestrates a staged canary rollout across a simulated server cluster:

1. Selects servers for each stage based on percentage targets
2. Updates servers to the target version in batches
3. Waits between stages (interruptible via abort event)
4. Invokes optional health-check callbacks after each stage
5. Tracks full deployment state with per-stage granularity

Usage::

    from cluster import generate_cluster, ClusterState
    from deploy import DeploymentEngine, DeploymentConfig

    state = ClusterState(generate_cluster(size=30))
    config = DeploymentConfig(target_version="2.0.0")
    engine = DeploymentEngine(state)
    result = engine.deploy(config)
"""

from __future__ import annotations

import math
import time
import uuid
from datetime import datetime
from typing import Any

from cluster.models import ServerStatus
from cluster.state import ClusterState
from deploy.audit import DeploymentEvent, DeploymentEventType
from deploy.config import DeploymentConfig
from deploy.state import DeploymentState, DeploymentStatus, StageResult
from governance import GovernanceDecision
from logging_config import get_logger

logger = get_logger(__name__)


class GovernanceViolationError(Exception):
    """Raised when a governance policy blocks rollout execution."""

    pass


class DeploymentEngine:
    """Coordinates a staged canary deployment across a cluster.

    The engine is stateless between deployments — each call to
    :meth:`deploy` creates a fresh :class:`DeploymentState` and
    returns it when the deployment completes (or fails/aborts).

    Args:
        cluster_state: The :class:`ClusterState` to deploy into.
    """

    def __init__(self, cluster_state: ClusterState) -> None:
        self._cluster = cluster_state
        self._current_deployment: DeploymentState | None = None
        self._current_config: DeploymentConfig | None = None
        self._last_event_id: str | None = None
        self._last_audit_logger: Any | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def current_deployment(self) -> DeploymentState | None:
        """Return the deployment state of the currently active (or most
        recent) deployment, or ``None`` if no deployment has been run."""
        return self._current_deployment

    def rollback(
        self,
        deployment: DeploymentState,
        force: bool = False,
        audit_logger: Any | None = None,
    ) -> list[str]:
        """Execute a rollback on a previously run deployment.

        Validates cluster consistency before executing.

        Args:
            deployment: The DeploymentState representing the rollout to revert.
            force: If True, bypass consistency checks and revert matching nodes anyway.
            audit_logger: Optional AuditLogger. Falls back to deployment config logger.

        Returns:
            List of successfully rolled back server IDs.
        """
        from deploy.rollback import rollback as run_rollback

        logger_to_use = audit_logger
        if logger_to_use is None:
            if self._current_config is not None:
                logger_to_use = self._current_config.audit_logger
            else:
                logger_to_use = self._last_audit_logger
        return run_rollback(self._cluster, deployment, force=force, audit_logger=logger_to_use)

    def deploy(self, config: DeploymentConfig) -> DeploymentState:  # noqa: C901
        """Execute a full canary deployment.

        This method runs a Temporal workflow in a local event loop.
        """
        import asyncio
        from deploy.temporal_helper import run_temporal_deployment
        self._current_config = config
        self._last_event_id = None
        self._last_audit_logger = config.audit_logger

        deployment = self._init_deployment(config)
        self._current_deployment = deployment

        try:
            asyncio.run(run_temporal_deployment(self._cluster, config, deployment, self))
            return deployment
        except Exception as exc:
            logger.exception("Unexpected error during Temporal deployment: %s", exc)
            summary = self._cluster.get_deployment_summary()
            versions = summary["versions"]
            source_version = max(versions, key=lambda v: versions[v])
            deployment = DeploymentState(
                deployment_id="error",
                target_version=config.target_version,
                source_version=source_version,
                total_servers=self._cluster.size,
            )
            deployment.mark_failed(f"Unexpected error: {exc}")
            self._current_deployment = deployment
            return deployment
        finally:
            self._current_config = None

    # ------------------------------------------------------------------
    # Stage execution
    # ------------------------------------------------------------------

    def _execute_stage(
        self,
        deployment: DeploymentState,
        config: DeploymentConfig,
        stage_idx: int,
        target_pct: int,
    ) -> StageResult:
        """Execute a single rollout stage.

        Determines which servers to update to reach *target_pct*, then
        updates them via the cluster state manager.
        """
        stage = StageResult(
            stage_index=stage_idx,
            target_percentage=target_pct,
            servers_total=deployment.total_servers,
            started_at=datetime.now(),
        )
        deployment.current_stage_index = stage_idx

        logger.info("-" * 50)
        logger.info(
            "STAGE %d: Rolling out to %d%% of cluster...",
            stage_idx,
            target_pct,
        )

        # Calculate how many servers should be updated cumulatively
        # Base quarantined rollouts on eligible (non-quarantined) capacity
        quarantined = set()
        if config.quarantine_system is not None:
            quarantined = config.quarantine_system.get_quarantined_regions()

        # FIX: Base quarantined rollouts on eligible capacity, resolving P2 review comment.
        # When quarantine_system is active, we must exclude servers in quarantined regions
        # from both the total eligible capacity and the currently updated eligible count.
        eligible_servers = [s for s in self._cluster.servers if s.region not in quarantined]
        eligible_total = len(eligible_servers)

        eligible_updated = len(
            [
                s
                for s in self._cluster.servers
                if s.id in deployment.servers_updated and s.region not in quarantined
            ]
        )

        target_count = math.ceil(eligible_total * target_pct / 100)
        already_updated = len(deployment.servers_updated)
        servers_needed = target_count - eligible_updated

        if servers_needed <= 0:
            logger.info(
                "  Stage %d: Already at %d%% — no additional servers needed",
                stage_idx,
                target_pct,
            )
            stage.completed_at = datetime.now()
            stage.duration_seconds = (stage.completed_at - stage.started_at).total_seconds()
            return stage

        # Select servers to update (from pending pool)
        servers_to_update = self._select_servers_for_stage(deployment, servers_needed)

        logger.info(
            "  Updating %d servers (cumulative: %d -> %d of %d)",
            len(servers_to_update),
            already_updated,
            already_updated + len(servers_to_update),
            deployment.total_servers,
        )

        # Update each server
        for server_id in servers_to_update:
            # Check for abort mid-stage
            if self._is_aborted(config):
                stage.error = "Abort signal received mid-stage"
                break

            success = self._cluster.update_server_version(server_id, config.target_version)
            if success:
                deployment.servers_updated.add(server_id)
                deployment.servers_pending.discard(server_id)
                stage.servers_updated.append(server_id)
            else:
                logger.warning("  Failed to update server %s — skipping", server_id)

        # Check if the required number of servers was successfully updated
        # (Only flag as error if we weren't aborted mid-stage)
        if not stage.error and len(stage.servers_updated) < servers_needed:
            stage.error = (
                f"Under-provisioned update: target required updating {servers_needed} servers, "
                f"but only {len(stage.servers_updated)} were successfully updated."
            )

        stage.completed_at = datetime.now()
        stage.duration_seconds = (stage.completed_at - stage.started_at).total_seconds()

        return stage

    def _select_servers_for_stage(
        self,
        deployment: DeploymentState,
        count: int,
    ) -> list[str]:
        """Select servers from the pending pool for the next stage.

        Distributes selection across regions for maximum coverage.
        Only selects servers that are in an updatable state.
        """
        quarantined = set()
        if self._current_config is not None and self._current_config.quarantine_system is not None:
            quarantined = self._current_config.quarantine_system.get_quarantined_regions()

        pending_servers = [
            s
            for s in self._cluster.servers
            if s.id in deployment.servers_pending and s.is_updatable and s.region not in quarantined
        ]

        # Sort by region to ensure cross-region distribution
        pending_servers.sort(key=lambda s: (s.region, s.id))

        # Round-robin across regions for balanced distribution
        by_region: dict[str, list[str]] = {}
        for s in pending_servers:
            by_region.setdefault(s.region, []).append(s.id)

        selected: list[str] = []
        regions = list(by_region.keys())
        region_idx = 0

        while len(selected) < count and any(by_region.values()):
            region = regions[region_idx % len(regions)]
            if by_region[region]:
                selected.append(by_region[region].pop(0))
            region_idx += 1

            # Remove exhausted regions
            regions = [r for r in regions if by_region.get(r)]
            if not regions:
                break

        return selected[:count]

    # ------------------------------------------------------------------
    # Health check integration
    # ------------------------------------------------------------------

    def _run_health_check(
        self,
        config: DeploymentConfig,
        stage_idx: int,
        target_pct: int,
    ) -> bool:
        """Run the health-check function if configured.

        Returns ``True`` if no health-check is configured or if the
        check passes.
        """
        if config.health_check_fn is None:
            logger.debug(
                "  No health check configured — assuming stage %d passed",
                stage_idx,
            )
            return True

        logger.info("  Running health check for stage %d (%d%%)...", stage_idx, target_pct)
        try:
            result = config.health_check_fn(self._cluster)
            status = "PASSED" if result else "FAILED"
            logger.info("  Health check %s for stage %d", status, stage_idx)
            return bool(result)
        except Exception as exc:
            logger.error(
                "  Health check raised exception: %s — treating as failure",
                exc,
            )
            return False

    # ------------------------------------------------------------------
    # Timing / inter-stage wait
    # ------------------------------------------------------------------

    def _wait_between_stages(
        self,
        config: DeploymentConfig,
        deployment: DeploymentState,
    ) -> bool:
        """Wait between stages, checking for abort periodically.

        Returns ``True`` if the wait completed normally, ``False`` if
        an abort signal was received.
        """
        delay = config.stage_delay_seconds
        if delay <= 0:
            return True

        logger.info(
            "  Waiting %.1fs before next stage (progress: %.1f%%)...",
            delay,
            deployment.progress_percentage,
        )

        if config.abort_event is not None:
            # Use the event's wait() — returns True if the event is set
            aborted = config.abort_event.wait(timeout=delay)
            return not aborted
        else:
            # Simple sleep, but in small increments to stay responsive
            elapsed = 0.0
            interval = config.health_check_interval
            # FIX: Avoid zero-length inter-stage sleeps, resolving P2 review comment.
            # If health_check_interval is <= 0, we fall back to using the full delay (which is > 0)
            # as the increment to avoid infinite loop.
            if interval <= 0:
                increment = delay
            else:
                increment = min(interval, delay)

            while elapsed < delay:
                time.sleep(increment)
                elapsed += increment
            return True

    # ------------------------------------------------------------------
    # Abort handling
    # ------------------------------------------------------------------

    def _is_aborted(self, config: DeploymentConfig) -> bool:
        """Check whether the abort event has been signalled."""
        if config.abort_event is not None:
            return bool(config.abort_event.is_set())
        return False

    def _handle_abort(self, deployment: DeploymentState, reason: str) -> None:
        """Handle an abort signal: log, mark state, trigger rollback."""
        logger.warning("DEPLOYMENT ABORTED: %s", reason)
        self._record_event(
            DeploymentEventType.ABORT_RECEIVED,
            {"reason": reason},
        )
        deployment.mark_aborted(reason)
        self._record_event(
            DeploymentEventType.ROLLBACK_INITIATED,
            {"reason": f"Aborted: {reason}", "stage_index": deployment.current_stage_index},
        )
        self._rollback_updated_servers(deployment)

    # ------------------------------------------------------------------
    # Rollback
    # ------------------------------------------------------------------

    def _handle_rollback(self, deployment: DeploymentState, reason: str) -> None:
        """Initiate a rollback of all servers updated so far."""
        logger.warning("INITIATING ROLLBACK: %s", reason)
        deployment.mark_rolling_back()
        deployment.error_message = reason
        self._record_event(
            DeploymentEventType.ROLLBACK_INITIATED,
            {"reason": reason, "stage_index": deployment.current_stage_index},
        )
        success = self._rollback_updated_servers(deployment)
        if success:
            deployment.mark_rolled_back()
            logger.info(
                "ROLLBACK COMPLETE: %d servers reverted to %s",
                len(deployment.servers_updated),
                deployment.source_version,
            )
        else:
            logger.error("ROLLBACK FAILED: some servers could not be reverted")

    def _rollback_updated_servers(self, deployment: DeploymentState) -> bool:
        """Revert all servers that were updated during this deployment."""
        # Checkpoint: evaluate_rollback
        if (
            self._current_config is not None
            and self._current_config.governance_coordinator is not None
        ):
            decision = self._current_config.governance_coordinator.evaluate_rollback(
                self._cluster,
                deployment,
                current_time=self._current_config.current_time,
                audit_logger=self._current_config.audit_logger,
            )
            if decision == GovernanceDecision.BLOCK:
                logger.warning("AUTOMATIC ROLLBACK BLOCKED BY GOVERNANCE POLICY")
                raise GovernanceViolationError("Automatic rollback blocked by governance policy.")

        # Take pre-rollback snapshot
        if self._current_config is not None and self._current_config.audit_logger is not None:
            from resilience.snapshots import ClusterSnapshotSystem

            snapshot_system = ClusterSnapshotSystem(self._cluster)
            snapshot = snapshot_system.create_snapshot(
                deployment, metadata={"event": "rollback_init"}
            )
            self._record_event(
                DeploymentEventType.SNAPSHOT_CREATE,
                {
                    "snapshot_id": snapshot.snapshot_id,
                    "reason": "Pre-rollback state snapshot",
                    "servers_count": len(snapshot.servers),
                },
            )

        self._record_event(
            DeploymentEventType.ROLLBACK_START,
            {
                "reason": deployment.error_message or "Automatic rollback",
                "source_version": deployment.source_version,
                "servers_to_rollback": list(deployment.servers_updated),
            },
        )

        run_recovery = False
        rolled_back_ids = []
        all_success = True

        if self._current_config is not None and self._current_config.quarantine_system is not None:
            from resilience.recovery import RecoveryPlanningEngine

            recovery_engine = RecoveryPlanningEngine(
                self._cluster, self._current_config.quarantine_system
            )
            quarantined = self._current_config.quarantine_system.get_quarantined_regions()
            if quarantined:
                strategy = "region_quarantine"
                target_region = list(quarantined)[0]
            else:
                strategy = "staged_recovery"
                target_region = None

            plan = recovery_engine.generate_plan(deployment, strategy, target_region)
            self._record_event(
                DeploymentEventType.RECOVERY_PLAN_EXECUTE,
                {
                    "plan_id": plan.plan_id,
                    "strategy": strategy,
                    "target_region": target_region,
                    "steps_count": len(plan.steps),
                },
            )

            pre_updated = list(deployment.servers_updated)
            success = recovery_engine.execute_recovery_plan(plan, deployment)

            # Check which ones are now reverted
            for s_id in pre_updated:
                srv = self._cluster.get_server(s_id)
                if srv and srv.current_version == deployment.source_version:
                    rolled_back_ids.append(s_id)

            self._record_event(
                DeploymentEventType.RECOVERY_PLAN_COMPLETE,
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
            for server_id in list(deployment.servers_updated):
                success = self._cluster.rollback_server(server_id)
                if success:
                    rolled_back_ids.append(server_id)
                    logger.info("  Rolled back server %s", server_id)
                else:
                    logger.error("  Failed to rollback server %s", server_id)
                    all_success = False

        self._record_event(
            DeploymentEventType.ROLLBACK_COMPLETE,
            {
                "servers_rolled_back": rolled_back_ids,
            },
        )
        return all_success and len(rolled_back_ids) == len(deployment.servers_updated)

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _init_deployment(self, config: DeploymentConfig) -> DeploymentState:
        """Create and initialise a new DeploymentState."""
        # Determine source version from the majority of servers
        summary = self._cluster.get_deployment_summary()
        versions = summary["versions"]
        source_version = max(versions, key=lambda v: versions[v])

        # Get all server IDs
        all_server_ids = {s.id for s in self._cluster.servers}

        deployment = DeploymentState(
            deployment_id=str(uuid.uuid4())[:8],
            target_version=config.target_version,
            source_version=source_version,
            total_servers=self._cluster.size,
            servers_pending=set(all_server_ids),
        )

        return deployment

    def _record_event(
        self,
        event_type: DeploymentEventType,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Helper to log structured events to the configured audit logger."""
        if self._current_config is not None and self._current_config.audit_logger is not None:
            if self._current_deployment is not None:
                event = DeploymentEvent(
                    event_type=event_type,
                    deployment_id=self._current_deployment.deployment_id,
                    details=details,
                    parent_event_id=self._last_event_id,
                )
                self._last_event_id = event.event_id
                self._current_config.audit_logger.log(event)

    def _handle_governance_block(self, deployment: DeploymentState, reason: str) -> None:
        """Handle a governance policy block."""
        logger.warning("DEPLOYMENT BLOCKED BY GOVERNANCE: %s", reason)
        deployment.mark_failed(reason)
        self._record_event(
            DeploymentEventType.POLICY_VIOLATION,
            {"reason": reason, "stage_index": deployment.current_stage_index},
        )
