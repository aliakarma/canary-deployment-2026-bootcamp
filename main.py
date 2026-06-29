"""
Canary Deployment Simulator — Entry Point

Initialises the logging system, generates a simulated cluster,
displays the initial cluster status report, and runs a staged
canary deployment with active health analysis and abort listener support.
"""

from __future__ import annotations

import datetime
import json
import os
import threading

from cluster import ClusterState, generate_cluster, inspect_cluster
from cluster.models import ServerStatus
from deploy import (
    AuditLogger,
    ConsoleAbortListener,
    DeploymentConfig,
    DeploymentEngine,
    RollbackConsistencyError,
    load_deployment_state,
    save_deployment_state,
    validate_rollback_consistency,
)
from governance import (
    ApprovalGate,
    GovernanceCoordinator,
)
from health import HealthThresholds, create_health_check_fn, inject_failures
from logging_config import get_logger
from resilience.observability import OperationalObservabilityLayer
from resilience.quarantine import RegionQuarantineSystem
from resilience.replay import EventReplayEngine
from resilience.snapshots import ClusterSnapshotSystem

logger = get_logger(__name__)

# Fixed business-hours clock used for the demo so that governance
# restricted-window policies (RiskPolicy) behave deterministically no
# matter which day/time the simulation is actually executed.
DEMO_CLOCK = datetime.datetime(2026, 6, 17, 10, 0)  # Wednesday morning
WEEKEND_CLOCK = datetime.datetime(2026, 6, 21, 15, 0)  # Sunday afternoon


def main() -> None:
    """Run the canary deployment simulation."""
    logger.info("=" * 60)
    logger.info("  CANARY DEPLOYMENT SIMULATOR")
    logger.info("=" * 60)

    # Initialize structured event audit logger exporting to logs/audit_trail.jsonl
    audit_file = "logs/audit_trail.jsonl"
    if os.path.exists(audit_file):
        try:
            os.remove(audit_file)
        except Exception:
            pass
    audit_logger = AuditLogger(file_path=audit_file)

    # Start the asynchronous console input abort listener
    listener = ConsoleAbortListener()
    listener.start()

    try:
        # ------------------------------------------------------------------
        # Phase 1: Generate cluster
        # ------------------------------------------------------------------
        logger.info("Phase 1: Generating simulated cluster ...")
        servers = generate_cluster(seed=2026)
        state = ClusterState(servers)

        # ------------------------------------------------------------------
        # Phase 2: Inspect initial state
        # ------------------------------------------------------------------
        logger.info("Phase 2: Inspecting initial cluster state ...")
        inspect_cluster(state)

        # ------------------------------------------------------------------
        # Phase 3: Staged canary deployment with Health Check Hook
        # ------------------------------------------------------------------
        logger.info("Phase 3: Starting healthy canary deployment with health check hook ...")

        # Define standard health thresholds
        thresholds = HealthThresholds()
        health_check = create_health_check_fn(thresholds)

        # Wire up abort event for this deployment
        abort_event = threading.Event()
        listener.set_abort_event(abort_event)

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[10, 25, 50, 75, 100],
            stage_delay_seconds=1.0,  # Shortened for demo
            health_check_fn=health_check,
            abort_event=abort_event,
            audit_logger=audit_logger,
            governance_coordinator=GovernanceCoordinator(),
            current_time=DEMO_CLOCK,
        )

        engine = DeploymentEngine(state)
        result = engine.deploy(config)
        listener.clear_abort_event()

        # ------------------------------------------------------------------
        # Phase 3b: Inspect post-deployment state
        # ------------------------------------------------------------------
        logger.info("Post-deployment cluster state:")
        inspect_cluster(state)

        logger.info(
            "Deployment result: %s (%.1fs, %d/%d servers)",
            result.status.value,
            result.duration_seconds,
            len(result.servers_updated),
            result.total_servers,
        )

        # ------------------------------------------------------------------
        # Phase 4: Staged canary deployment with Failure Injection
        # ------------------------------------------------------------------
        logger.info("=" * 60)
        logger.info("Phase 4: Running failure injection and rollback demonstration")
        logger.info("=" * 60)

        # Generate a fresh cluster for the failure scenario
        logger.info("Generating a fresh cluster state ...")
        failing_servers = generate_cluster(size=20, seed=42)
        failing_state = ClusterState(failing_servers)

        logger.info("Initial status of fresh cluster:")
        inspect_cluster(failing_state)

        # Configure zero degraded servers threshold to abort immediately on any failure
        strict_thresholds = HealthThresholds(max_degraded_server_percentage=0.0)
        base_health_fn = create_health_check_fn(strict_thresholds)

        call_count = 0

        def failure_injection_hook(cs: ClusterState) -> bool:
            nonlocal call_count
            # Inject failures right after Stage 1 completes (25% progress)
            if call_count == 1:
                logger.warning(
                    "!!! CHAOS INJECTION: Spiking latency & degrading target-version servers !!!"
                )
                inject_failures(
                    cs,
                    target_version="2.0.0",
                    failure_rate=0.4,
                    failure_type="degrade",
                    seed=42,
                )
            call_count += 1
            return base_health_fn(cs)

        # Wire up abort event for the failing deployment demo
        failing_abort_event = threading.Event()
        listener.set_abort_event(failing_abort_event)

        failing_config = DeploymentConfig(
            target_version="2.0.0",
            stages=[10, 25, 50, 75, 100],
            stage_delay_seconds=1.0,
            health_check_interval=0.5,
            health_check_fn=failure_injection_hook,
            abort_event=failing_abort_event,
            max_retries_per_stage=0,  # Fail immediately to trigger rollback
            audit_logger=audit_logger,
        )

        logger.info("Starting deployment with failure hook active ...")
        failing_engine = DeploymentEngine(failing_state)
        failing_result = failing_engine.deploy(failing_config)
        listener.clear_abort_event()

        # Inspect final state to show everything is rolled back to v1.0.0
        logger.info("Final cluster state after failure detection & automatic rollback:")
        inspect_cluster(failing_state)

        logger.info(
            "Failing deployment result: %s (%.1fs, %d/%d servers updated, final error: %s)",
            failing_result.status.value,
            failing_result.duration_seconds,
            len(failing_result.servers_updated),
            failing_result.total_servers,
            failing_result.error_message,
        )

        # ------------------------------------------------------------------
        # Phase 5: Manual Rollback System Demonstration
        # ------------------------------------------------------------------
        logger.info("=" * 60)
        logger.info("Phase 5: Running manual rollback system demonstration")
        logger.info("=" * 60)

        # 1. Store/Save deployment state of the healthy Phase 3 run
        state_file = "logs/phase3_deployment_state.json"
        logger.info("Saving healthy deployment state to disk: %s", state_file)
        save_deployment_state(result, state_file)

        # 2. Restore/Load the deployment state from disk
        logger.info("Restoring deployment state from disk ...")
        loaded_state = load_deployment_state(state_file)
        logger.info(
            "Successfully loaded deployment state. ID: %s, Target Version: %s, Updated Servers: %d",
            loaded_state.deployment_id,
            loaded_state.target_version,
            len(loaded_state.servers_updated),
        )

        # 3. Modify a server's version manually in the cluster to simulate a configuration drift/inconsistency
        drift_server_id = list(loaded_state.servers_updated)[0]
        drift_server = state.get_server(drift_server_id)
        if drift_server:
            logger.info(
                "Simulating configuration drift by changing %s version manually to '2.1.0'",
                drift_server_id,
            )
            # Apply drift through the thread-safe public helper (no private lock access).
            state.override_version(drift_server_id, "2.1.0")

        # 4. Validate rollback consistency
        logger.info("Running consistency check on the cluster state ...")
        inconsistencies = validate_rollback_consistency(state, loaded_state)
        logger.warning("Inconsistencies detected: %s", inconsistencies)

        # 5. Attempt consistent rollback (should fail due to drift)
        logger.info("Attempting to run a consistent rollback (force=False) ...")
        try:
            engine.rollback(loaded_state, force=False, audit_logger=audit_logger)
        except RollbackConsistencyError as exc:
            logger.error("Rollback aborted! Consistency check failed: %s", exc)
            logger.error("Detailed server mismatches: %s", exc.errors)

        # 6. Run forced rollback (force=True) to override drift and successfully revert all updated nodes
        logger.info("Executing forced rollback (force=True) to revert all nodes ...")
        rolled_back_servers = engine.rollback(loaded_state, force=True, audit_logger=audit_logger)

        # 7. Print final cluster state after manual rollback
        logger.info("Final cluster state after manual rollback:")
        inspect_cluster(state)

        logger.info(
            "Manual rollback completed: reverted %d servers back to version %s",
            len(rolled_back_servers),
            loaded_state.source_version,
        )

        # ------------------------------------------------------------------
        # Phase 6: Governance Policy Engine Demonstration
        # ------------------------------------------------------------------
        logger.info("=" * 60)
        logger.info("Phase 6: Governance Policy Engine & Advanced Operational Control")
        logger.info("=" * 60)

        # ------------------------------------------------------------------
        # Scenario 6a: Weekend Deployment Block
        # ------------------------------------------------------------------
        logger.info("Scenario 6a: Weekend Deployment Block (Restricted Window)")

        # Pin the governance clock to a Sunday afternoon to deterministically
        # demonstrate the restricted-window block via the default RiskPolicy.
        weekend_config = DeploymentConfig(
            target_version="2.0.0",
            stages=[10, 50, 100],
            stage_delay_seconds=0.1,
            health_check_fn=create_health_check_fn(HealthThresholds()),
            governance_coordinator=GovernanceCoordinator(),
            audit_logger=audit_logger,
            current_time=WEEKEND_CLOCK,
        )

        weekend_cluster = ClusterState(generate_cluster(size=10, seed=42))
        weekend_engine = DeploymentEngine(weekend_cluster)
        weekend_result = weekend_engine.deploy(weekend_config)

        logger.info(
            "Weekend deployment status: %s (Expected: FAILED)",
            weekend_result.status.value,
        )
        logger.info("Weekend deployment error message: %s", weekend_result.error_message)

        # ------------------------------------------------------------------
        # Scenario 6b: Human Approval Denied Gatekeeper Block
        # ------------------------------------------------------------------
        logger.info("-" * 50)
        logger.info("Scenario 6b: Manual Approval Denied Gatekeeper Block")

        # Approval gate callback that always denies (simulating operator rejects the prompt/gate)
        approval_gate_deny = ApprovalGate(callback=lambda req: False)

        coordinator_deny = GovernanceCoordinator(approval_gate=approval_gate_deny)

        # Deployment stages hitting 100% (progress >= 75%) which triggers approval
        deny_config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.1,
            health_check_fn=create_health_check_fn(HealthThresholds()),
            governance_coordinator=coordinator_deny,
            audit_logger=audit_logger,
            current_time=DEMO_CLOCK,
        )

        deny_cluster = ClusterState(generate_cluster(size=10, seed=42))
        deny_engine = DeploymentEngine(deny_cluster)
        deny_result = deny_engine.deploy(deny_config)

        logger.info(
            "Denied approval deployment status: %s (Expected: FAILED)",
            deny_result.status.value,
        )
        logger.info("Denied approval deployment error: %s", deny_result.error_message)
        logger.info(
            "Partially updated servers running 2.0.0: %d",
            len([s for s in deny_cluster.servers if s.current_version == "2.0.0"]),
        )

        # ------------------------------------------------------------------
        # Scenario 6c: Critical Risk Rollback Suspension (Auto-rollback Suspended)
        # ------------------------------------------------------------------
        logger.info("-" * 50)
        logger.info("Scenario 6c: CRITICAL Risk Rollback Suspension Block")

        suspension_cluster = ClusterState(generate_cluster(size=10, seed=42))

        def failing_risk_spike_hook(cs: ClusterState) -> bool:
            logger.warning("Spiking cluster errors to trigger CRITICAL risk classification...")
            servers = cs.servers
            cs.update_server_status(servers[0].id, ServerStatus.DEGRADED)
            cs.update_server_status(servers[1].id, ServerStatus.FAILED)
            cs.update_server_status(servers[2].id, ServerStatus.FAILED)
            cs.update_server_status(servers[3].id, ServerStatus.FAILED)
            cs.update_server_status(servers[4].id, ServerStatus.FAILED)
            return False

        coordinator_suspend = GovernanceCoordinator()

        suspend_config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.1,
            health_check_interval=0.1,
            health_check_fn=failing_risk_spike_hook,
            max_retries_per_stage=0,
            governance_coordinator=coordinator_suspend,
            audit_logger=audit_logger,
            current_time=DEMO_CLOCK,
        )

        suspend_engine = DeploymentEngine(suspension_cluster)
        suspend_result = suspend_engine.deploy(suspend_config)

        logger.info(
            "Rollback-suspended deployment status: %s (Expected: FAILED)",
            suspend_result.status.value,
        )
        logger.info("Rollback-suspended error message: %s", suspend_result.error_message)
        logger.info(
            "Servers currently running 2.0.0: %d (Expected > 0, rollback was prevented)",
            len([s for s in suspension_cluster.servers if s.current_version == "2.0.0"]),
        )

        # ------------------------------------------------------------------
        # Phase 7: Operational Resilience, Failure Recovery & Observability
        # ------------------------------------------------------------------
        logger.info("=" * 60)
        logger.info("Phase 7: Operational Resilience, Failure Recovery & Observability")
        logger.info("=" * 60)

        # 1. Snapshot Creation and Restoration Demonstration
        logger.info("Scenario 7a: Point-in-time Snapshot and safe restoration")

        snapshot_cluster = ClusterSnapshotSystem(state)

        # Capture current state of the healthy cluster
        pre_fail_snapshot = snapshot_cluster.create_snapshot(
            result, metadata={"note": "Pre-chaos snapshot"}
        )
        logger.info("Created pre-chaos cluster snapshot: %s", pre_fail_snapshot.snapshot_id)

        # Simulate cluster failure (degrade a server manually)
        servers_list = state.servers
        state.update_server_status(servers_list[0].id, ServerStatus.FAILED)
        logger.warning("Simulated failure: Server %s is now FAILED", servers_list[0].id)

        # Restore snapshot to recover state
        logger.info("Restoring cluster to pre-chaos snapshot...")
        restored = snapshot_cluster.restore_snapshot(pre_fail_snapshot)
        logger.info("Snapshot restoration result: %s (Expected: True)", restored)
        logger.info(
            "Server %s status recovered: %s (Expected: healthy)",
            servers_list[0].id,
            state.get_server(servers_list[0].id).status.value,  # type: ignore[union-attr]
        )

        # 2. Region Quarantine Demonstration
        logger.info("-" * 50)
        logger.info("Scenario 7b: Region Quarantine under cascading failure")

        quarantine_sys = RegionQuarantineSystem(state)

        # Force failures on all us-east-1 servers to trigger auto-quarantine threshold (e.g. >30% degraded)
        us_east_nodes = [s for s in state.servers if s.region == "us-east-1"]
        for node in us_east_nodes:
            state.update_server_status(node.id, ServerStatus.FAILED)

        quarantined = quarantine_sys.check_and_auto_quarantine(threshold_percentage=30.0)
        logger.warning("Regions auto-quarantined: %s (Expected: ['us-east-1'])", quarantined)

        # 3. Quarantine-aware deployment routing
        logger.info("-" * 50)
        logger.info("Scenario 7c: Quarantine-aware rollout routing")
        routing_config = DeploymentConfig(
            target_version="2.1.0",
            stages=[50, 100],
            stage_delay_seconds=0.1,
            quarantine_system=quarantine_sys,
            audit_logger=audit_logger,
        )
        routing_engine = DeploymentEngine(state)
        routing_result = routing_engine.deploy(routing_config)
        logger.info("Rollout routing completed. Status: %s", routing_result.status.value)
        # Check that none of the updated servers are in the quarantined region
        quarantined_updated = [
            s_id
            for s_id in routing_result.servers_updated
            if state.get_server(s_id).region == "us-east-1"  # type: ignore[union-attr]
        ]
        logger.info(
            "Number of updated servers in quarantined us-east-1: %d (Expected: 0)",
            len(quarantined_updated),
        )

        # 4. Event Replay and Causality Graph
        logger.info("-" * 50)
        logger.info("Scenario 7d: Event Replay causality graph & timeline reconstruction")

        replay_engine = EventReplayEngine()
        loaded_events = replay_engine.load_audit_trail(audit_file)

        is_lineage_valid, lineage_errors = replay_engine.verify_event_lineage(loaded_events)
        logger.info(
            "Audit lineage verification: %s",
            "VALID" if is_lineage_valid else f"INVALID: {lineage_errors}",
        )

        timeline = replay_engine.reconstruct_timeline(loaded_events)
        logger.info("Timeline reconstructed. Total replayed events: %d", len(timeline))

        # Verify state reconstruction at the last event, scoped to that
        # event's own deployment so multiple deployments in one audit file
        # are not blended together.
        last_evt = timeline[-1]
        last_evt_id = last_evt["event_id"]
        last_deployment_id = last_evt.get("deployment_id")
        reconstructed_state = replay_engine.reconstruct_state_at_step(
            loaded_events, last_evt_id, deployment_id=last_deployment_id
        )
        logger.info(
            "Reconstructed deployment status at last step: %s",
            reconstructed_state["deployment_status"],
        )

        # 5. Observability Metrics
        logger.info("-" * 50)
        logger.info("Scenario 7e: Operational Observability Layer Metrics Aggregation")

        observability_layer = OperationalObservabilityLayer()
        aggregated_metrics = observability_layer.aggregate_metrics(loaded_events)
        logger.info("Aggregated Observability Metrics:")
        logger.info("  Total deployments: %d", aggregated_metrics["total_deployments"])
        logger.info("  Completed deployments: %d", aggregated_metrics["completions"])
        logger.info("  Total failures/aborts: %d", aggregated_metrics["failures"])
        logger.info("  Rollback frequency ratio: %.2f", aggregated_metrics["rollback_ratio"])

        # ------------------------------------------------------------------
        # Phase 8: Structured Event Log Summary Demonstration
        # ------------------------------------------------------------------
        logger.info("=" * 60)
        logger.info("Phase 8: Structured Event Audit Log Summary")
        logger.info("=" * 60)
        logger.info(
            "Total structured events recorded in memory: %d", len(audit_logger.get_events())
        )
        logger.info("Persistent audit file saved to: %s", audit_file)
        logger.info("Sample serialized events (first 5 events):")

        for idx, ev in enumerate(audit_logger.get_events()[:5]):
            logger.info("  Event %d:\n%s", idx + 1, json.dumps(ev.to_dict(), indent=2))

    finally:
        # Tear down the listener thread and audit handle cleanly
        listener.stop()
        audit_logger.close()


if __name__ == "__main__":
    main()
