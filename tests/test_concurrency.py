"""
Concurrency stress and torture tests for the Canary Deployment Simulator.
Verifies thread-safety, lock correctness, and race-condition safety.
"""

from __future__ import annotations

import threading
import time

import pytest

from cluster.generator import generate_cluster
from cluster.state import ClusterState
from deploy.audit import AuditLogger, DeploymentEvent, DeploymentEventType
from deploy.config import DeploymentConfig
from deploy.engine import DeploymentEngine
from deploy.state import DeploymentStatus
from governance import GovernanceCoordinator


class TestConcurrencyStress:
    """Stress tests asserting system stability under concurrent loads."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=20, seed=42))

    def test_rapid_audit_log_burst(self) -> None:
        """Stress log method of AuditLogger with rapid parallel event streams."""
        logger = AuditLogger()
        num_threads = 20
        events_per_thread = 100
        threads = []

        def worker(thread_idx: int) -> None:
            for i in range(events_per_thread):
                evt = DeploymentEvent(
                    event_type=DeploymentEventType.HEALTH_CHECK,
                    deployment_id=f"dep-{thread_idx}",
                    details={"index": i, "thread": thread_idx},
                )
                logger.log(evt)

        for idx in range(num_threads):
            t = threading.Thread(target=worker, args=(idx,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        all_events = logger.get_events()
        assert len(all_events) == num_threads * events_per_thread

    def test_simultaneous_rollback_triggers(self, cluster_state: ClusterState) -> None:
        """Trigger concurrent rollbacks on the same engine to ensure thread safety."""
        engine = DeploymentEngine(cluster_state)
        config = DeploymentConfig(target_version="2.0.0", stages=[100], stage_delay_seconds=0.0)
        dep_state = engine._init_deployment(config)
        engine._current_deployment = dep_state
        engine._current_config = config

        # Mark some nodes updated
        servers = cluster_state.servers
        for s in servers[:5]:
            cluster_state.update_server_version(s.id, "2.0.0")
            dep_state.servers_updated.add(s.id)

        num_threads = 10
        errors = []

        def trigger_rollback() -> None:
            try:
                # Concurrent rollback calls
                engine.rollback(dep_state, force=True)
            except Exception as e:
                errors.append(e)

        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=trigger_rollback)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Should complete without crashing (no deadlocks or unhandled concurrency failures)
        assert len(errors) == 0
        # All servers should be rolled back to version 1.0.0
        for s in cluster_state.servers[:5]:
            assert s.current_version == "1.0.0"

    def test_repeated_abort_spam(self, cluster_state: ClusterState) -> None:
        """Spam abort_event trigger concurrently while a deployment is running."""
        engine = DeploymentEngine(cluster_state)
        abort_event = threading.Event()

        # Simple slow health check to ensure deployment is in progress
        def slow_health(cs: ClusterState) -> bool:
            time.sleep(0.05)
            return True

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[20, 50, 100],
            stage_delay_seconds=0.2,
            abort_event=abort_event,
            health_check_fn=slow_health,
        )

        def deploy_worker() -> None:
            engine.deploy(config)

        t_deploy = threading.Thread(target=deploy_worker)
        t_deploy.start()

        # Spam set and clear abort event concurrently
        def spam_abort() -> None:
            for _ in range(50):
                abort_event.set()
                time.sleep(0.005)
                abort_event.clear()
                time.sleep(0.005)

        t_spam = threading.Thread(target=spam_abort)
        t_spam.start()

        t_spam.join()
        abort_event.set()  # Make sure it's fully aborted
        t_deploy.join()

        # Deployment should gracefully enter aborted/failed state
        res = engine.current_deployment
        assert res is not None
        assert res.status in (DeploymentStatus.FAILED, DeploymentStatus.ABORTED)

    def test_governance_during_concurrent_changes(self, cluster_state: ClusterState) -> None:
        """Trigger random cluster state modifications while governance evaluation runs."""
        coordinator = GovernanceCoordinator()
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.01,
            governance_coordinator=coordinator,
        )
        engine = DeploymentEngine(cluster_state)
        dep_state = engine._init_deployment(config)

        stop_threads = threading.Event()

        def cluster_mutator() -> None:
            # Randomly mutate server status to cause risk calculations to fluctuate
            servers = cluster_state.servers
            import random

            from cluster.models import ServerStatus

            while not stop_threads.is_set():
                s = random.choice(servers)
                status = random.choice(
                    [ServerStatus.HEALTHY, ServerStatus.DEGRADED, ServerStatus.FAILED]
                )
                cluster_state.update_server_status(s.id, status)
                time.sleep(0.002)

        t_mutator = threading.Thread(target=cluster_mutator)
        t_mutator.start()

        # Run several coordinator check runs
        try:
            for _ in range(20):
                coordinator.evaluate_start(cluster_state, dep_state)
                coordinator.evaluate_stage_start(cluster_state, dep_state, 0, 50)
                coordinator.evaluate_stage_complete(cluster_state, dep_state, 0, 50)
                time.sleep(0.005)
        finally:
            stop_threads.set()
            t_mutator.join()


class TestConcurrencyOrderingGuarantees:
    """Tests verifying ordering consistency guarantees after the AuditLogger
    atomic single-lock refactor."""

    def test_audit_logger_concurrent_burst_file_ordering(self) -> None:
        """Verify that file-backed audit logger maintains memory-to-disk ordering
        consistency under 20-thread concurrent burst writes."""
        import json
        import os
        import tempfile

        from deploy.audit import AuditLogger, DeploymentEvent, DeploymentEventType

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "burst_ordering.jsonl")
            logger = AuditLogger(file_path=file_path)
            num_threads = 20
            events_per_thread = 100

            def worker(thread_idx: int) -> None:
                for i in range(events_per_thread):
                    event = DeploymentEvent(
                        event_type=DeploymentEventType.HEALTH_CHECK,
                        deployment_id=f"dep-burst-{thread_idx}",
                        details={"thread": thread_idx, "seq": i},
                    )
                    logger.log(event)

            threads = []
            for idx in range(num_threads):
                t = threading.Thread(target=worker, args=(idx,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            # Verify total counts
            memory_events = logger.get_events()
            expected_total = num_threads * events_per_thread
            assert len(memory_events) == expected_total

            with open(file_path, "r", encoding="utf-8") as f:
                file_lines = [line.strip() for line in f if line.strip()]
            assert len(file_lines) == expected_total

            # Verify ordering consistency
            memory_ids = [e.event_id for e in memory_events]
            file_ids = [json.loads(line)["event_id"] for line in file_lines]
            assert memory_ids == file_ids

    def test_simultaneous_rollback_logging_ordering(self) -> None:
        """Verify rollback audit events logged from multiple concurrent rollback
        triggers maintain atomic ordering consistency."""
        import json
        import os
        import tempfile

        from deploy.audit import AuditLogger, DeploymentEvent, DeploymentEventType

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "rollback_ordering.jsonl")
            logger = AuditLogger(file_path=file_path)
            num_threads = 10

            def rollback_worker(thread_idx: int) -> None:
                # Simulate a rollback logging sequence
                logger.log(
                    DeploymentEvent(
                        event_type=DeploymentEventType.ROLLBACK_INITIATED,
                        deployment_id=f"dep-rb-{thread_idx}",
                        details={"reason": f"Thread {thread_idx} rollback"},
                    )
                )
                logger.log(
                    DeploymentEvent(
                        event_type=DeploymentEventType.ROLLBACK_START,
                        deployment_id=f"dep-rb-{thread_idx}",
                        details={"source_version": "1.0.0"},
                    )
                )
                logger.log(
                    DeploymentEvent(
                        event_type=DeploymentEventType.ROLLBACK_COMPLETE,
                        deployment_id=f"dep-rb-{thread_idx}",
                        details={"servers_rolled_back": [f"server-{thread_idx}"]},
                    )
                )

            threads = []
            for idx in range(num_threads):
                t = threading.Thread(target=rollback_worker, args=(idx,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            # Verify total events
            memory_events = logger.get_events()
            assert len(memory_events) == num_threads * 3

            with open(file_path, "r", encoding="utf-8") as f:
                file_lines = [line.strip() for line in f if line.strip()]
            assert len(file_lines) == num_threads * 3

            # Verify memory-to-disk ordering consistency
            memory_ids = [e.event_id for e in memory_events]
            file_ids = [json.loads(line)["event_id"] for line in file_lines]
            assert memory_ids == file_ids

            # Verify each thread's 3 events appear in correct relative order
            for idx in range(num_threads):
                dep_id = f"dep-rb-{idx}"
                thread_events = [e for e in memory_events if e.deployment_id == dep_id]
                assert len(thread_events) == 3
                assert thread_events[0].event_type == DeploymentEventType.ROLLBACK_INITIATED
                assert thread_events[1].event_type == DeploymentEventType.ROLLBACK_START
                assert thread_events[2].event_type == DeploymentEventType.ROLLBACK_COMPLETE

    def test_replay_consistency_under_contention(self) -> None:
        """Replay a file-backed audit trail produced under concurrent write pressure
        and verify lineage + timeline integrity."""
        import json
        import os
        import tempfile

        from deploy.audit import AuditLogger, DeploymentEvent, DeploymentEventType
        from resilience.replay import EventReplayEngine

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "replay_contention.jsonl")
            logger = AuditLogger(file_path=file_path)

            # Simulate 5 concurrent deployment traces
            num_deployments = 5

            def deployment_trace(dep_idx: int) -> None:
                dep_id = f"dep-replay-{dep_idx}"
                # Record a deployment lifecycle
                start_evt = DeploymentEvent(
                    event_type=DeploymentEventType.DEPLOYMENT_START,
                    deployment_id=dep_id,
                    details={"target_version": "2.0.0"},
                )
                logger.log(start_evt)

                stage_evt = DeploymentEvent(
                    event_type=DeploymentEventType.STAGE_TRANSITION,
                    deployment_id=dep_id,
                    details={"stage_index": 0, "servers_updated": [f"server-{dep_idx}"]},
                    parent_event_id=start_evt.event_id,
                )
                logger.log(stage_evt)

                complete_evt = DeploymentEvent(
                    event_type=DeploymentEventType.DEPLOYMENT_COMPLETED,
                    deployment_id=dep_id,
                    details={"duration_seconds": 1.0},
                    parent_event_id=stage_evt.event_id,
                )
                logger.log(complete_evt)

            threads = []
            for idx in range(num_deployments):
                t = threading.Thread(target=deployment_trace, args=(idx,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            # Load and replay from file
            replay = EventReplayEngine()
            loaded_events = replay.load_audit_trail(file_path)

            assert len(loaded_events) == num_deployments * 3

            # Verify all events are parseable and have required fields
            for ev in loaded_events:
                assert "event_id" in ev
                assert "event_type" in ev
                assert "deployment_id" in ev

            # Timeline reconstruction should sort deterministically
            timeline = replay.reconstruct_timeline(loaded_events)
            assert len(timeline) == num_deployments * 3

            # Verify file ordering matches memory ordering
            memory_events = logger.get_events()
            memory_ids = [e.event_id for e in memory_events]
            with open(file_path, "r", encoding="utf-8") as f:
                file_ids = [json.loads(line.strip())["event_id"] for line in f if line.strip()]
            assert memory_ids == file_ids
