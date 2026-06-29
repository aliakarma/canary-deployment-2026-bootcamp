"""
Unit tests for the Phase 7 Structured Event Logging & Auditability module.
"""

from __future__ import annotations

import datetime
import json
import os
import tempfile
import threading
import time

from cluster.generator import generate_cluster
from cluster.state import ClusterState
from deploy.audit import AuditLogger, DeploymentEvent, DeploymentEventType
from deploy.config import DeploymentConfig
from deploy.engine import DeploymentEngine
from deploy.rollback import rollback
from deploy.state import DeploymentStatus


class TestAuditLogging:
    """Comprehensive tests for structured deployment event logs."""

    def test_event_serialization(self) -> None:
        """Verify that DeploymentEvent is serializable and converts properly."""
        now = datetime.datetime.now(datetime.timezone.utc)
        event = DeploymentEvent(
            event_type=DeploymentEventType.DEPLOYMENT_START,
            deployment_id="dep-123",
            timestamp=now,
            details={"version": "2.0.0", "servers": 20},
        )

        d = event.to_dict()
        assert d["event_type"] == "deployment_start"
        assert d["deployment_id"] == "dep-123"
        assert d["timestamp"] == now.isoformat()
        assert d["details"]["version"] == "2.0.0"
        assert d["details"]["servers"] == 20

    def test_logger_in_memory_recording(self) -> None:
        """Verify AuditLogger saves events in memory."""
        logger = AuditLogger()
        event1 = DeploymentEvent(DeploymentEventType.DEPLOYMENT_START, "dep-1")
        event2 = DeploymentEvent(DeploymentEventType.STAGE_TRANSITION, "dep-1")

        logger.log(event1)
        logger.log(event2)

        events = logger.get_events()
        assert len(events) == 2
        assert events[0].event_type == DeploymentEventType.DEPLOYMENT_START
        assert events[1].event_type == DeploymentEventType.STAGE_TRANSITION

        logger.clear()
        assert len(logger.get_events()) == 0

    def test_logger_concurrency_safety(self) -> None:
        """Verify AuditLogger is safe for concurrent writes across threads."""
        logger = AuditLogger()
        num_threads = 10
        events_per_thread = 50

        def worker(thread_idx: int) -> None:
            for i in range(events_per_thread):
                event = DeploymentEvent(
                    event_type=DeploymentEventType.HEALTH_CHECK,
                    deployment_id=f"dep-{thread_idx}",
                    details={"index": i},
                )
                logger.log(event)

        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        events = logger.get_events()
        assert len(events) == num_threads * events_per_thread

    def test_logger_file_export_jsonl(self) -> None:
        """Verify events are properly exported as JSON lines (JSONL) on disk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "logs", "nested", "audit.jsonl")
            logger = AuditLogger(file_path=file_path)

            event1 = DeploymentEvent(
                event_type=DeploymentEventType.DEPLOYMENT_START,
                deployment_id="dep-foo",
                details={"version": "1.2.3"},
            )
            event2 = DeploymentEvent(
                event_type=DeploymentEventType.DEPLOYMENT_COMPLETED,
                deployment_id="dep-foo",
            )

            logger.log(event1)
            logger.log(event2)

            # Verify file exists
            assert os.path.exists(file_path)

            # Read lines and load JSON
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            assert len(lines) == 2

            data1 = json.loads(lines[0].strip())
            data2 = json.loads(lines[1].strip())

            assert data1["event_type"] == "deployment_start"
            assert data1["deployment_id"] == "dep-foo"
            assert data1["details"]["version"] == "1.2.3"
            assert data2["event_type"] == "deployment_completed"

    def test_engine_integration_success(self) -> None:
        """Verify successful staged rollout records expected event sequences."""
        cluster = ClusterState(generate_cluster(size=10, seed=42))
        audit = AuditLogger()
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.0,
            audit_logger=audit,
        )
        engine = DeploymentEngine(cluster)
        res = engine.deploy(config)

        assert res.status == DeploymentStatus.COMPLETED

        events = audit.get_events()
        # Expect at least: DEPLOYMENT_START, STAGE_TRANSITION, HEALTH_CHECK, STAGE_TRANSITION, HEALTH_CHECK, DEPLOYMENT_COMPLETED
        assert len(events) >= 5
        assert events[0].event_type == DeploymentEventType.DEPLOYMENT_START
        assert events[-1].event_type == DeploymentEventType.DEPLOYMENT_COMPLETED

        # Verify details
        start_details = events[0].details
        assert start_details["target_version"] == "2.0.0"
        assert start_details["total_servers"] == 10
        assert start_details["stages"] == [50, 100]

        comp_details = events[-1].details
        assert comp_details["target_version"] == "2.0.0"
        assert len(comp_details["servers_updated"]) == 10

    def test_engine_integration_aborted(self) -> None:
        """Verify aborted rollout records abort and rollback events."""
        cluster = ClusterState(generate_cluster(size=10, seed=42))
        audit = AuditLogger()
        abort_ev = threading.Event()
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=10.0,  # Ensure we have time to abort during delay
            abort_event=abort_ev,
            audit_logger=audit,
        )
        engine = DeploymentEngine(cluster)

        # Start in separate thread so we can trigger abort
        res_container = []

        def deploy_thread() -> None:
            res_container.append(engine.deploy(config))

        t = threading.Thread(target=deploy_thread)
        t.start()

        # Wait until stage 0 executes, then abort
        time.sleep(0.3)
        abort_ev.set()
        t.join()

        res = res_container[0]
        assert res.status == DeploymentStatus.ABORTED

        events = audit.get_events()
        event_types = [e.event_type for e in events]

        # Verify key abort sequence events are logged
        assert DeploymentEventType.DEPLOYMENT_START in event_types
        assert DeploymentEventType.ABORT_RECEIVED in event_types
        assert DeploymentEventType.ROLLBACK_START in event_types
        assert DeploymentEventType.ROLLBACK_COMPLETE in event_types
        assert DeploymentEventType.ROLLBACK_INITIATED in event_types

        # ROLLBACK_INITIATED replaces DEPLOYMENT_FAILED in rollback flows
        # DEPLOYMENT_FAILED should NOT appear — abort+rollback is a recovery, not a failure
        assert DeploymentEventType.DEPLOYMENT_FAILED not in event_types

        # Verify abort reason
        abort_event = [e for e in events if e.event_type == DeploymentEventType.ABORT_RECEIVED][0]
        assert "inter-stage wait" in abort_event.details["reason"]

    def test_manual_rollback_logging(self) -> None:
        """Verify manual rollback records ROLLBACK_START and ROLLBACK_COMPLETE events."""
        cluster = ClusterState(generate_cluster(size=5, seed=42))
        audit = AuditLogger()
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[100],
            stage_delay_seconds=0.0,
        )
        engine = DeploymentEngine(cluster)
        dep_state = engine.deploy(config)

        # Perform manual rollback with audit logging
        rolled_back = rollback(cluster, dep_state, force=True, audit_logger=audit)
        assert len(rolled_back) == 5

        events = audit.get_events()
        assert len(events) == 2
        assert events[0].event_type == DeploymentEventType.ROLLBACK_START
        assert events[1].event_type == DeploymentEventType.ROLLBACK_COMPLETE

        assert events[0].details["source_version"] == "1.0.0"
        assert events[1].details["servers_rolled_back"] == rolled_back


class TestAuditLoggerAtomicOrdering:
    """Tests verifying that AuditLogger maintains consistent ordering
    between in-memory events and file-backed JSONL output under concurrency."""

    def test_audit_logger_atomic_ordering(self) -> None:
        """Verify in-memory event ordering matches file ordering under concurrent writes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "ordering_test.jsonl")
            logger = AuditLogger(file_path=file_path)
            num_threads = 20
            events_per_thread = 50

            def worker(thread_idx: int) -> None:
                for i in range(events_per_thread):
                    event = DeploymentEvent(
                        event_type=DeploymentEventType.HEALTH_CHECK,
                        deployment_id=f"dep-{thread_idx}",
                        details={"thread": thread_idx, "index": i},
                    )
                    logger.log(event)

            threads = []
            for idx in range(num_threads):
                t = threading.Thread(target=worker, args=(idx,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            # Verify counts match
            memory_events = logger.get_events()
            assert len(memory_events) == num_threads * events_per_thread

            with open(file_path, "r", encoding="utf-8") as f:
                file_lines = [line.strip() for line in f if line.strip()]
            assert len(file_lines) == num_threads * events_per_thread

            # Verify ordering: in-memory event_ids must match file event_ids in same order
            memory_ids = [e.event_id for e in memory_events]
            file_ids = [json.loads(line)["event_id"] for line in file_lines]
            assert memory_ids == file_ids

    def test_concurrent_file_write_ordering(self) -> None:
        """Verify JSONL file lines are valid JSON and maintain sequential consistency."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "concurrent_write.jsonl")
            logger = AuditLogger(file_path=file_path)
            num_threads = 20
            events_per_thread = 30

            def worker(thread_idx: int) -> None:
                for i in range(events_per_thread):
                    event = DeploymentEvent(
                        event_type=DeploymentEventType.STAGE_TRANSITION,
                        deployment_id=f"dep-{thread_idx}",
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

            # Every line must be valid JSON (no interleaving/corruption)
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            assert len(lines) == num_threads * events_per_thread
            for line in lines:
                parsed = json.loads(line.strip())
                assert "event_id" in parsed
                assert "event_type" in parsed
                assert "deployment_id" in parsed
