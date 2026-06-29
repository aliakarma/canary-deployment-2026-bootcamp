"""
Event Replay Engine for timeline reconstruction, causality tracing, and log verification.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Set, Tuple

from logging_config import get_logger

logger = get_logger(__name__)


class EventReplayEngine:
    """Replays structured deployment audit logs to verify causality, timelines, and logic."""

    def __init__(self) -> None:
        self.replayed_events: List[Dict[str, Any]] = []

    def load_audit_trail(self, filepath: str) -> List[Dict[str, Any]]:
        """Load audit trail events from a JSONL file."""
        events = []
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    stripped = line.strip()
                    if stripped:
                        events.append(json.loads(stripped))
        except Exception as exc:
            logger.error("Failed to load audit trail file %s: %s", filepath, exc)
        return events

    def reconstruct_timeline(
        self,
        events: List[Dict[str, Any]],
        deployment_id: str | None = None,
    ) -> List[Dict[str, Any]]:
        """Reconstruct execution order, honouring causal parent→child linkage.

        A plain timestamp sort is ambiguous when many events share the same
        sub-second timestamp.  Instead we order primarily by the recorded
        ``parent_event_id`` chain (so a parent always precedes its children)
        and break ties by timestamp, which keeps the order stable and
        causally correct.

        Args:
            events: Raw events loaded from one or more audit trails.
            deployment_id: If provided, only events belonging to that
                deployment (by ``deployment_id`` or ``correlation_id``) are
                included — useful when a single audit file aggregates
                multiple independent deployments.
        """
        scoped = self._scope_to_deployment(events, deployment_id)

        # Index events and build the parent → children adjacency map.
        by_id: Dict[str, Dict[str, Any]] = {e["event_id"]: e for e in scoped if e.get("event_id")}
        children: Dict[str, List[str]] = {}
        for ev in scoped:
            event_id = ev.get("event_id")
            if not event_id:
                continue
            parent = ev.get("parent_event_id")
            key = parent if (parent and parent in by_id) else "ROOT"
            children.setdefault(key, []).append(event_id)

        ts = lambda eid: by_id[eid].get("timestamp", "")  # noqa: E731

        # Deterministic DFS from the roots, visiting children in timestamp
        # order, producing a parent-before-child ordering.
        ordered: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        stack = sorted(children.get("ROOT", []), key=ts, reverse=True)
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            ordered.append(by_id[node])
            stack.extend(sorted(children.get(node, []), key=ts, reverse=True))

        # Append any events not reachable from a root (orphans), by timestamp.
        orphans = [by_id[eid] for eid in by_id if eid not in seen]
        ordered.extend(sorted(orphans, key=lambda e: e.get("timestamp", "")))
        return ordered

    @staticmethod
    def _scope_to_deployment(
        events: List[Dict[str, Any]], deployment_id: str | None
    ) -> List[Dict[str, Any]]:
        """Filter events down to a single deployment, if requested."""
        if deployment_id is None:
            return list(events)
        return [
            e
            for e in events
            if e.get("deployment_id") == deployment_id or e.get("correlation_id") == deployment_id
        ]

    def build_causality_graph(self, events: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Build a parent-to-child causality map of event UUIDs.

        Returns:
            A dictionary mapping parent_event_id -> list of child_event_ids.
        """
        graph: Dict[str, List[str]] = {}
        for ev in events:
            parent = ev.get("parent_event_id")
            event_id = ev.get("event_id")
            if event_id:
                if parent:
                    graph.setdefault(parent, []).append(event_id)
                else:
                    graph.setdefault("ROOT", []).append(event_id)
        return graph

    def verify_event_lineage(self, events: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        """Validate parent-child event linkages to detect trace gaps, missing parents, or cycles.

        Returns:
            A tuple of (is_valid, list_of_error_messages).
        """
        errors: List[str] = []
        known_ids: Set[str] = {e["event_id"] for e in events if "event_id" in e}

        for ev in events:
            event_id = ev.get("event_id")
            parent_id = ev.get("parent_event_id")

            if not event_id:
                errors.append("Malformed event: missing 'event_id'.")
                continue

            if parent_id and parent_id not in known_ids:
                errors.append(
                    f"Causality gap: Event {event_id} references missing parent {parent_id}."
                )

            if parent_id and parent_id == event_id:
                errors.append(f"Causality corruption: Event {event_id} is its own parent.")

        # Reconstruct path traversal to detect cycles. An explicit stack is
        # used instead of recursion so that very long or malformed audit
        # trails cannot exhaust Python's recursion limit.
        graph = self.build_causality_graph(events)
        visited: Set[str] = set()

        for root in graph.get("ROOT", []):
            # Each frame on the stack is (node, on_path) where on_path marks
            # the post-visit "pop" that clears the node from the active path.
            path: Set[str] = set()
            stack: List[Tuple[str, bool]] = [(root, False)]
            while stack:
                node, finishing = stack.pop()
                if finishing:
                    path.discard(node)
                    continue
                if node in path:
                    errors.append(f"Causality loop detected: cycle involving event '{node}'")
                    continue
                if node in visited:
                    continue
                visited.add(node)
                path.add(node)
                stack.append((node, True))
                for child in graph.get(node, []):
                    stack.append((child, False))

        return (len(errors) == 0, errors)

    def reconstruct_state_at_step(
        self,
        events: List[Dict[str, Any]],
        step_event_id: str,
        deployment_id: str | None = None,
    ) -> Dict[str, Any]:
        """Reconstruct a virtual view of the cluster state at a specific step in the audit trail.

        Iterates sequentially over events up to `step_event_id` and aggregates transitions.

        Args:
            events: Raw events loaded from one or more audit trails.
            step_event_id: The event ID to reconstruct state up to (inclusive).
            deployment_id: If provided, restricts reconstruction to a single
                deployment so that aggregating multiple deployments in one
                audit file does not blend unrelated state.
        """
        timeline = self.reconstruct_timeline(events, deployment_id=deployment_id)

        # Virtual model
        virtual_servers: Dict[str, Dict[str, Any]] = {}
        deployment_status = "pending"
        target_version = None
        source_version = None

        for ev in timeline:
            evt_type = ev.get("event_type")
            details = ev.get("details", {})

            if evt_type == "deployment_start":
                target_version = details.get("target_version")
                source_version = details.get("source_version")
                deployment_status = "in_progress"

            elif evt_type == "stage_transition":
                servers_updated = details.get("servers_updated", [])
                for s_id in servers_updated:
                    virtual_servers[s_id] = {"version": target_version, "status": "healthy"}

            elif evt_type == "health_check":
                # If health check failed in details, update virtual server status
                if details.get("status") == "fail":
                    # Mark updated servers as degraded for simulated failures
                    for s_id, s_data in virtual_servers.items():
                        if s_data["version"] == target_version:
                            s_data["status"] = "degraded"

            elif evt_type == "rollback_complete":
                # Rollback reverted servers
                servers_reverted = details.get("servers_rolled_back", [])
                for s_id in servers_reverted:
                    if s_id in virtual_servers:
                        virtual_servers[s_id] = {"version": source_version, "status": "healthy"}
                deployment_status = "rolled_back"

            elif evt_type == "deployment_completed":
                deployment_status = "completed"

            elif evt_type == "rollback_initiated":
                deployment_status = "rolling_back"

            elif evt_type == "deployment_failed":
                deployment_status = "failed"

            if ev.get("event_id") == step_event_id:
                break

        return {
            "deployment_status": deployment_status,
            "target_version": target_version,
            "source_version": source_version,
            "servers": virtual_servers,
        }
