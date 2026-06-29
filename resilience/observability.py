"""
Operational Observability Layer for aggregating metrics and analyzing trends from logs.
"""

from __future__ import annotations

from typing import Any, Dict, List

from logging_config import get_logger

logger = get_logger(__name__)


class OperationalObservabilityLayer:
    """Aggregates metrics and performs failure trend analyses from structured audit logs."""

    def __init__(self) -> None:
        self.metrics: Dict[str, Any] = {}

    def aggregate_metrics(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a list of events to calculate rollout statistics."""
        total_deployments = 0
        completions = 0
        failures = 0
        rollbacks_started = 0
        rollbacks_completed = 0
        rollbacks_initiated = 0
        policy_violations = 0
        approval_requests = 0
        approvals_approved = 0
        approvals_denied = 0

        durations: List[float] = []
        risk_scores: List[float] = []
        failure_regions: Dict[str, int] = {}
        violated_policies: Dict[str, int] = {}

        for ev in events:
            evt_type = ev.get("event_type")
            details = ev.get("details", {})

            if evt_type == "deployment_start":
                total_deployments += 1

            elif evt_type == "deployment_completed":
                completions += 1
                dur = details.get("duration_seconds")
                if dur is not None:
                    durations.append(float(dur))

            elif evt_type == "deployment_failed":
                failures += 1

            elif evt_type == "rollback_start":
                rollbacks_started += 1

            elif evt_type == "rollback_complete":
                rollbacks_completed += 1

            elif evt_type == "rollback_initiated":
                rollbacks_initiated += 1

            elif evt_type == "policy_violation":
                policy_violations += 1
                policy_name = details.get("policy_name", "Unknown")
                violated_policies[policy_name] = violated_policies.get(policy_name, 0) + 1

            elif evt_type == "approval_request":
                approval_requests += 1

            elif evt_type == "approval_decision":
                decision = details.get("decision")
                if decision == "APPROVED":
                    approvals_approved += 1
                elif decision == "DENIED":
                    approvals_denied += 1

            elif evt_type == "risk_score_transition":
                score = details.get("risk_score")
                if score is not None:
                    risk_scores.append(float(score))

            # Look for failure details
            if "error" in details or "reason" in details:
                reason = str(details.get("error", details.get("reason", "")))
                # Extract region from message if present
                for reg in ("us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"):
                    if reg in reason:
                        failure_regions[reg] = failure_regions.get(reg, 0) + 1

        avg_duration = sum(durations) / len(durations) if durations else 0.0
        avg_risk = sum(risk_scores) / len(risk_scores) if risk_scores else 0.0
        rollback_ratio = rollbacks_completed / total_deployments if total_deployments else 0.0

        self.metrics = {
            "total_deployments": total_deployments,
            "completions": completions,
            "failures": failures,
            "rollbacks_initiated": rollbacks_initiated,
            "rollbacks_started": rollbacks_started,
            "rollbacks_completed": rollbacks_completed,
            "rollback_ratio": rollback_ratio,
            "policy_violations": policy_violations,
            "approval_requests": approval_requests,
            "approvals_approved": approvals_approved,
            "approvals_denied": approvals_denied,
            "average_completed_duration_seconds": avg_duration,
            "average_risk_score": avg_risk,
            "failure_by_region": failure_regions,
            "violations_by_policy": violated_policies,
        }
        return self.metrics
