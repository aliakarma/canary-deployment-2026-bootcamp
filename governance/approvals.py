"""
Human Approval Gates for the Governance Policy Engine.
"""

from __future__ import annotations

from typing import Callable

from governance.models import ApprovalDecision, ApprovalRequest


class ApprovalGate:
    """Simulates a manual/human approval gate for deployments."""

    def __init__(
        self,
        callback: Callable[[ApprovalRequest], bool] | None = None,
        auto_approve_below_risk: str = "HIGH",
    ) -> None:
        """Initialize ApprovalGate.

        Args:
            callback: Optional Callable[[ApprovalRequest], bool]. If it returns True,
                the request is approved; if False, denied.
            auto_approve_below_risk: String risk name representing the threshold
                below which requests are automatically approved.
        """
        self._callback = callback
        self._auto_approve_below_risk = auto_approve_below_risk

    def evaluate_request(self, request: ApprovalRequest, risk_category: str) -> ApprovalDecision:
        """Evaluate an approval request.

        Args:
            request: The ApprovalRequest.
            risk_category: The current RiskScore category name (e.g. "LOW").
        """
        # Validate risk hierarchy
        risk_hierarchy = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        try:
            req_index = risk_hierarchy.index(risk_category.upper())
            threshold_index = risk_hierarchy.index(self._auto_approve_below_risk.upper())
        except ValueError:
            req_index = 3
            threshold_index = 2

        if req_index < threshold_index and self._callback is None:
            request.status = ApprovalDecision.BYPASSED
            return ApprovalDecision.BYPASSED

        # If a callback is registered, delegate to it
        if self._callback is not None:
            try:
                approved = self._callback(request)
                decision = ApprovalDecision.APPROVED if approved else ApprovalDecision.DENIED
                request.status = decision
                return decision
            except Exception as exc:
                request.status = ApprovalDecision.DENIED
                request.details["error"] = str(exc)
                return ApprovalDecision.DENIED

        # Default to DENIED for HIGH/CRITICAL if no callback is registered
        request.status = ApprovalDecision.DENIED
        return ApprovalDecision.DENIED
