"""
Governance models, enums, and dataclasses for the Canary Deployment Simulator.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict


class RiskScore(str, Enum):
    """Classification of deployment risk levels."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ApprovalDecision(str, Enum):
    """The outcome of an approval checkpoint request."""

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    DENIED = "DENIED"
    BYPASSED = "BYPASSED"


class GovernanceDecision(str, Enum):
    """Actions decreed by the Governance Policy Engine."""

    ALLOW = "ALLOW"
    BLOCK = "BLOCK"
    ROLLBACK = "ROLLBACK"


@dataclass
class ApprovalRequest:
    """A formal request for operational execution approval."""

    request_id: str
    deployment_id: str
    stage_index: int
    reason: str
    status: ApprovalDecision = ApprovalDecision.PENDING
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PolicyEvaluationResult:
    """The result of evaluating a single governance policy rule."""

    policy_name: str
    passed: bool
    decision: GovernanceDecision
    message: str
