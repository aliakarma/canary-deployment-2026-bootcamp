"""
Deployment coordination package for the Canary Deployment Simulator.

Re-exports the key public symbols::

    from deploy import DeploymentEngine, DeploymentConfig
    from deploy import DeploymentState, DeploymentStatus, StageResult
"""

from deploy.abort_listener import ConsoleAbortListener
from deploy.audit import AuditLogger, DeploymentEvent, DeploymentEventType
from deploy.config import DeploymentConfig
from deploy.engine import DeploymentEngine
from deploy.rollback import (
    RollbackConsistencyError,
    load_deployment_state,
    save_deployment_state,
    validate_rollback_consistency,
)
from deploy.state import DeploymentState, DeploymentStatus, StageResult

__all__ = [
    "DeploymentConfig",
    "DeploymentEngine",
    "DeploymentState",
    "DeploymentStatus",
    "StageResult",
    "save_deployment_state",
    "load_deployment_state",
    "validate_rollback_consistency",
    "RollbackConsistencyError",
    "ConsoleAbortListener",
    "AuditLogger",
    "DeploymentEvent",
    "DeploymentEventType",
]
