"""
FastAPI route definitions for the Canary Deployment Simulator dashboard.

All routes delegate to :class:`dashboard.state.SimulatorState` which
serialises access through a threading lock.  The routes themselves are
intentionally thin — they validate input, call the state manager, and
return JSON.
"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from dashboard.state import SimulatorState

router = APIRouter()

# The SimulatorState instance is injected by the application factory
# (see dashboard.app) and stored here so routes can access it.
_state: SimulatorState | None = None


def set_state(state: SimulatorState) -> None:
    """Bind the shared simulator state to the route module."""
    global _state
    _state = state


def _get_state() -> SimulatorState:
    assert _state is not None, "SimulatorState not initialised"
    return _state


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class InjectFailureRequest(BaseModel):
    failure_type: str = Field(default="degrade", description="degrade | fail | resource_spike")
    failure_rate: float = Field(default=0.3, ge=0.0, le=1.0)
    target_version: str | None = Field(default=None)


class StartRolloutRequest(BaseModel):
    target_version: str = Field(default="2.0.0")
    stages: List[int] | None = Field(default=None)


class ScenarioRequest(BaseModel):
    name: str = Field(description="successful_rollout | regional_failure | governance_block")
    region: str | None = Field(default=None)


# ------------------------------------------------------------------
# GET endpoints
# ------------------------------------------------------------------


@router.get("/cluster")
def get_cluster() -> Dict[str, Any]:
    """Return full cluster state: regions, nodes, versions, quarantine."""
    return _get_state().get_cluster_data()


@router.get("/events")
def get_events(limit: int = Query(default=100, ge=1, le=1000)) -> List[Dict[str, Any]]:
    """Return recent structured audit events."""
    return _get_state().get_events_data(limit=limit)


@router.get("/health")
def get_health() -> Dict[str, Any]:
    """Return aggregate health metrics and risk score."""
    return _get_state().get_health_data()


@router.get("/governance")
def get_governance() -> Dict[str, Any]:
    """Return live governance signals: decisions, approvals, quarantines."""
    return _get_state().get_governance_data()


@router.get("/replay/{event_id}")
def replay_event(event_id: str) -> Dict[str, Any]:
    """Reconstruct historical cluster state at a specific event."""
    return _get_state().replay_event(event_id)


@router.get("/replay_latest")
def replay_latest() -> Dict[str, Any]:
    """Reconstruct historical cluster state at the most recent event."""
    return _get_state().replay_latest()


# ------------------------------------------------------------------
# POST endpoints
# ------------------------------------------------------------------


@router.post("/inject_failure")
def inject_failure(body: InjectFailureRequest) -> Dict[str, Any]:
    """Trigger controlled chaos injection."""
    return _get_state().inject_failure(
        failure_type=body.failure_type,
        failure_rate=body.failure_rate,
        target_version=body.target_version,
    )


@router.post("/start_rollout")
def start_rollout(body: StartRolloutRequest) -> Dict[str, Any]:
    """Start a progressive canary rollout in the background."""
    return _get_state().start_rollout(
        target_version=body.target_version,
        stages=body.stages,
    )


@router.post("/scenario")
def run_scenario(body: ScenarioRequest) -> Dict[str, Any]:
    """Run a one-click demonstration scenario."""
    return _get_state().run_scenario(name=body.name, region=body.region)


@router.post("/rollback")
def rollback() -> Dict[str, Any]:
    """Trigger rollback of the current deployment."""
    return _get_state().trigger_rollback()


@router.post("/reset")
def reset() -> Dict[str, Any]:
    """Reset simulator state to fresh defaults."""
    return _get_state().reset()
