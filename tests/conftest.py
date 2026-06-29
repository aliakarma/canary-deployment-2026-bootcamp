"""
Shared pytest fixtures for the Canary Deployment Simulator test suite.

Provides standardized cluster state fixtures for deterministic testing.
"""

from __future__ import annotations

import pytest

from cluster.generator import generate_cluster
from cluster.state import ClusterState


@pytest.fixture
def cluster_state_10() -> ClusterState:
    """10-server cluster for deterministic testing."""
    return ClusterState(generate_cluster(size=10, seed=42))


@pytest.fixture
def cluster_state_20() -> ClusterState:
    """20-server cluster for deterministic testing."""
    return ClusterState(generate_cluster(size=20, seed=42))
