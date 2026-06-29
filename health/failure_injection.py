"""
Failure injection utility for the Canary Deployment Simulator.

Provides simulated chaos injection to degrade or fail a configurable subset
of servers in the cluster state, targeting specific versions or resource spikes.
"""

from __future__ import annotations

import math
import random
from typing import TYPE_CHECKING

from cluster.models import ServerStatus
from logging_config import get_logger

if TYPE_CHECKING:
    from cluster.state import ClusterState

logger = get_logger(__name__)


def inject_failures(
    cluster_state: ClusterState,
    *,
    target_version: str | None = None,
    failure_rate: float = 0.2,
    failure_type: str = "degrade",
    seed: int | None = None,
) -> list[str]:
    """Inject failures/spikes into a subset of servers.

    Mutates server states and resource usage metrics via thread-safe ClusterState methods.

    Args:
        cluster_state: The ClusterState instance representing the server cluster.
        target_version: Optional version string to restrict failure injection to.
        failure_rate: Proportion of matching servers to degrade (0.0 to 1.0).
        failure_type: Type of failure: "degrade", "fail", or "resource_spike".
        seed: Optional RNG seed for reproducible server selection and spikes.

    Returns:
        List of server IDs that were mutated.
    """
    if not (0.0 <= failure_rate <= 1.0):
        raise ValueError(f"failure_rate must be between 0.0 and 1.0, got {failure_rate}")

    # Gather matching candidate servers
    servers = cluster_state.servers
    if target_version is not None:
        candidates = [s for s in servers if s.current_version == target_version]
    else:
        candidates = list(servers)

    if not candidates:
        logger.warning(
            "Failure injection: No candidate servers found matching version '%s'",
            target_version,
        )
        return []

    # Seeding
    rng = random.Random(seed)

    # Sort to ensure stable list ordering before shuffle/sample
    candidates.sort(key=lambda s: s.id)
    k = math.ceil(len(candidates) * failure_rate)
    if k == 0 and failure_rate > 0:
        k = 1
    k = min(k, len(candidates))

    affected_servers = rng.sample(candidates, k)
    affected_ids: list[str] = []

    logger.info(
        "Injecting '%s' failures into %d/%d servers (version target: %s) ...",
        failure_type,
        k,
        len(candidates),
        target_version,
    )

    for server in affected_servers:
        if failure_type == "degrade":
            new_status = ServerStatus.DEGRADED
            cpu = rng.uniform(86.0, 95.0)
            mem = rng.uniform(81.0, 92.0)
        elif failure_type == "fail":
            new_status = ServerStatus.FAILED
            cpu = rng.uniform(96.0, 100.0)
            mem = rng.uniform(91.0, 99.0)
        elif failure_type == "resource_spike":
            # Retain current status (e.g. HEALTHY/UPDATING) but spike resource usage
            new_status = server.status
            cpu = rng.uniform(86.0, 98.0)
            mem = rng.uniform(91.0, 99.0)
        else:
            raise ValueError(f"Unknown failure_type: {failure_type}")

        # Update status first (thread-safe, writes history)
        if new_status != server.status:
            cluster_state.update_server_status(server.id, new_status)

        # Update CPU/memory resources (thread-safe)
        cluster_state.update_server_resources(server.id, cpu, mem)
        affected_ids.append(server.id)

    logger.info(
        "Chaos injected successfully. Mutated servers: %s",
        ", ".join(affected_ids) if affected_ids else "None",
    )
    return affected_ids
