"""
Cluster generator for the Canary Deployment Simulator.

Creates a realistic simulated cluster of 20–50 servers distributed across
multiple cloud regions with randomised resource utilisation.
"""

from __future__ import annotations

import random
from datetime import datetime

from cluster.models import Server, ServerStatus
from logging_config import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_MIN_SERVERS = 20
DEFAULT_MAX_SERVERS = 50
INITIAL_VERSION = "1.0.0"

REGIONS: list[str] = [
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "ap-southeast-1",
]

# Mapping from region to hostname subdomain
_REGION_SUBDOMAINS: dict[str, str] = {
    "us-east-1": "use1",
    "us-west-2": "usw2",
    "eu-west-1": "euw1",
    "ap-southeast-1": "apse1",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_cluster(
    size: int | None = None,
    *,
    seed: int | None = None,
) -> list[Server]:
    """Generate a simulated cluster of servers.

    Args:
        size: Exact number of servers to create.  When ``None`` a random
            count between *DEFAULT_MIN_SERVERS* and *DEFAULT_MAX_SERVERS*
            (inclusive) is chosen.
        seed: Optional RNG seed for reproducible cluster generation.

    Returns:
        A list of :class:`Server` instances ready for deployment simulation.

    Raises:
        ValueError: If *size* is less than 1.
    """
    # Use a local RNG instance so cluster generation never mutates the global
    # ``random`` state (which would couple successive calls and perturb any
    # other code relying on the module-level RNG).
    rng = random.Random(seed)

    if size is None:
        size = rng.randint(DEFAULT_MIN_SERVERS, DEFAULT_MAX_SERVERS)
    elif size < 1:
        raise ValueError(f"Cluster size must be ≥ 1, got {size}")

    logger.info("Generating cluster with %d servers across %d regions ...", size, len(REGIONS))

    servers: list[Server] = []
    now = datetime.now()

    for i in range(1, size + 1):
        region = REGIONS[i % len(REGIONS)]
        subdomain = _REGION_SUBDOMAINS[region]
        server_id = f"server-{i:03d}"

        server = Server(
            id=server_id,
            hostname=f"node-{subdomain}-{i:03d}.internal",
            ip_address=f"10.0.{(i // 256) % 256}.{i % 256}",
            region=region,
            current_version=INITIAL_VERSION,
            previous_version=None,
            status=ServerStatus.HEALTHY,
            cpu_usage=round(rng.uniform(30.0, 70.0), 1),
            memory_usage=round(rng.uniform(30.0, 70.0), 1),
            last_health_check=now,
            deployment_history=[
                {
                    "version": INITIAL_VERSION,
                    "timestamp": now.isoformat(),
                    "action": "initial_deployment",
                }
            ],
        )
        servers.append(server)

    # Log summary by region
    region_counts: dict[str, int] = {}
    for s in servers:
        region_counts[s.region] = region_counts.get(s.region, 0) + 1

    logger.info(
        "Cluster generated: %d servers | Regions: %s",
        len(servers),
        ", ".join(f"{r}={c}" for r, c in sorted(region_counts.items())),
    )

    return servers
