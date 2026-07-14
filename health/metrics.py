"""
Health metrics evaluation for the Canary Deployment Simulator.

Defines the :class:`ServerHealthReport` dataclass and functions to simulate
request-level metrics (error rates and latencies) and evaluate them
against configurable thresholds.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from cluster.models import ServerStatus

if TYPE_CHECKING:
    from cluster.models import Server
    from health.thresholds import HealthThresholds


@dataclass
class ServerHealthReport:
    """Detailed health evaluation report for a single server.

    Attributes:
        server_id: ID of the evaluated server.
        status: Status of the server at evaluation time.
        cpu_usage: CPU utilization percentage.
        memory_usage: Memory utilization percentage.
        error_rate: Simulated request error rate percentage.
        latency_ms: Simulated average request latency in milliseconds.
        passed: True if all health metrics satisfy thresholds.
        failed_metrics: List of metric names that violated their thresholds.
    """

    server_id: str
    status: ServerStatus
    cpu_usage: float
    memory_usage: float
    error_rate: float
    latency_ms: float
    passed: bool
    failed_metrics: list[str] = field(default_factory=list)


def simulate_server_metrics(server: Server, seed: int | None = None) -> tuple[float, float]:
    """Simulate request error rate and average latency for a server.

    Simulation is deterministic and reproducible based on the server's ID
    and status. An optional seed can be provided to vary the simulation.

    Args:
        server: The server to simulate metrics for.
        seed: Optional seed modifier to alter randomness.

    Returns:
        A tuple of (error_rate_percentage, latency_ms).
    """
    # Parse the server ID number to create a deterministic baseline seed
    try:
        server_num = int(server.id.split("-")[1])
    except (IndexError, ValueError):
        import zlib

        server_num = zlib.adler32(server.id.encode("utf-8"))

    rng_seed = server_num if seed is None else (server_num + seed)
    rng = random.Random(rng_seed)

    if server.status == ServerStatus.HEALTHY:
        # Healthy servers: low error rate, fast response
        error_rate = rng.uniform(0.05, 1.2)
        latency = rng.uniform(40.0, 120.0)
    elif server.status == ServerStatus.UPDATING:
        # Updating servers: minor overhead
        error_rate = rng.uniform(0.1, 1.8)
        latency = rng.uniform(70.0, 180.0)
    elif server.status == ServerStatus.DEGRADED:
        # Degraded servers: elevated error rates and latencies
        error_rate = rng.uniform(6.0, 15.0)
        latency = rng.uniform(350.0, 800.0)
    elif server.status == ServerStatus.FAILED:
        # Failed servers: high error rate, timeouts
        error_rate = 100.0
        latency = 5000.0
    else:
        error_rate = 0.0
        latency = 50.0

    return round(error_rate, 2), round(latency, 1)


def evaluate_server_health(
    server: Server,
    thresholds: HealthThresholds,
    seed: int | None = None,
) -> ServerHealthReport:
    """Evaluate all health metrics of a server against the given thresholds.

    Args:
        server: The server instance to evaluate.
        thresholds: The thresholds to validate metrics against.
        seed: Optional seed modifier for simulating error rates and latencies.

    Returns:
        A :class:`ServerHealthReport` detailing the evaluation.
    """
    error_rate, latency_ms = simulate_server_metrics(server, seed=seed)
    failed_metrics: list[str] = []

    # 1. Resource metrics validation
    if server.cpu_usage > thresholds.max_server_cpu_usage:
        failed_metrics.append("cpu_usage")
    if server.memory_usage > thresholds.max_server_memory_usage:
        failed_metrics.append("memory_usage")

    # 2. Simulated request metrics validation
    if error_rate > thresholds.max_server_error_rate:
        failed_metrics.append("error_rate")
    if latency_ms > thresholds.max_server_latency_ms:
        failed_metrics.append("latency_ms")

    # 3. Explicit failed status check
    if server.status == ServerStatus.FAILED:
        failed_metrics.append("status_failed")

    passed = len(failed_metrics) == 0

    return ServerHealthReport(
        server_id=server.id,
        status=server.status,
        cpu_usage=server.cpu_usage,
        memory_usage=server.memory_usage,
        error_rate=error_rate,
        latency_ms=latency_ms,
        passed=passed,
        failed_metrics=failed_metrics,
    )
