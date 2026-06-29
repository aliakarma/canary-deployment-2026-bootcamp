"""
Health thresholds for the Canary Deployment Simulator.

Defines the :class:`HealthThresholds` dataclass containing configurable limits
used by the health analyzer to evaluate server-level and cluster-wide health.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class HealthThresholds:
    """Configurable limits for defining healthy servers and cluster states.

    Attributes:
        max_server_cpu_usage: Maximum allowed CPU usage percentage on a single server.
        max_server_memory_usage: Maximum allowed memory usage percentage on a single server.
        max_degraded_server_percentage: Maximum allowed percentage of degraded servers in the cluster.
        max_failed_server_percentage: Maximum allowed percentage of failed servers in the cluster.
        max_unhealthy_server_percentage: Maximum allowed percentage of servers
            failing their individual health checks (a broader signal than the
            explicit DEGRADED status count).
        max_cluster_cpu_usage_avg: Maximum allowed average CPU usage percentage across the cluster.
        max_cluster_memory_usage_avg: Maximum allowed average memory usage percentage across the cluster.
        max_server_error_rate: Maximum allowed error rate percentage on a single server.
        max_server_latency_ms: Maximum allowed average request latency in milliseconds on a single server.
    """

    max_server_cpu_usage: float = 85.0
    max_server_memory_usage: float = 90.0
    max_degraded_server_percentage: float = 10.0
    max_failed_server_percentage: float = 0.0
    max_unhealthy_server_percentage: float = 10.0
    max_cluster_cpu_usage_avg: float = 75.0
    max_cluster_memory_usage_avg: float = 80.0
    max_server_error_rate: float = 5.0
    max_server_latency_ms: float = 500.0

    def __post_init__(self) -> None:
        """Validate thresholds on initialization."""
        for name, val in self.__dict__.items():
            if isinstance(val, (int, float)):
                if val < 0:
                    raise ValueError(f"Threshold '{name}' must be non-negative, got {val}")
                if "percentage" in name or "usage" in name or "rate" in name:
                    if val > 100.0 and "latency" not in name:
                        raise ValueError(
                            f"Percentage/usage threshold '{name}' cannot exceed 100, got {val}"
                        )
            else:
                raise TypeError(f"Threshold '{name}' must be numeric, got {type(val)}")
