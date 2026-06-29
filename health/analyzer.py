"""
Health analysis engine for the Canary Deployment Simulator.

Evaluates cluster-wide health state by aggregating individual server reports
and checking them against configured health thresholds. Provides an adapter hook
for integration with the deployment engine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from cluster.models import ServerStatus
from cluster.state import ClusterState
from health.metrics import ServerHealthReport, evaluate_server_health
from health.thresholds import HealthThresholds
from logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class ClusterHealthReport:
    """Aggregated health report for the entire cluster.

    Attributes:
        overall_passed: True if the cluster satisfies all health thresholds.
        server_reports: Mapping of server ID to individual ServerHealthReport.
        degraded_count: Number of servers explicitly in DEGRADED status.
        failed_count: Number of servers explicitly in FAILED status.
        degraded_percentage: Percentage of degraded servers.
        failed_percentage: Percentage of failed servers.
        unhealthy_count: Number of servers failing their individual checks.
        unhealthy_percentage: Percentage of servers failing individual checks.
        avg_cpu_usage: Average CPU usage across the cluster.
        avg_memory_usage: Average memory usage across the cluster.
        failed_checks: List of descriptions of violated thresholds.
        regional_breakdown: Mapping of region name to detailed health stats.
    """

    overall_passed: bool
    server_reports: dict[str, ServerHealthReport]
    degraded_count: int
    failed_count: int
    degraded_percentage: float
    failed_percentage: float
    unhealthy_count: int
    unhealthy_percentage: float
    avg_cpu_usage: float
    avg_memory_usage: float
    failed_checks: list[str] = field(default_factory=list)
    regional_breakdown: dict[str, dict[str, Any]] = field(default_factory=dict)


def analyze(
    cluster_state: ClusterState,
    thresholds: HealthThresholds,
    seed: int | None = None,
) -> ClusterHealthReport:
    """Analyze the health of the entire cluster.

    Args:
        cluster_state: The ClusterState to analyze.
        thresholds: The thresholds to evaluate health against.
        seed: Optional seed modifier for simulated request metrics.

    Returns:
        A :class:`ClusterHealthReport` detailing the analysis results.
    """
    servers = cluster_state.servers
    total_servers = len(servers)

    server_reports: dict[str, ServerHealthReport] = {}
    degraded_count = 0
    failed_count = 0
    unhealthy_count = 0
    total_cpu = 0.0
    total_mem = 0.0

    # Evaluate each server individually
    for server in servers:
        report = evaluate_server_health(server, thresholds, seed=seed)
        server_reports[server.id] = report

        if server.status == ServerStatus.DEGRADED:
            degraded_count += 1
        elif server.status == ServerStatus.FAILED:
            failed_count += 1

        if not report.passed:
            unhealthy_count += 1

        total_cpu += server.cpu_usage
        total_mem += server.memory_usage

    # Compute aggregate metrics
    avg_cpu = (total_cpu / total_servers) if total_servers > 0 else 0.0
    avg_mem = (total_mem / total_servers) if total_servers > 0 else 0.0
    degraded_pct = (degraded_count / total_servers * 100.0) if total_servers > 0 else 0.0
    failed_pct = (failed_count / total_servers * 100.0) if total_servers > 0 else 0.0
    unhealthy_pct = (unhealthy_count / total_servers * 100.0) if total_servers > 0 else 0.0

    failed_checks: list[str] = []

    # Validate aggregate resource metrics
    if avg_cpu > thresholds.max_cluster_cpu_usage_avg:
        failed_checks.append(
            f"Average cluster CPU usage ({avg_cpu:.1f}%) exceeds threshold "
            f"({thresholds.max_cluster_cpu_usage_avg:.1f}%)"
        )
    if avg_mem > thresholds.max_cluster_memory_usage_avg:
        failed_checks.append(
            f"Average cluster memory usage ({avg_mem:.1f}%) exceeds threshold "
            f"({thresholds.max_cluster_memory_usage_avg:.1f}%)"
        )

    # Validate server status ratios
    if degraded_pct > thresholds.max_degraded_server_percentage:
        failed_checks.append(
            f"Degraded server percentage ({degraded_pct:.1f}%) exceeds threshold "
            f"({thresholds.max_degraded_server_percentage:.1f}%)"
        )
    if failed_pct > thresholds.max_failed_server_percentage:
        failed_checks.append(
            f"Failed server percentage ({failed_pct:.1f}%) exceeds threshold "
            f"({thresholds.max_failed_server_percentage:.1f}%)"
        )
    if unhealthy_pct > thresholds.max_unhealthy_server_percentage:
        failed_checks.append(
            f"Unhealthy server percentage ({unhealthy_pct:.1f}%) exceeds "
            f"threshold ({thresholds.max_unhealthy_server_percentage:.1f}%)"
        )

    overall_passed = len(failed_checks) == 0

    # Group metrics by region
    regional_servers: dict[str, list] = {}
    for s in servers:
        regional_servers.setdefault(s.region, []).append(s)

    regional_breakdown: dict[str, dict[str, Any]] = {}
    for r, r_servers in regional_servers.items():
        r_total = len(r_servers)
        r_degraded = sum(1 for s in r_servers if s.status == ServerStatus.DEGRADED)
        r_failed = sum(1 for s in r_servers if s.status == ServerStatus.FAILED)
        r_unhealthy = sum(1 for s in r_servers if not server_reports[s.id].passed)
        r_unhealthy_pct = (r_unhealthy / r_total * 100.0) if r_total > 0 else 0.0
        regional_breakdown[r] = {
            "total_servers": r_total,
            "degraded_count": r_degraded,
            "failed_count": r_failed,
            "unhealthy_percentage": round(r_unhealthy_pct, 1),
        }

    return ClusterHealthReport(
        overall_passed=overall_passed,
        server_reports=server_reports,
        degraded_count=degraded_count,
        failed_count=failed_count,
        degraded_percentage=round(degraded_pct, 1),
        failed_percentage=round(failed_pct, 1),
        unhealthy_count=unhealthy_count,
        unhealthy_percentage=round(unhealthy_pct, 1),
        avg_cpu_usage=round(avg_cpu, 1),
        avg_memory_usage=round(avg_mem, 1),
        failed_checks=failed_checks,
        regional_breakdown=regional_breakdown,
    )


def create_health_check_fn(
    thresholds: HealthThresholds,
    seed: int | None = None,
) -> Callable[[ClusterState], bool]:
    """Factory creating a health check callback for DeploymentConfig.

    The returned function evaluates the cluster state and returns True if
    health standards are met.

    Args:
        thresholds: The HealthThresholds to apply.
        seed: Optional seed modifier for the simulated metric analyzer.

    Returns:
        Callable[[ClusterState], bool]: A deployment engine compatible health hook.
    """

    def health_check_fn(cluster_state: ClusterState) -> bool:
        report = analyze(cluster_state, thresholds, seed=seed)
        if report.overall_passed:
            logger.info(
                "Health analysis: PASSED. Avg CPU: %.1f%%, Avg Mem: %.1f%%, "
                "Degraded: %d/%d, Failed: %d/%d",
                report.avg_cpu_usage,
                report.avg_memory_usage,
                report.degraded_count,
                len(cluster_state.servers),
                report.failed_count,
                len(cluster_state.servers),
            )
            return True
        else:
            logger.warning(
                "Health analysis: FAILED. Violations: %s",
                "; ".join(report.failed_checks),
            )
            return False

    return health_check_fn
