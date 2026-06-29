"""
Health analysis and monitoring module for the Canary Deployment Simulator.

Provides data models, simulators, and analysis engines to evaluate individual
servers and cluster-wide health. Includes failure injection tools to simulate chaos.
"""

from health.analyzer import ClusterHealthReport, analyze, create_health_check_fn
from health.failure_injection import inject_failures
from health.metrics import ServerHealthReport, evaluate_server_health, simulate_server_metrics
from health.thresholds import HealthThresholds

__all__ = [
    "HealthThresholds",
    "ServerHealthReport",
    "evaluate_server_health",
    "simulate_server_metrics",
    "inject_failures",
    "ClusterHealthReport",
    "analyze",
    "create_health_check_fn",
]
