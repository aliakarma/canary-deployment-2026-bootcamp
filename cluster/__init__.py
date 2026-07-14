"""
Cluster simulation package for the Canary Deployment Simulator.

Re-exports the key public symbols so that consumers can write::

    from cluster import Server, ServerStatus, generate_cluster, ClusterState, inspect_cluster
"""

from cluster.generator import generate_cluster
from cluster.inspector import inspect_cluster
from cluster.models import Server, ServerStatus
from cluster.state import ClusterState

__all__ = [
    "Server",
    "ServerStatus",
    "generate_cluster",
    "ClusterState",
    "inspect_cluster",
]
