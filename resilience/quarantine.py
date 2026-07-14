"""
Region Quarantine System to isolate unstable regions and route rollouts around them.
"""

from __future__ import annotations

import datetime
import threading
from typing import Any, Dict, List, Set

from cluster.models import ServerStatus
from cluster.state import ClusterState
from logging_config import get_logger
from resilience.models import QuarantineState, QuarantineStatus

logger = get_logger(__name__)


class RegionQuarantineSystem:
    """Manages region quarantines, isolated scopes, and release workflows."""

    def __init__(self, cluster: ClusterState) -> None:
        self.cluster = cluster
        self._quarantines: Dict[str, QuarantineState] = {}
        self._lock = threading.Lock()

    def quarantine_region(
        self, region: str, reason: str, metadata: Dict[str, Any] | None = None
    ) -> QuarantineState:
        """Quarantine a region, freezing updates and routing deployments around it."""
        with self._lock:
            state = QuarantineState(
                region=region,
                status=QuarantineStatus.ACTIVE,
                reason=reason,
                quarantined_at=datetime.datetime.now(datetime.timezone.utc),
                metadata=metadata or {},
            )
            self._quarantines[region] = state
            logger.warning(
                "REGION QUARANTINED: region '%s' is now isolated. Reason: %s", region, reason
            )
            return state

    def release_region(self, region: str, metadata: Dict[str, Any] | None = None) -> bool:
        """Release a region from quarantine, returning it to service."""
        with self._lock:
            state = self._quarantines.get(region)
            if state is None or state.status == QuarantineStatus.RELEASED:
                logger.debug(
                    "Quarantine release ignored: region '%s' is not active in quarantine.", region
                )
                return False

            state.status = QuarantineStatus.RELEASED
            state.released_at = datetime.datetime.now(datetime.timezone.utc)
            if metadata:
                state.metadata.update(metadata)
            logger.info("REGION RELEASED: region '%s' has been returned to service.", region)
            return True

    def is_quarantined(self, region: str) -> bool:
        """Check if a region is currently quarantined."""
        with self._lock:
            state = self._quarantines.get(region)
            return state is not None and state.status == QuarantineStatus.ACTIVE

    def get_quarantined_regions(self) -> Set[str]:
        """Return the set of all active quarantined regions."""
        with self._lock:
            return {r for r, s in self._quarantines.items() if s.status == QuarantineStatus.ACTIVE}

    def check_and_auto_quarantine(self, threshold_percentage: float = 30.0) -> List[str]:
        """Examine regional degradation and auto-quarantine regions exceeding threshold.

        Threshold percentage is evaluated on (degraded + failed) servers in each region.
        """
        # Group servers by region
        by_region: Dict[str, list] = {}
        for server in self.cluster.servers:
            by_region.setdefault(server.region, []).append(server)

        quarantined = []
        for region, servers in by_region.items():
            if not servers:
                continue

            unstable_count = sum(
                1 for s in servers if s.status in (ServerStatus.DEGRADED, ServerStatus.FAILED)
            )
            pct_unstable = (unstable_count / len(servers)) * 100.0

            if pct_unstable >= threshold_percentage:
                if not self.is_quarantined(region):
                    self.quarantine_region(
                        region=region,
                        reason=f"Auto-quarantine: regional instability threshold exceeded ({pct_unstable:.1f}% >= {threshold_percentage:.1f}%)",
                        metadata={
                            "degraded_pct": pct_unstable,
                            "degraded_count": unstable_count,
                            "total_count": len(servers),
                        },
                    )
                    quarantined.append(region)

        return quarantined

    def get_all_quarantine_states(self) -> List[QuarantineState]:
        """Retrieve a copy of all current quarantine records."""
        with self._lock:
            return list(self._quarantines.values())
