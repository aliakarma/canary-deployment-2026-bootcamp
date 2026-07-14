"""
Cluster inspection utility for the Canary Deployment Simulator.

Produces formatted terminal reports showing cluster health, version
distribution, and region breakdown.
"""

from __future__ import annotations

import re
import sys

from cluster.models import ServerStatus
from cluster.state import ClusterState
from logging_config import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# ANSI helpers
# ---------------------------------------------------------------------------
_SUPPORTS_COLOUR = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

if _SUPPORTS_COLOUR and sys.platform == "win32":
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        _SUPPORTS_COLOUR = False


def _c(code: str, text: str) -> str:
    """Wrap *text* in an ANSI colour code if the terminal supports it."""
    if _SUPPORTS_COLOUR:
        return f"\033[{code}m{text}\033[0m"
    return text


_GREEN = "32"
_YELLOW = "33"
_RED = "31"
_CYAN = "36"
_BOLD = "1"
_DIM = "2"

# Box-drawing characters
_TL = "╔"
_TR = "╗"
_BL = "╚"
_BR = "╝"
_H = "═"
_V = "║"
_ML = "╠"
_MR = "╣"

_WIDTH = 64  # inner width between the vertical bars


def _bar(fraction: float, max_width: int = 30) -> str:
    """Return an ASCII progress bar for the given fraction (0.0 – 1.0)."""
    filled = int(fraction * max_width)
    return "█" * filled + "░" * (max_width - filled)


def _status_colour(status: ServerStatus) -> str:
    """Return the ANSI colour code for a given server status."""
    return {
        ServerStatus.HEALTHY: _GREEN,
        ServerStatus.DEGRADED: _YELLOW,
        ServerStatus.FAILED: _RED,
        ServerStatus.UPDATING: _CYAN,
    }.get(status, "0")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def inspect_cluster(state: ClusterState, *, verbose: bool = False) -> str:
    """Generate a formatted cluster status report.

    Args:
        state: The :class:`ClusterState` to inspect.
        verbose: If ``True``, include a per-server detail table.

    Returns:
        The formatted report as a string (also printed to stdout).
    """
    summary = state.get_deployment_summary()
    servers = state.servers
    total = state.size

    lines: list[str] = []

    def add(content: str = "") -> None:
        lines.append(content)

    # ── Header ──────────────────────────────────────────────────────
    add(f"{_TL}{_H * _WIDTH}{_TR}")
    title = "CLUSTER STATUS REPORT"
    pad = (_WIDTH - len(title)) // 2
    add(f"{_V}{' ' * pad}{_c(_BOLD, title)}{' ' * (_WIDTH - pad - len(title))}{_V}")
    add(f"{_ML}{_H * _WIDTH}{_MR}")

    # ── Summary ─────────────────────────────────────────────────────
    total_line = f"  Total Servers: {_c(_BOLD, str(total))}"
    add(f"{_V}{total_line:<{_WIDTH + (_len_ansi_offset(total_line))}}{_V}")

    status_parts: list[str] = []
    for st in ServerStatus:
        count = summary["statuses"].get(st.value, 0)
        colour = _status_colour(st)
        label = f"{st.value.capitalize()}: {_c(colour, str(count))}"
        status_parts.append(label)

    status_line = "  " + "  │  ".join(status_parts)
    add(f"{_V}{status_line:<{_WIDTH + (_len_ansi_offset(status_line))}}{_V}")
    add(f"{_ML}{_H * _WIDTH}{_MR}")

    # ── Version Distribution ────────────────────────────────────────
    add(
        f"{_V}  {_c(_BOLD, 'Version Distribution'):<{_WIDTH - 2 + _len_ansi_offset(_c(_BOLD, 'Version Distribution'))}}{_V}"
    )

    for version, count in sorted(summary["versions"].items()):
        frac = count / total if total > 0 else 0
        bar = _bar(frac)
        pct = f"{frac * 100:.0f}%"
        ver_line = f"    v{version}  {_c(_CYAN, bar)}  {count} ({pct})"
        add(f"{_V}{ver_line:<{_WIDTH + _len_ansi_offset(ver_line)}}{_V}")

    add(f"{_ML}{_H * _WIDTH}{_MR}")

    # ── Region Breakdown ────────────────────────────────────────────
    add(
        f"{_V}  {_c(_BOLD, 'Region Breakdown'):<{_WIDTH - 2 + _len_ansi_offset(_c(_BOLD, 'Region Breakdown'))}}{_V}"
    )

    regions: dict[str, int] = {}
    for s in servers:
        regions[s.region] = regions.get(s.region, 0) + 1

    for region, count in sorted(regions.items(), key=lambda x: -x[1]):
        reg_line = f"    {region:<20} {_c(_DIM, str(count) + ' servers')}"
        add(f"{_V}{reg_line:<{_WIDTH + _len_ansi_offset(reg_line)}}{_V}")

    # ── Verbose: Per-server table ───────────────────────────────────
    if verbose:
        add(f"{_ML}{_H * _WIDTH}{_MR}")
        header = f"  {'ID':<12} {'Status':<10} {'Version':<10} {'Region':<16} {'CPU':>5} {'MEM':>5}"
        add(f"{_V}{_c(_BOLD, header):<{_WIDTH + _len_ansi_offset(_c(_BOLD, header))}}{_V}")
        add(f"{_V}  {'-' * (_WIDTH - 4)}{' ' * 2}{_V}")

        for s in servers:
            colour = _status_colour(s.status)
            status_str = _c(colour, f"{s.status.value:<10}")
            row = (
                f"  {s.id:<12} {status_str} {s.current_version:<10} "
                f"{s.region:<16} {s.cpu_usage:5.1f} {s.memory_usage:5.1f}"
            )
            add(f"{_V}{row:<{_WIDTH + _len_ansi_offset(row)}}{_V}")

    # ── Footer ──────────────────────────────────────────────────────
    add(f"{_BL}{_H * _WIDTH}{_BR}")

    report = "\n".join(lines)

    # Write with encoding-error resilience for Windows cp1252 terminals
    try:
        print(report)
    except UnicodeEncodeError:
        sys.stdout.write(report.encode("utf-8", errors="replace").decode("utf-8") + "\n")
    logger.debug("Cluster inspection report generated (%d servers)", total)
    return report


def _len_ansi_offset(text: str) -> int:
    """Return the number of extra characters from ANSI escape codes in *text*.

    This is used to compensate ``str.ljust`` / ``:<`` formatting which
    counts escape codes as visible characters.
    """
    ansi_codes = re.findall(r"\033\[[0-9;]*m", text)
    return sum(len(code) for code in ansi_codes)
