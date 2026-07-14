"""
Centralised logging configuration for the Canary Deployment Simulator.

Provides dual-output logging:
  - Console: Coloured, human-readable (INFO level)
  - File:    Detailed, timestamped (DEBUG level) → logs/canary_deployment.log

Usage:
    from logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("Deployment started")
"""

import logging
import logging.config
import os
import sys

# ---------------------------------------------------------------------------
# Fix Windows console encoding — ensures Unicode output works on cp1252 terminals
# ---------------------------------------------------------------------------
if sys.platform == "win32":
    for _stream_name in ("stdout", "stderr"):
        _stream = getattr(sys, _stream_name)
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, "canary_deployment.log")

# Ensure the log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# ANSI colour codes (gracefully degraded on unsupported terminals)
# ---------------------------------------------------------------------------
_SUPPORTS_COLOUR = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

if _SUPPORTS_COLOUR:
    # Enable ANSI escape sequences on Windows 10+
    if sys.platform == "win32":
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
        except Exception:
            _SUPPORTS_COLOUR = False


class _ColourFormatter(logging.Formatter):
    """Formatter that adds ANSI colour codes based on log level."""

    COLOURS = {
        logging.DEBUG: "\033[36m",  # Cyan
        logging.INFO: "\033[32m",  # Green
        logging.WARNING: "\033[33m",  # Yellow
        logging.ERROR: "\033[31m",  # Red
        logging.CRITICAL: "\033[1;31m",  # Bold Red
    }
    RESET = "\033[0m"

    def __init__(self, fmt: str, datefmt: str | None = None):
        super().__init__(fmt, datefmt)

    def format(self, record: logging.LogRecord) -> str:
        if _SUPPORTS_COLOUR:
            colour = self.COLOURS.get(record.levelno, self.RESET)
            record.levelname = f"{colour}{record.levelname:<8}{self.RESET}"
        else:
            record.levelname = f"{record.levelname:<8}"
        return super().format(record)


# ---------------------------------------------------------------------------
# Logging configuration dict
# ---------------------------------------------------------------------------
LOGGING_CONFIG: dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "console": {
            "()": _ColourFormatter,
            "fmt": "%(asctime)s | %(levelname)s | %(name)-25s | %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        },
        "file": {
            "format": "%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "console",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "file",
            "filename": LOG_FILE,
            "maxBytes": 5_242_880,  # 5 MB
            "backupCount": 3,
            "encoding": "utf-8",
        },
    },
    "root": {
        "level": "DEBUG",
        "handlers": ["console", "file"],
    },
}

# ---------------------------------------------------------------------------
# Apply configuration on import
# ---------------------------------------------------------------------------
logging.config.dictConfig(LOGGING_CONFIG)


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured with the project-wide settings.

    Args:
        name: Typically ``__name__`` of the calling module.

    Returns:
        A :class:`logging.Logger` instance.
    """
    return logging.getLogger(name)
