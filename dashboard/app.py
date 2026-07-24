"""
FastAPI application factory and entry point for the dashboard server.

Usage::

    # From the project root:
    python -m dashboard.app

    # Or with uvicorn directly:
    uvicorn dashboard.app:app --host 127.0.0.1 --port 8000 --reload
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from dashboard.api import router, set_state
from dashboard.state import SimulatorState

TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def create_app(state: SimulatorState | None = None) -> FastAPI:
    """Build and return a configured FastAPI application.

    Args:
        state: Optional pre-built simulator state (useful for testing).
            When ``None``, a default state is created.
    """
    if state is None:
        state = SimulatorState()

    set_state(state)

    application = FastAPI(
        title="Canary Deployment Simulator — Dashboard",
        description="Operational dashboard for progressive rollout governance",
        version="1.0.0",
    )
    application.include_router(router, prefix="/api")

    @application.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request) -> HTMLResponse:
        """Serve the operational dashboard UI."""
        return templates.TemplateResponse(request=request, name="index.html")

    return application


app = create_app()

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("DASHBOARD_PORT", "8000"))
    uvicorn.run(
        "dashboard.app:app",
        host="127.0.0.1",
        port=port,
        reload=False,
        log_level="info",
    )
