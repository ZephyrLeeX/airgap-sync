"""Independent read-only Destination web process."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from airgap_sync.common.models import AppConfig, Role
from airgap_sync.monitor.dashboard import snapshot
from airgap_sync.monitor.ingest import BackgroundIngest
from airgap_sync.monitor.store import read_sources


def create_app(config: AppConfig) -> FastAPI:
    if config.role is not Role.DESTINATION or config.destination is None:
        raise ValueError("monitor-web requires destination config")
    root = Path(__file__).resolve().parent
    background = BackgroundIngest(config.monitor_ingest) if config.monitor_ingest else None

    @asynccontextmanager
    async def lifespan(app):
        if background:
            background.start()
        try:
            yield
        finally:
            if background:
                await asyncio.to_thread(background.stop)

    app = FastAPI(
        title="Airgap Sync Destination Monitor", docs_url=None, redoc_url=None, lifespan=lifespan
    )

    def data_snapshot():
        data = snapshot(config)
        data["sources"] = read_sources(config.monitor_ingest)
        data["ingest_status"] = background.status if background else "DISABLED"
        return data

    app.mount("/static", StaticFiles(directory=root / "static"), name="static")
    templates = Jinja2Templates(directory=root / "templates")
    zone = ZoneInfo(config.destination.report_timezone)

    def display_time(value: str | None) -> str:
        if value is None:
            return "Unknown"
        observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=UTC)
        return observed.astimezone(zone).strftime("%Y-%m-%d %H:%M:%S %Z")

    templates.env.filters["display_time"] = display_time

    def render(request: Request, page: str) -> HTMLResponse:
        return templates.TemplateResponse(
            request, f"{page}.html", {"data": data_snapshot(), "page": page}
        )

    @app.get("/", response_class=HTMLResponse)
    def overview(request: Request) -> HTMLResponse:
        return render(request, "overview")

    @app.get("/tables", response_class=HTMLResponse)
    def tables(request: Request) -> HTMLResponse:
        return render(request, "tables")

    @app.get("/runs", response_class=HTMLResponse)
    def runs(request: Request) -> HTMLResponse:
        return render(request, "runs")

    @app.get("/system", response_class=HTMLResponse)
    def system(request: Request) -> HTMLResponse:
        return render(request, "system")

    @app.get("/problems", response_class=HTMLResponse)
    def problems(request: Request) -> HTMLResponse:
        return render(request, "problems")

    @app.get("/api/dashboard")
    def dashboard() -> dict:
        return data_snapshot()

    @app.get("/api/sources")
    def sources(
        limit: int = Query(100, ge=1, le=200), before: str | None = Query(None, max_length=64)
    ):
        return read_sources(config.monitor_ingest, limit=limit, before=before)

    @app.get("/api/sources/{node_id}/history")
    def history(
        node_id: str,
        window: Literal["24h", "7d", "30d"] = "24h",
        limit: int = Query(100, ge=1, le=200),
        before: str | None = Query(None, max_length=100),
    ):
        if len(node_id) > 64:
            return {"status": "UNAVAILABLE", "samples": [], "next_cursor": None}
        return read_sources(
            config.monitor_ingest, node=node_id, window=window, limit=limit, before=before
        )

    @app.get("/source-history", response_class=HTMLResponse)
    def source_history(
        request: Request,
        node: str = Query(..., max_length=64),
        window: Literal["24h", "7d", "30d"] = "24h",
        before: str | None = Query(None, max_length=100),
    ):
        return templates.TemplateResponse(
            request,
            "source-history.html",
            {
                "data": {
                    "sources": read_sources(
                        config.monitor_ingest, node=node, window=window, before=before
                    ),
                    "timezone": str(zone),
                    "status": "OBSERVATIONS",
                    "generated_at": datetime.now(UTC).isoformat(),
                },
                "node": node,
                "window": window,
                "page": "system",
            },
        )

    return app
