"""Independent read-only Destination web process."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from airgap_sync.common.models import AppConfig, Role
from airgap_sync.monitor.dashboard import snapshot


def create_app(config: AppConfig) -> FastAPI:
    if config.role is not Role.DESTINATION or config.destination is None:
        raise ValueError("monitor-web requires destination config")
    root = Path(__file__).resolve().parent
    app = FastAPI(title="Airgap Sync Destination Monitor", docs_url=None, redoc_url=None)
    app.mount("/static", StaticFiles(directory=root / "static"), name="static")
    templates = Jinja2Templates(directory=root / "templates")

    def render(request: Request, page: str) -> HTMLResponse:
        return templates.TemplateResponse(
            request, f"{page}.html", {"data": snapshot(config), "page": page}
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
        return snapshot(config)

    return app
