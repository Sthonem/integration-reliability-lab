"""Hub entrypoint.

    uvicorn irlab.main:app --reload

With MOUNT_MOCKS=true (default) the three mock providers are mounted under
/mock/{pos,payments,loyalty} and deliver their webhooks to this same process
in-memory (ASGI), signed with the first secret configured for each source. The
outbox dispatcher and the reconciliation job reach them the same way.

With RUN_WORKER=true (default) the processor/dispatcher loop runs as a
background task in this process; Docker Compose runs it as its own container.
"""

import asyncio
import random
import re
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from irlab import __version__
from irlab.api.inbox import router as inbox_router
from irlab.api.monitoring import router as monitoring_router
from irlab.api.ops import router as ops_router
from irlab.api.webhooks import router as webhooks_router
from irlab.clock import Clock
from irlab.config import SOURCES, Settings, get_settings
from irlab.db import configure_engine, dispose_engine, init_db, session_factory
from irlab.idempotency import IdempotencyStore, build_idempotency_store
from irlab.ingest import WebhookIngestor
from irlab.logs import configure_logging, log_context
from irlab.mocks import MOCK_FACTORIES, MockConfig
from irlab.runtime import build_runtime

_CORRELATION_RE = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")


class CorrelationMiddleware:
    """Accepts X-Correlation-ID (or X-Request-ID) if well-formed, else generates one;
    adds it to every log line of the request and echoes it on the response."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {k.decode().lower(): v.decode(errors="replace") for k, v in scope.get("headers", [])}
        incoming = headers.get("x-correlation-id") or headers.get("x-request-id") or ""
        cid = incoming if _CORRELATION_RE.match(incoming) else f"req_{uuid.uuid4().hex[:16]}"

        async def send_with_id(message):
            if message["type"] == "http.response.start":
                message.setdefault("headers", [])
                message["headers"] = list(message["headers"]) + [(b"x-correlation-id", cid.encode())]
            await send(message)

        with log_context(correlation_id=cid):
            await self.app(scope, receive, send_with_id)


def _mount_mocks(app: FastAPI, settings: Settings) -> list:
    def hub_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://hub")

    states = []
    for source, factory in MOCK_FACTORIES.items():
        secrets = settings.webhook_secrets(source)
        mock_app = factory(
            MockConfig(
                name=source,
                webhook_url=f"http://hub/webhooks/{source}",
                webhook_secret=secrets[0] if secrets else None,
                client_factory=hub_client,
            )
        )
        app.mount(f"/mock/{source}", mock_app)
        states.append(mock_app.state.mock)
    return states


def create_app(
    settings: Settings | None = None,
    *,
    idempotency_store: IdempotencyStore | None = None,
    clock: Clock | None = None,
    rng: random.Random | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_format)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_engine(settings.database_url)
        if settings.db_auto_create:
            await init_db()
        store = idempotency_store or build_idempotency_store(settings.redis_url)
        app.state.ingestor = WebhookIngestor(settings, store)
        runtime = build_runtime(settings, clock=clock, asgi_app_factory=lambda: app, rng=rng)
        app.state.runtime = runtime
        stop = asyncio.Event()
        task = asyncio.create_task(runtime.worker.run_forever(stop)) if settings.run_worker else None
        try:
            yield
        finally:
            stop.set()
            if task is not None:
                try:
                    await asyncio.wait_for(task, timeout=10)
                except TimeoutError:
                    task.cancel()
            for mock_state in app.state.mocks:
                await mock_state.emitter.aclose()
            await runtime.aclose()
            await store.aclose()
            await dispose_engine()

    app = FastAPI(
        title="Integration Reliability Lab — ordering hub",
        version=__version__,
        description=(
            "Ordering hub that integrates mock POS, payment and loyalty systems. "
            "Synthetic data and mock services only."
        ),
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.add_middleware(CorrelationMiddleware)
    app.include_router(webhooks_router)
    app.include_router(inbox_router)
    app.include_router(ops_router)
    app.include_router(monitoring_router)
    app.state.mocks = _mount_mocks(app, settings) if settings.mount_mocks else []

    @app.get("/health", tags=["health"])
    async def health(request: Request):
        try:
            async with session_factory()() as session:
                await session.execute(text("SELECT 1"))
            db_ok = True
        except Exception:
            db_ok = False
        store: IdempotencyStore = request.app.state.ingestor.store
        store_ok = await store.ping()
        body = {
            # Redis down is degraded, not down: the DB constraint still deduplicates.
            "status": "ok" if db_ok and store_ok else ("degraded" if db_ok else "down"),
            "version": __version__,
            "database": db_ok,
            "idempotency_store": {"backend": store.backend, "ok": store_ok},
            "webhook_sources_configured": {s: bool(settings.webhook_secrets(s)) for s in SOURCES},
            "providers_configured": {s: request.app.state.runtime.providers.configured(s) for s in SOURCES},
            "ops_key_configured": bool(settings.ops_api_key.get_secret_value()),
            "worker_in_process": settings.run_worker,
            "mocks_mounted": settings.mount_mocks,
            # Non-secret tuning the scenario runner needs to know (timeouts it must wait out).
            "worker_config": {
                "park_timeout_s": settings.park_timeout_s,
                "outbound_timeout_s": settings.outbound_timeout_s,
                "retry_base_s": settings.retry_base_s,
                "retry_cap_s": settings.retry_cap_s,
                "retry_max_attempts": settings.retry_max_attempts,
                "reconcile_grace_s": settings.reconcile_grace_s,
            },
            "triage_provider": settings.triage_provider,
        }
        return JSONResponse(body, status_code=200 if db_ok else 503)

    return app


app = create_app()
