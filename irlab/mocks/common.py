"""Building blocks shared by the three mock providers.

Each mock behaves like an external SaaS the hub does not control:

* its own in-memory state (no shared database with the hub),
* provider-side `Idempotency-Key` handling on POSTs,
* signed webhooks with a per-resource `sequence`,
* a mock-only control plane under `/_admin` for failure injection.

The `/_admin` routes exist only in the mocks; never expose them outside a lab.
"""

import asyncio
import hashlib
import json
import logging
import os
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from irlab.signing import sign_headers

logger = logging.getLogger("irlab.mocks")


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def error(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail={"code": code, "message": message})


# ------------------------------------------------------------------ config


@dataclass
class MockConfig:
    name: str  # "pos" | "payments" | "loyalty"
    webhook_url: str | None = None
    webhook_secret: str | None = None
    # Lets the hub deliver in-process (ASGITransport) when the mocks are mounted.
    client_factory: Callable[[], httpx.AsyncClient] | None = None
    webhook_timeout_s: float = 5.0

    @classmethod
    def from_env(cls, name: str) -> "MockConfig":
        """Standalone containers: MOCK_POS_WEBHOOK_URL / MOCK_POS_WEBHOOK_SECRET, etc.

        Without MOCK_<NAME>_WEBHOOK_SECRET the mock signs with the first entry of the
        hub's <NAME>_WEBHOOK_SECRETS, so one .env file serves every container.
        """
        prefix = f"MOCK_{name.upper()}_"
        secret = os.environ.get(prefix + "WEBHOOK_SECRET") or None
        if secret is None:
            hub_list = os.environ.get(f"{name.upper()}_WEBHOOK_SECRETS", "")
            secret = next((s.strip() for s in hub_list.split(",") if s.strip()), None)
        return cls(name=name, webhook_url=os.environ.get(prefix + "WEBHOOK_URL") or None, webhook_secret=secret)


class FaultConfig(BaseModel):
    """Failure injection for the mock's business API (not for /_admin)."""

    fail_next: int = Field(0, ge=0, le=1000, description="The next N API calls fail with fail_status")
    fail_status: int = Field(503, ge=400, le=599)
    latency_ms: int = Field(0, ge=0, le=60_000, description="Added before every API call (slow provider)")
    slow_next: int = Field(
        0, ge=0, le=1000, description="The next N API calls are processed normally but answered slow_response_ms late"
    )
    slow_response_ms: int = Field(
        0, ge=0, le=60_000, description="Response delay for slow_next (timeout scenario: work done, reply late)"
    )
    webhook_copies: int = Field(1, ge=0, le=5, description="0 drops webhooks, 2+ delivers duplicates")


# ------------------------------------------------------------ webhooks out


@dataclass
class EmittedEvent:
    envelope: dict[str, Any]
    deliveries: list[dict[str, Any]] = field(default_factory=list)


class WebhookEmitter:
    """Provider-side webhook sender. Delivers inline; records every attempt."""

    def __init__(self, config: MockConfig, faults: FaultConfig):
        self.config = config
        self.faults = faults
        self.events: OrderedDict[str, EmittedEvent] = OrderedDict()
        self._client: httpx.AsyncClient | None = None

    @property
    def configured(self) -> bool:
        return bool(self.config.webhook_url and self.config.webhook_secret)

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            factory = self.config.client_factory
            self._client = factory() if factory else httpx.AsyncClient(timeout=self.config.webhook_timeout_s)
        return self._client

    async def emit(
        self, event_type: str, resource_type: str, resource_id: str, sequence: int, data: dict[str, Any]
    ) -> dict[str, Any]:
        envelope = {
            "id": f"evt_{uuid.uuid4().hex[:24]}",
            "type": event_type,
            "occurred_at": now_iso(),
            "resource": {"type": resource_type, "id": resource_id},
            "sequence": sequence,
            "data": data,
        }
        self.events[envelope["id"]] = EmittedEvent(envelope)
        for _ in range(self.faults.webhook_copies):
            await self.deliver(envelope["id"])
        return envelope

    async def deliver(self, event_id: str) -> dict[str, Any]:
        """One delivery attempt: same event id, fresh timestamp and signature (like a provider retry)."""
        event = self.events[event_id]
        attempt: dict[str, Any] = {"at": now_iso()}
        if not self.configured:
            attempt |= {"status": None, "error": "webhook_not_configured"}
        else:
            body = json.dumps(event.envelope, separators=(",", ":")).encode()
            headers = sign_headers(self.config.webhook_secret, event_id, body) | {"content-type": "application/json"}
            try:
                resp = await self._http().post(self.config.webhook_url, content=body, headers=headers)
                attempt |= {"status": resp.status_code, "response": _safe_json(resp)}
            except httpx.HTTPError as exc:
                attempt |= {"status": None, "error": type(exc).__name__}
        event.deliveries.append(attempt)
        logger.info(
            "mock webhook delivery",
            extra={"provider": self.config.name, "event_id": event_id, "http_status": attempt.get("status")},
        )
        return attempt

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


def _safe_json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return resp.text[:200]


# ------------------------------------------------- provider idempotency


class ProviderIdempotency:
    """`Idempotency-Key` semantics as most payment APIs implement them.

    * same key + same request → original response replayed (`Idempotent-Replayed: true`)
    * same key + different request → 422, never silently a different operation
    * same key while the first request is still running → 409
    * only 2xx responses are cached: a 404 (e.g. member not found) is not, so a
      retry with the same key succeeds once the data is fixed.
    """

    def __init__(self) -> None:
        self._done: dict[str, tuple[str, int, Any]] = {}
        self._inflight: set[str] = set()

    async def run(
        self,
        request: Request,
        key: str | None,
        body: dict[str, Any],
        handler: Callable[[], Awaitable[tuple[int, Any]]],
        *,
        required: bool,
    ) -> JSONResponse:
        if key is None:
            if required:
                raise error(400, "idempotency_key_required", "This endpoint requires an Idempotency-Key header")
            status, payload = await handler()
            return JSONResponse(payload, status_code=status)
        if not 1 <= len(key) <= 255:
            raise error(400, "invalid_idempotency_key", "Idempotency-Key must be 1-255 characters")

        fingerprint = hashlib.sha256(
            json.dumps({"path": request.url.path, "body": body}, sort_keys=True).encode()
        ).hexdigest()
        if key in self._done:
            original_fp, status, payload = self._done[key]
            if original_fp != fingerprint:
                raise error(422, "idempotency_key_reused", "Idempotency-Key was already used with a different request")
            return JSONResponse(payload, status_code=status, headers={"Idempotent-Replayed": "true"})
        if key in self._inflight:
            raise error(409, "idempotency_key_in_use", "A request with this Idempotency-Key is still in progress")

        self._inflight.add(key)
        try:
            status, payload = await handler()
        finally:
            self._inflight.discard(key)
        if 200 <= status < 300:
            self._done[key] = (fingerprint, status, payload)
        return JSONResponse(payload, status_code=status)


IdempotencyKey = Annotated[str | None, Header(alias="Idempotency-Key")]


# ------------------------------------------------------------ mock state


@dataclass
class MockState:
    config: MockConfig
    faults: FaultConfig = field(default_factory=FaultConfig)
    idempotency: ProviderIdempotency = field(default_factory=ProviderIdempotency)
    emitter: WebhookEmitter = field(init=False)

    def __post_init__(self) -> None:
        self.emitter = WebhookEmitter(self.config, self.faults)

    def set_faults(self, faults: FaultConfig) -> None:
        # Mutate in place: the emitter holds a reference to the same object.
        for name, value in faults.model_dump().items():
            setattr(self.faults, name, value)


def get_state(request: Request) -> MockState:
    return request.app.state.mock


async def inject_faults(request: Request) -> None:
    """Router-level dependency on every business endpoint of a mock."""
    faults = get_state(request).faults
    if faults.latency_ms:
        await asyncio.sleep(faults.latency_ms / 1000)
    if faults.fail_next > 0:
        faults.fail_next -= 1
        raise error(faults.fail_status, "injected_fault", "Failure injected by the mock control plane")


class SlowResponseMiddleware:
    """Timeout scenario: the provider *does the work*, then answers late.

    This is the ambiguous case a client cannot distinguish from "request lost":
    the request was processed (and its idempotent response cached), but the
    reply arrives after the caller's deadline. Only a retry with the same
    Idempotency-Key is safe here; a new key double-applies the operation.
    """

    def __init__(self, app, state: "MockState"):
        self.app = app
        self.state = state

    async def __call__(self, scope, receive, send):
        faults = self.state.faults
        path = scope.get("path", "")
        if (
            scope["type"] != "http"
            or "/_admin" in path
            or path.endswith("/health")
            or faults.slow_next <= 0
            or faults.slow_response_ms <= 0
        ):
            await self.app(scope, receive, send)
            return
        faults.slow_next -= 1
        held: list = []

        async def hold(message) -> None:
            held.append(message)

        await self.app(scope, receive, hold)
        await asyncio.sleep(faults.slow_response_ms / 1000)
        for message in held:
            await send(message)


def created_after_filter(items: list[dict[str, Any]], created_after: datetime | None) -> list[dict[str, Any]]:
    if created_after is None:
        return items
    if created_after.tzinfo is None:
        created_after = created_after.replace(tzinfo=UTC)
    return [i for i in items if datetime.fromisoformat(i["created_at"]) >= created_after]


def business_router() -> APIRouter:
    return APIRouter(dependencies=[Depends(inject_faults)])


def admin_router() -> APIRouter:
    router = APIRouter(prefix="/_admin", tags=["mock control plane"])

    @router.get("/faults", response_model=FaultConfig)
    async def get_faults(request: Request):
        return get_state(request).faults

    @router.put("/faults", response_model=FaultConfig)
    async def put_faults(faults: FaultConfig, request: Request):
        get_state(request).set_faults(faults)
        return get_state(request).faults

    @router.delete("/faults", response_model=FaultConfig)
    async def reset_faults(request: Request):
        get_state(request).set_faults(FaultConfig())
        return get_state(request).faults

    @router.get("/webhooks", summary="Emitted events and every delivery attempt")
    async def list_webhooks(request: Request):
        return [
            {"event": e.envelope, "deliveries": e.deliveries} for e in reversed(get_state(request).emitter.events.values())
        ]

    @router.post("/webhooks/{event_id}/deliver", summary="Redeliver an event now (provider retry / duplicate)")
    async def redeliver(event_id: str, request: Request):
        emitter = get_state(request).emitter
        if event_id not in emitter.events:
            raise error(404, "event_not_found", f"No emitted event {event_id}")
        return await emitter.deliver(event_id)

    return router


def build_mock_app(state: MockState, title: str, description: str, routers: list[APIRouter]) -> FastAPI:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Runs when the mock is served standalone; the hub closes mounted mocks itself.
        try:
            yield
        finally:
            await state.emitter.aclose()

    app = FastAPI(title=title, description=description, version="mock", lifespan=lifespan)
    app.state.mock = state
    app.add_middleware(SlowResponseMiddleware, state=state)
    for router in routers:
        app.include_router(router)
    app.include_router(admin_router())

    @app.get("/health", tags=["health"])
    async def health():
        return {"status": "ok", "provider": state.config.name, "webhooks_configured": state.emitter.configured}

    return app
