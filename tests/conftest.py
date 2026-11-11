import json
import os
import secrets
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import fakeredis
import httpx
import pytest
from fastapi import FastAPI

from irlab.config import Settings
from irlab.idempotency import IdempotencyStore, InMemoryIdempotencyStore, RedisIdempotencyStore
from irlab.main import create_app
from irlab.signing import sign_headers

# Optional: run the suite against real services (Docker Compose exposes them on 127.0.0.1):
#   TEST_DATABASE_URL=postgresql+asyncpg://irlab:<password>@127.0.0.1:8154/irlab_test
#   TEST_REDIS_URL=redis://127.0.0.1:8163/15
# Unset → temp SQLite per test and fakeredis. Each test starts from an empty schema / Redis DB.
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "")


def redis_client():
    if TEST_REDIS_URL:
        from redis.asyncio import Redis

        return Redis.from_url(TEST_REDIS_URL)
    return fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer())


@pytest.fixture(autouse=True)
async def _clean_external_services():
    """Real Postgres/Redis are shared between tests: wipe them before each one."""
    if TEST_DATABASE_URL:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine

        from irlab import models  # noqa: F401
        from irlab.db import Base

        engine = create_async_engine(TEST_DATABASE_URL)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await engine.dispose()
    if TEST_REDIS_URL:
        client = redis_client()
        await client.flushdb()
        await client.aclose()
    yield


def make_settings(tmp_path, **overrides: Any) -> Settings:
    """Isolated settings: no .env file, fresh random secrets, temp SQLite."""
    values: dict[str, Any] = {
        "_env_file": None,
        "database_url": TEST_DATABASE_URL or f"sqlite+aiosqlite:///{tmp_path / 'hub.db'}",
        "redis_url": "",
        "pos_webhook_secrets": secrets.token_urlsafe(32),
        "payments_webhook_secrets": secrets.token_urlsafe(32),
        "loyalty_webhook_secrets": secrets.token_urlsafe(32),
        "webhook_tolerance_s": 300,
        "webhook_max_body_bytes": 256 * 1024,
        "mount_mocks": False,
        # Tests drive the worker explicitly (runtime.worker.tick / processor.run_once).
        "run_worker": False,
        "ops_api_key": secrets.token_urlsafe(24),
        "log_format": "kv",
        "reconcile_grace_s": 0,
    }
    values.update(overrides)
    return Settings(**values)


def make_store(kind: str) -> IdempotencyStore:
    if kind == "memory":
        return InMemoryIdempotencyStore()
    return RedisIdempotencyStore(redis_client())


@asynccontextmanager
async def running_hub(
    settings: Settings, store: IdempotencyStore | None = None, clock=None, rng=None
) -> AsyncIterator[FastAPI]:
    app = create_app(settings, idempotency_store=store, clock=clock, rng=rng)
    async with app.router.lifespan_context(app):
        yield app


def hub_client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://hub")


@pytest.fixture
def settings(tmp_path) -> Settings:
    return make_settings(tmp_path)


@pytest.fixture(params=["memory", "redis"])
def store_kind(request) -> str:
    """Hub tests run against both idempotency backends (Redis via fakeredis)."""
    return request.param


@pytest.fixture
async def hub_app(settings, store_kind) -> AsyncIterator[FastAPI]:
    async with running_hub(settings, make_store(store_kind)) as app:
        yield app


@pytest.fixture
async def hub(hub_app) -> AsyncIterator[httpx.AsyncClient]:
    async with hub_client(hub_app) as client:
        yield client


def make_event(
    event_type: str = "order.created",
    resource: tuple[str, str] = ("order", "POS-000001"),
    sequence: int | None = 1,
    data: dict[str, Any] | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    return {
        "id": event_id or f"evt_{uuid.uuid4().hex[:24]}",
        "type": event_type,
        "occurred_at": datetime.now(UTC).isoformat(),
        "resource": {"type": resource[0], "id": resource[1]},
        "sequence": sequence,
        "data": data if data is not None else {"total_minor": 19000, "currency": "TRY"},
    }


class Sender:
    """Plays an external provider: signs and posts webhooks to the hub."""

    def __init__(self, client: httpx.AsyncClient, settings: Settings):
        self.client = client
        self.settings = settings

    def secret(self, source: str) -> str:
        return self.settings.webhook_secrets(source)[0]

    async def send(
        self,
        source: str,
        event: dict[str, Any],
        *,
        secret: str | list[str] | None = None,
        timestamp: int | None = None,
        body: bytes | None = None,
        signed_body: bytes | None = None,
        headers: dict[str, str] | None = None,
        msg_id: str | None = None,
    ) -> httpx.Response:
        raw = body if body is not None else json.dumps(event).encode()
        signing_secret = secret if secret is not None else self.secret(source)
        h = sign_headers(
            signing_secret,
            msg_id or event["id"],
            signed_body if signed_body is not None else raw,
            timestamp=timestamp,
        )
        h["content-type"] = "application/json"
        h.update(headers or {})
        return await self.client.post(f"/webhooks/{source}", content=raw, headers=h)


@pytest.fixture
def sender(hub, settings) -> Sender:
    return Sender(hub, settings)
