"""Outbox dispatcher: sends outbound calls written by the processor, safely retried.

Life of a message:

    pending --claim--> in_flight --2xx--------------------------> sent
                           |------retryable, budget left-------> pending (next_attempt_at = now + backoff)
                           |------retryable, budget exhausted--> dead + DLQ retries_exhausted
                           |------non-retryable 4xx------------> dead + DLQ <provider error code>
    in_flight with an expired lease (worker died) -------------> pending

Rules that make retries safe:

* **Stable `Idempotency-Key`** per business action (`loyalty-accrual:<order>`), the
  same on every retry and replay. A timeout is ambiguous — the provider may have
  done the work — so the retry must be recognisable as the same request.
* **Deadline per attempt** with `asyncio.timeout` (ASGITransport ignores httpx
  timeouts; real sockets get the httpx timeout as well).
* **Exponential backoff with full jitter**: `uniform(0, min(cap, base·2^(n-1)))`,
  so a provider recovering from an outage is not hit by synchronised retries.
* **No DB transaction is open during the HTTP call.** The claim commits first; the
  result is recorded in a second transaction.

Retryable: timeouts, connection errors, 408, 409 (key in use), 425, 429, 5xx.
Everything else in 4xx is permanent for this request and goes straight to the
DLQ with the provider's error code (e.g. `member_not_found`): retrying a 404
five times only delays the human who has to fix the data.
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from irlab.clock import Clock
from irlab.config import Settings
from irlab.db import session_factory
from irlab.deadletters import record_dead_letter, resolve_dead_letter
from irlab.logs import log_context
from irlab.models import LoyaltyStatus, Order, OrderStatus, OutboxMessage, OutboxStatus
from irlab.providers import ProviderClients, ProviderNotConfigured

logger = logging.getLogger("irlab.outbox")

RETRYABLE_STATUS = frozenset({408, 409, 425, 429})


def backoff_delay(attempt: int, base: float, cap: float, rng: random.Random) -> float:
    """Full-jitter exponential backoff for the retry after `attempt` (1-based) failed."""
    ceiling = min(cap, base * 2 ** max(attempt - 1, 0))
    return rng.uniform(0, ceiling)


def is_retryable_status(status: int) -> bool:
    return status in RETRYABLE_STATUS or status >= 500


@dataclass
class AttemptResult:
    kind: str  # success | retry | dead | skipped
    http_status: int | None = None
    error: str | None = None
    reason_code: str | None = None
    response: dict[str, Any] | None = None
    duration_ms: float = 0.0


def _error_code(resp: httpx.Response) -> str | None:
    try:
        body = resp.json()
    except ValueError:
        return None
    detail = body.get("detail") if isinstance(body, dict) else None
    if isinstance(detail, dict) and isinstance(detail.get("code"), str):
        return detail["code"]
    return None


class Dispatcher:
    def __init__(self, settings: Settings, providers: ProviderClients, clock: Clock, rng: random.Random | None = None):
        self.settings = settings
        self.providers = providers
        self.clock = clock
        self.rng = rng or random.Random()

    async def run_once(self, limit: int | None = None) -> int:
        """Send every due message once. Returns how many were attempted."""
        await self.recover_expired_claims()
        now = self.clock.now()
        async with session_factory()() as s:
            ids = (
                await s.scalars(
                    select(OutboxMessage.id)
                    .where(OutboxMessage.status == OutboxStatus.PENDING, OutboxMessage.next_attempt_at <= now)
                    .order_by(OutboxMessage.next_attempt_at, OutboxMessage.id)
                    .limit(limit or self.settings.worker_batch_size)
                )
            ).all()
        attempted = 0
        for msg_id in ids:
            if await self.dispatch_one(msg_id) is not None:
                attempted += 1
        return attempted

    async def recover_expired_claims(self) -> int:
        now = self.clock.now()
        async with session_factory()() as s:
            res = await s.execute(
                update(OutboxMessage)
                .where(OutboxMessage.status == OutboxStatus.IN_FLIGHT, OutboxMessage.claimed_until < now)
                .values(status=OutboxStatus.PENDING, claimed_until=None, next_attempt_at=now)
            )
            await s.commit()
        if res.rowcount:
            logger.warning("recovered outbox messages from expired claims", extra={"count": res.rowcount})
        return res.rowcount or 0

    async def next_due_in(self) -> float | None:
        """Seconds until the next pending message is due (None if none)."""
        async with session_factory()() as s:
            nxt = await s.scalar(
                select(OutboxMessage.next_attempt_at)
                .where(OutboxMessage.status == OutboxStatus.PENDING)
                .order_by(OutboxMessage.next_attempt_at)
                .limit(1)
            )
        if nxt is None:
            return None
        return max((nxt - self.clock.now()).total_seconds(), 0.0)

    async def dispatch_one(self, msg_id: int) -> str | None:
        """Claim, send and record one message. Returns its new status, or None if not claimable."""
        now = self.clock.now()
        async with session_factory()() as s:
            res = await s.execute(
                update(OutboxMessage)
                .where(
                    OutboxMessage.id == msg_id,
                    OutboxMessage.status == OutboxStatus.PENDING,
                    OutboxMessage.next_attempt_at <= now,
                )
                .values(
                    status=OutboxStatus.IN_FLIGHT,
                    attempts=OutboxMessage.attempts + 1,
                    claimed_until=now + timedelta(seconds=self.settings.claim_lease_s),
                    updated_at=now,
                )
            )
            await s.commit()
            if res.rowcount != 1:
                return None
            msg = await s.get(OutboxMessage, msg_id)
            assert msg is not None
            snapshot = {
                "kind": msg.kind,
                "target": msg.target,
                "method": msg.method,
                "path": msg.path,
                "payload": msg.payload,
                "key": msg.idempotency_key,
                "order_id": msg.order_id,
                "correlation_id": msg.correlation_id,
                "attempt": msg.attempts,
            }
            order_status = None
            if msg.order_id:
                order_status = await s.scalar(select(Order.status).where(Order.id == msg.order_id))

        with log_context(correlation_id=snapshot["correlation_id"], order_id=snapshot["order_id"], outbox_id=msg_id):
            if snapshot["kind"] == "loyalty.accrue" and order_status == OrderStatus.CANCELLED:
                result = AttemptResult("skipped", error="order cancelled before the accrual was sent")
            else:
                result = await self._send(snapshot)
            status = await self._record(msg_id, result)
            log = logger.info if result.kind in ("success", "skipped") else logger.warning
            log(
                "outbox attempt %s",
                result.kind,
                extra={
                    "kind": snapshot["kind"],
                    "attempt": snapshot["attempt"],
                    "http_status": result.http_status,
                    "error": result.error,
                    "duration_ms": round(result.duration_ms, 2),
                    "new_status": status,
                },
            )
            return status

    async def _send(self, m: dict[str, Any]) -> AttemptResult:
        started = time.perf_counter()

        def elapsed() -> float:
            return (time.perf_counter() - started) * 1000

        try:
            client = self.providers.get(m["target"])
        except ProviderNotConfigured as exc:
            return AttemptResult("retry", error=str(exc), reason_code="provider_not_configured")
        headers = {"Idempotency-Key": m["key"]}
        if m["correlation_id"]:
            headers["X-Correlation-ID"] = m["correlation_id"]
        try:
            async with asyncio.timeout(self.settings.outbound_timeout_s):
                resp = await client.request(m["method"], m["path"], json=m["payload"], headers=headers)
        except TimeoutError:
            return AttemptResult(
                "retry", error=f"timeout after {self.settings.outbound_timeout_s:g}s", reason_code="timeout",
                duration_ms=elapsed(),
            )
        except httpx.HTTPError as exc:
            return AttemptResult("retry", error=type(exc).__name__, reason_code="connection_error", duration_ms=elapsed())

        code = _error_code(resp)
        if 200 <= resp.status_code < 300:
            try:
                body = resp.json()
            except ValueError:
                body = None
            return AttemptResult("success", http_status=resp.status_code,
                                 response=body if isinstance(body, dict) else None, duration_ms=elapsed())
        if is_retryable_status(resp.status_code):
            return AttemptResult("retry", http_status=resp.status_code, error=code or f"http_{resp.status_code}",
                                 reason_code=code or f"http_{resp.status_code}", duration_ms=elapsed())
        return AttemptResult("dead", http_status=resp.status_code, error=code or f"http_{resp.status_code}",
                             reason_code=code or f"http_{resp.status_code}", duration_ms=elapsed())

    async def _record(self, msg_id: int, result: AttemptResult) -> str:
        now = self.clock.now()
        async with session_factory()() as s, s.begin():
            msg = await s.get(OutboxMessage, msg_id, with_for_update=True)
            assert msg is not None
            if msg.status != OutboxStatus.IN_FLIGHT:
                # Our lease expired and another worker took over; its result wins.
                logger.warning("outbox result discarded: claim lost", extra={"status": msg.status})
                return msg.status
            msg.claimed_until = None
            msg.updated_at = now
            msg.last_http_status = result.http_status
            order = await s.get(Order, msg.order_id, with_for_update=True) if msg.order_id else None

            if result.kind == "success":
                msg.status = OutboxStatus.SENT
                msg.sent_at = now
                msg.response = result.response
                msg.last_error = None
                self._effect_success(msg, order, result)
                await resolve_dead_letter(s, origin="outbox", origin_id=msg.id, resolution="replayed", now=now)
            elif result.kind == "skipped":
                msg.status = OutboxStatus.SKIPPED
                msg.last_error = result.error
                if order is not None and msg.kind == "loyalty.accrue":
                    order.loyalty_status = LoyaltyStatus.SKIPPED
                await resolve_dead_letter(s, origin="outbox", origin_id=msg.id, resolution="skipped", now=now)
            elif result.kind == "retry" and msg.attempts < self.settings.retry_max_attempts:
                delay = backoff_delay(msg.attempts, self.settings.retry_base_s, self.settings.retry_cap_s, self.rng)
                msg.status = OutboxStatus.PENDING
                msg.next_attempt_at = now + timedelta(seconds=delay)
                msg.last_error = result.error
            else:
                reason = "retries_exhausted" if result.kind == "retry" else (result.reason_code or "rejected")
                msg.status = OutboxStatus.DEAD
                msg.last_error = result.error
                self._effect_dead(msg, order, reason)
                await record_dead_letter(
                    s,
                    origin="outbox",
                    origin_id=msg.id,
                    reason_code=reason,
                    last_error=result.error,
                    attempts=msg.attempts,
                    now=now,
                    order_id=msg.order_id,
                    source=msg.target,
                    correlation_id=msg.correlation_id,
                    details={
                        "kind": msg.kind,
                        "http_status": result.http_status,
                        "last_attempt_error": result.reason_code,
                        "idempotency_key": msg.idempotency_key,
                        "request": {"method": msg.method, "path": msg.path, "payload": msg.payload},
                    },
                )
            return msg.status

    @staticmethod
    def _effect_success(msg: OutboxMessage, order: Order | None, result: AttemptResult) -> None:
        if order is None or msg.kind != "loyalty.accrue":
            return
        body = result.response or {}
        order.loyalty_status = LoyaltyStatus.ACCRUED
        if isinstance(body.get("points"), int):
            order.points_awarded = body["points"]
        if isinstance(body.get("id"), str) and not order.loyalty_accrual_id:
            order.loyalty_accrual_id = body["id"]

    @staticmethod
    def _effect_dead(msg: OutboxMessage, order: Order | None, reason: str) -> None:
        if order is None or msg.kind != "loyalty.accrue":
            return
        order.loyalty_status = LoyaltyStatus.MEMBER_MISSING if reason == "member_not_found" else LoyaltyStatus.FAILED


async def requeue_outbox(s: AsyncSession, msg: OutboxMessage, now: datetime) -> None:
    """Reset a dead message for a replay: fresh retry budget, same idempotency key."""
    msg.status = OutboxStatus.PENDING
    msg.attempts = 0
    msg.next_attempt_at = now
    msg.claimed_until = None
    msg.last_error = None
    msg.updated_at = now
    if msg.order_id and msg.kind == "loyalty.accrue":
        order = await s.get(Order, msg.order_id, with_for_update=True)
        if order is not None:
            order.loyalty_status = LoyaltyStatus.PENDING
