"""Webhook ingestion: authenticate, validate, deduplicate, store.

Order of checks matters:

1. known + configured source        (404 / 503 fail-closed, before reading secrets)
2. size limit                       (413, before any HMAC work)
3. signature + timestamp on raw body (400 / 401, before JSON parsing)
4. JSON + envelope schema            (400)
5. header id == envelope id          (400; the id we dedupe on must be the signed one)
6. idempotency claim                 (fast path: done → 200 duplicate, held → 409 retry)
7. insert into inbox                 (unique constraint is the real guarantee)

Status code contract with senders: 2xx means "stop retrying" (accepted *or*
duplicate), 409/5xx mean "retry later", 4xx other than 409 mean "this request
will never succeed".
"""

import hashlib
import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from irlab.config import SOURCES, Settings
from irlab.db import session_factory, utcnow
from irlab.idempotency import ClaimState, IdempotencyStore
from irlab.models import InboxEvent, InboxStatus, ReceiptOutcome, WebhookReceipt
from irlab.schemas import EventEnvelope
from irlab.signing import SignatureError, verify

logger = logging.getLogger("irlab.webhooks")

# Event types the hub consumes. Anything else from a verified source is stored as
# "ignored" and acknowledged, so the provider does not retry it forever.
KNOWN_EVENT_TYPES: dict[str, frozenset[str]] = {
    "pos": frozenset({"order.created", "order.updated", "order.cancelled"}),
    "payments": frozenset({"payment.authorized", "payment.captured", "payment.failed", "payment.refunded"}),
    "loyalty": frozenset({"points.accrued", "points.reversed", "member.updated"}),
}

RETRY_AFTER_S = 5


@dataclass
class IngestResult:
    http_status: int
    outcome: ReceiptOutcome
    reason: str | None = None
    event_id: str | None = None
    inbox_id: int | None = None
    inbox_status: str | None = None
    payload_matches: bool | None = None
    headers: dict[str, str] = field(default_factory=dict)

    def body(self) -> dict[str, Any]:
        out: dict[str, Any] = {"status": self.outcome.value}
        for key in ("reason", "event_id", "inbox_id", "inbox_status", "payload_matches"):
            value = getattr(self, key)
            if value is not None:
                out[key] = value
        return out


def canonical_sha256(payload: Any) -> str:
    """Hash of the parsed payload, independent of key order and whitespace."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


class WebhookIngestor:
    def __init__(self, settings: Settings, store: IdempotencyStore):
        self.settings = settings
        self.store = store

    @property
    def max_body_bytes(self) -> int:
        return self.settings.webhook_max_body_bytes

    @staticmethod
    def idempotency_key(source: str, event_id: str) -> str:
        return f"webhook:{source}:{event_id}"

    async def ingest(self, source: str, headers: Mapping[str, str], body: bytes) -> IngestResult:
        started = time.perf_counter()
        result = await self._ingest(source, headers, body)
        duration_ms = (time.perf_counter() - started) * 1000
        if source in SOURCES:
            await self._record_receipt(source, body, result, duration_ms)
        log = logger.warning if result.outcome in (ReceiptOutcome.REJECTED, ReceiptOutcome.ERROR) else logger.info
        log(
            "webhook %s",
            result.outcome.value,
            extra={
                "source": source[:32],
                "event_id": result.event_id,
                "outcome": result.outcome.value,
                "reason": result.reason,
                "http_status": result.http_status,
                "duration_ms": round(duration_ms, 2),
            },
        )
        return result

    async def _ingest(self, source: str, headers: Mapping[str, str], body: bytes) -> IngestResult:
        def reject(status: int, reason: str, event_id: str | None = None) -> IngestResult:
            return IngestResult(status, ReceiptOutcome.REJECTED, reason=reason, event_id=event_id)

        if source not in SOURCES:
            return reject(404, "unknown_source")
        secrets = self.settings.webhook_secrets(source)
        if not secrets:
            # Fail closed: an unconfigured secret must never mean "accept unsigned".
            return IngestResult(503, ReceiptOutcome.REJECTED, reason="source_not_configured")
        if len(body) > self.max_body_bytes:
            return reject(413, "payload_too_large")

        try:
            msg_id = verify(headers, body, secrets, tolerance_s=self.settings.webhook_tolerance_s)
        except SignatureError as exc:
            return reject(exc.http_status, exc.code)

        # From here on the bytes are authenticated.
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return reject(400, "invalid_json", msg_id)
        try:
            envelope = EventEnvelope.model_validate(payload)
        except ValidationError:
            return reject(400, "invalid_envelope", msg_id)
        if envelope.id != msg_id:
            return reject(400, "event_id_mismatch", msg_id)

        digest = canonical_sha256(payload)
        inbox_status = InboxStatus.RECEIVED if envelope.type in KNOWN_EVENT_TYPES[source] else InboxStatus.IGNORED
        key = self.idempotency_key(source, envelope.id)

        claim = await self._begin(key)
        if claim is ClaimState.IN_PROGRESS:
            return IngestResult(
                409,
                ReceiptOutcome.IN_PROGRESS,
                reason="concurrent_delivery",
                event_id=envelope.id,
                headers={"Retry-After": str(RETRY_AFTER_S)},
            )

        try:
            if claim is ClaimState.DONE:
                dup = await self._record_duplicate(source, envelope.id, digest)
                if dup is not None:
                    return self._duplicate_result(envelope.id, *dup)
                logger.warning(
                    "idempotency store says done but inbox row is missing; inserting",
                    extra={"source": source, "event_id": envelope.id},
                )

            inbox_id = await self._insert(source, envelope, payload, digest, inbox_status)
            if inbox_id is None:
                # Fast path missed (expired, flushed, store down, or a parallel worker):
                # the unique constraint caught it.
                dup = await self._record_duplicate(source, envelope.id, digest)
                if dup is None:
                    raise RuntimeError("unique violation but no existing inbox row")
                await self._complete(key)
                return self._duplicate_result(envelope.id, *dup)
        except Exception:
            if claim is ClaimState.NEW:
                # Give the claim back, otherwise the sender's retry would be told
                # "in progress" until the lock TTL runs out.
                await self._release(key)
            logger.exception("webhook storage failed", extra={"source": source, "event_id": envelope.id})
            return IngestResult(
                503,
                ReceiptOutcome.ERROR,
                reason="storage_unavailable",
                event_id=envelope.id,
                headers={"Retry-After": str(RETRY_AFTER_S)},
            )

        await self._complete(key)
        return IngestResult(
            202,
            ReceiptOutcome.ACCEPTED,
            event_id=envelope.id,
            inbox_id=inbox_id,
            inbox_status=inbox_status.value,
        )

    @staticmethod
    def _duplicate_result(event_id: str, inbox_id: int, matches: bool) -> IngestResult:
        return IngestResult(
            200,
            ReceiptOutcome.DUPLICATE,
            reason=None if matches else "payload_differs_from_first_delivery",
            event_id=event_id,
            inbox_id=inbox_id,
            payload_matches=matches,
        )

    # ------------------------------------------------------------ storage

    async def _insert(
        self, source: str, env: EventEnvelope, payload: dict[str, Any], digest: str, status: InboxStatus
    ) -> int | None:
        now = utcnow()
        row = InboxEvent(
            source=source,
            event_id=env.id,
            event_type=env.type,
            resource_type=env.resource.type,
            resource_id=env.resource.id,
            sequence=env.sequence,
            occurred_at=env.occurred_at,
            received_at=now,
            last_received_at=now,
            delivery_count=1,
            conflicting_deliveries=0,
            payload=payload,
            payload_sha256=digest,
            status=status.value,
        )
        async with session_factory()() as session:
            session.add(row)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                return None
            return row.id

    async def _record_duplicate(self, source: str, event_id: str, digest: str) -> tuple[int, bool] | None:
        async with session_factory()() as session:
            row = await session.scalar(
                select(InboxEvent).where(InboxEvent.source == source, InboxEvent.event_id == event_id)
            )
            if row is None:
                return None
            matches = row.payload_sha256 == digest
            await session.execute(
                update(InboxEvent)
                .where(InboxEvent.id == row.id)
                .values(
                    delivery_count=InboxEvent.delivery_count + 1,
                    last_received_at=utcnow(),
                    conflicting_deliveries=InboxEvent.conflicting_deliveries + (0 if matches else 1),
                )
            )
            await session.commit()
        if not matches:
            logger.warning(
                "event id reused with a different payload; keeping the first",
                extra={"source": source, "event_id": event_id},
            )
        return row.id, matches

    async def _record_receipt(self, source: str, body: bytes, result: IngestResult, duration_ms: float) -> None:
        receipt = WebhookReceipt(
            received_at=utcnow(),
            source=source,
            event_id=result.event_id,
            outcome=result.outcome.value,
            reason=result.reason,
            http_status=result.http_status,
            body_bytes=len(body),
            body_sha256=hashlib.sha256(body).hexdigest(),
            duration_ms=duration_ms,
        )
        try:
            async with session_factory()() as session:
                session.add(receipt)
                await session.commit()
        except Exception:
            logger.exception("could not record webhook receipt", extra={"source": source})

    # ------------------------------------------------- fast path, fail open

    async def _begin(self, key: str) -> ClaimState:
        try:
            return await self.store.begin(key, self.settings.idempotency_lock_ttl_s)
        except Exception:
            # Fail open on the fast path only: the unique constraint still dedupes.
            logger.warning("idempotency store unavailable; relying on DB constraint", extra={"key": key})
            return ClaimState.NEW

    async def _complete(self, key: str) -> None:
        try:
            await self.store.complete(key, self.settings.idempotency_done_ttl_s)
        except Exception:
            logger.warning("idempotency store unavailable on complete", extra={"key": key})

    async def _release(self, key: str) -> None:
        try:
            await self.store.release(key)
        except Exception:
            logger.warning("idempotency store unavailable on release", extra={"key": key})
