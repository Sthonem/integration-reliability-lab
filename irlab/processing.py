"""Inbox processor: projects verified events into canonical orders.

Per event, one database transaction updates the projection (orders,
order_payments), the per-resource sequence, any outbox messages and the event's
status. Nothing in that transaction talks to the network; outbound calls are
written to the outbox and sent by the dispatcher (`irlab.outbox`).

Ordering. Providers number each resource's versions (`sequence`). For POS
orders and payments:

* `sequence <= last applied`  → `superseded` (an equal or newer snapshot is already applied)
* `sequence == last + 1`      → apply, then wake any parked events of that resource
* `sequence >  last + 1`      → `parked` until the gap closes; after `park_timeout_s`
                                 the event is dead-lettered (`sequence_gap_timeout`)
                                 and an operator replay applies it as a snapshot.

Payments may arrive before their order (different providers, no ordering
between them): they are stored by `order_ref` and folded in when the order
arrives. Loyalty events confirm accruals and are order-independent.
"""

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from irlab.clock import Clock
from irlab.config import Settings
from irlab.db import session_factory
from irlab.deadletters import record_dead_letter, resolve_dead_letter
from irlab.logs import log_context
from irlab.models import (
    DeadLetter,
    DeadLetterStatus,
    InboxEvent,
    InboxStatus,
    LoyaltyStatus,
    Order,
    OrderPayment,
    OrderStatus,
    OutboxMessage,
    OutboxStatus,
    ResourceVersion,
)

logger = logging.getLogger("irlab.processor")

ORDERED_RESOURCES = {("pos", "order"), ("payments", "payment")}
CAPTURED_STATUSES = ("captured", "partially_captured")
AUTHORIZED_STATUSES = ("authorized", "captured", "partially_captured")


class PayloadError(ValueError):
    """The event can never be applied as sent: dead-letter it, do not retry."""


def _require(data: dict[str, Any], key: str, kind: type | tuple[type, ...]) -> Any:
    value = data.get(key)
    if not isinstance(value, kind) or isinstance(value, bool) and kind is int:
        raise PayloadError(f"data.{key} missing or not {getattr(kind, '__name__', kind)}")
    return value


def _parse_dt(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else None


def order_id_for(event: InboxEvent) -> str | None:
    """The canonical order an event belongs to (lock key, DLQ/triage context)."""
    data = (event.payload or {}).get("data") or {}
    if event.source == "pos" and event.resource_type == "order":
        return event.resource_id
    ref = data.get("order_ref")
    return ref if isinstance(ref, str) else None


def accrual_key(order_id: str) -> str:
    """Stable per business action: every retry and replay sends the same key."""
    return f"loyalty-accrual:{order_id}"


class Processor:
    def __init__(self, settings: Settings, clock: Clock):
        self.settings = settings
        self.clock = clock
        self._handlers: dict[tuple[str, str], Callable[[AsyncSession, InboxEvent, datetime], Awaitable[None]]] = {
            ("pos", "order"): self._apply_order,
            ("payments", "payment"): self._apply_payment,
            ("loyalty", "points.accrued"): self._apply_accrual,
        }

    # ------------------------------------------------------------ driver

    async def run_once(self, max_events: int | None = None) -> int:
        """Process every received event (including ones woken from parking). Returns the count."""
        limit = max_events or self.settings.worker_batch_size * 10
        done = 0
        while done < limit:
            async with session_factory()() as s:
                ids = (
                    await s.scalars(
                        select(InboxEvent.id)
                        .where(InboxEvent.status == InboxStatus.RECEIVED)
                        .order_by(InboxEvent.id)
                        .limit(min(self.settings.worker_batch_size, limit - done))
                    )
                ).all()
            if not ids:
                break
            progressed = 0
            for inbox_id in ids:
                if await self.process_one(inbox_id) is not None:
                    progressed += 1
            done += progressed
            if progressed == 0:
                break
        return done

    async def recover_expired_claims(self) -> int:
        now = self.clock.now()
        async with session_factory()() as s:
            res = await s.execute(
                update(InboxEvent)
                .where(InboxEvent.status == InboxStatus.PROCESSING, InboxEvent.claimed_until < now)
                .values(status=InboxStatus.RECEIVED, claimed_until=None)
            )
            await s.commit()
        if res.rowcount:
            logger.warning("recovered inbox events from expired claims", extra={"count": res.rowcount})
        return res.rowcount or 0

    async def expire_parked(self) -> int:
        """Parked longer than park_timeout_s → dead letter `sequence_gap_timeout`."""
        now = self.clock.now()
        cutoff = now - timedelta(seconds=self.settings.park_timeout_s)
        expired = 0
        async with session_factory()() as s, s.begin():
            events = (
                await s.scalars(
                    select(InboxEvent)
                    .where(InboxEvent.status == InboxStatus.PARKED, InboxEvent.parked_at < cutoff)
                    .with_for_update()
                )
            ).all()
            for ev in events:
                rv = await s.get(ResourceVersion, (ev.source, ev.resource_type, ev.resource_id))
                last = rv.last_sequence if rv else 0
                ev.status = InboxStatus.DEAD
                ev.last_error = f"waited {self.settings.park_timeout_s:g}s for sequence {last + 1}, have {ev.sequence}"
                await record_dead_letter(
                    s,
                    origin="inbox",
                    origin_id=ev.id,
                    reason_code="sequence_gap_timeout",
                    last_error=ev.last_error,
                    attempts=ev.processing_attempts,
                    now=now,
                    order_id=order_id_for(ev),
                    source=ev.source,
                    correlation_id=ev.event_id,
                    details={
                        "resource": f"{ev.resource_type}/{ev.resource_id}",
                        "expected_sequence": last + 1,
                        "parked_sequence": ev.sequence,
                        "event_type": ev.event_type,
                    },
                )
                expired += 1
        return expired

    async def process_one(self, inbox_id: int) -> str | None:
        """Claim and process one received event. Returns its new status, or None if not claimable."""
        now = self.clock.now()
        async with session_factory()() as s:
            res = await s.execute(
                update(InboxEvent)
                .where(InboxEvent.id == inbox_id, InboxEvent.status == InboxStatus.RECEIVED)
                .values(
                    status=InboxStatus.PROCESSING,
                    claimed_until=now + timedelta(seconds=self.settings.claim_lease_s),
                    processing_attempts=InboxEvent.processing_attempts + 1,
                )
            )
            await s.commit()
        if res.rowcount != 1:
            return None

        try:
            async with session_factory()() as s, s.begin():
                ev = await s.get(InboxEvent, inbox_id)
                assert ev is not None
                with log_context(correlation_id=ev.event_id, order_id=order_id_for(ev), inbox_id=ev.id):
                    status = await self._apply(s, ev, now)
                    logger.info(
                        "inbox event %s",
                        status,
                        extra={"event_type": ev.event_type, "sequence": ev.sequence, "source": ev.source},
                    )
                    return status
        except PayloadError as exc:
            return await self._fail(inbox_id, "invalid_payload", str(exc), dead=True)
        except Exception as exc:  # noqa: BLE001 - every failure must end in retry or DLQ
            logger.exception("inbox processing failed", extra={"inbox_id": inbox_id})
            return await self._fail(inbox_id, "processing_failed", f"{type(exc).__name__}: {exc}"[:500], dead=False)

    async def _fail(self, inbox_id: int, reason: str, error: str, *, dead: bool) -> str:
        now = self.clock.now()
        async with session_factory()() as s, s.begin():
            ev = await s.get(InboxEvent, inbox_id, with_for_update=True)
            assert ev is not None
            ev.last_error = error
            ev.claimed_until = None
            if not dead and ev.processing_attempts < self.settings.processing_max_attempts:
                ev.status = InboxStatus.RECEIVED
                return ev.status
            ev.status = InboxStatus.DEAD
            await record_dead_letter(
                s,
                origin="inbox",
                origin_id=ev.id,
                reason_code=reason,
                last_error=error,
                attempts=ev.processing_attempts,
                now=now,
                order_id=order_id_for(ev),
                source=ev.source,
                correlation_id=ev.event_id,
                details={"event_type": ev.event_type, "resource": f"{ev.resource_type}/{ev.resource_id}"},
            )
            return ev.status

    # ------------------------------------------------------------ apply

    async def _apply(self, s: AsyncSession, ev: InboxEvent, now: datetime) -> str:
        await self._lock(s, ev)
        handler = self._handlers.get((ev.source, ev.resource_type)) or self._handlers.get((ev.source, ev.event_type))
        ordered = (ev.source, ev.resource_type) in ORDERED_RESOURCES and ev.sequence is not None

        rv: ResourceVersion | None = None
        if ordered:
            rv = await s.get(ResourceVersion, (ev.source, ev.resource_type, ev.resource_id), with_for_update=True)
            last = rv.last_sequence if rv else 0
            assert ev.sequence is not None
            if ev.sequence <= last:
                ev.status = InboxStatus.SUPERSEDED
                ev.processed_at = now
                ev.claimed_until = None
                await resolve_dead_letter(
                    s, origin="inbox", origin_id=ev.id, resolution="superseded", now=now,
                    statuses=(DeadLetterStatus.OPEN, DeadLetterStatus.REPLAYING),
                )
                return ev.status
            if ev.sequence > last + 1 and not ev.skip_sequence_check:
                ev.status = InboxStatus.PARKED
                ev.parked_at = ev.parked_at or now
                ev.claimed_until = None
                logger.warning(
                    "event parked: sequence gap",
                    extra={"expected_sequence": last + 1, "sequence": ev.sequence},
                )
                return ev.status

        if handler is not None:
            await handler(s, ev, now)

        if ordered:
            assert ev.sequence is not None
            if rv is None:
                rv = ResourceVersion(source=ev.source, resource_type=ev.resource_type, resource_id=ev.resource_id)
                s.add(rv)
            rv.last_sequence = ev.sequence
            rv.updated_at = now
            await self._wake_parked(s, ev)
            await self._supersede_dead(s, ev, ev.sequence, now)

        ev.status = InboxStatus.PROCESSED
        ev.processed_at = now
        ev.claimed_until = None
        ev.last_error = None
        await resolve_dead_letter(s, origin="inbox", origin_id=ev.id, resolution="replayed", now=now)
        return ev.status

    async def _lock(self, s: AsyncSession, ev: InboxEvent) -> None:
        """Serialise all events of one order across worker replicas (PostgreSQL only).

        SQLite has a single writer anyway. The lock is per order, not per resource,
        because payment and loyalty events also update the order row.
        """
        if s.bind.dialect.name != "postgresql":
            return
        key = order_id_for(ev) or f"{ev.source}:{ev.resource_type}:{ev.resource_id}"
        await s.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"irlab:{key}"})

    async def _wake_parked(self, s: AsyncSession, ev: InboxEvent) -> None:
        await s.execute(
            update(InboxEvent)
            .where(
                InboxEvent.source == ev.source,
                InboxEvent.resource_type == ev.resource_type,
                InboxEvent.resource_id == ev.resource_id,
                InboxEvent.status == InboxStatus.PARKED,
            )
            .values(status=InboxStatus.RECEIVED)
        )

    async def _supersede_dead(self, s: AsyncSession, ev: InboxEvent, applied: int, now: datetime) -> None:
        """Dead-lettered events of this resource that a newer snapshot made obsolete."""
        stale = (
            await s.scalars(
                select(InboxEvent).where(
                    InboxEvent.source == ev.source,
                    InboxEvent.resource_type == ev.resource_type,
                    InboxEvent.resource_id == ev.resource_id,
                    InboxEvent.status == InboxStatus.DEAD,
                    InboxEvent.sequence <= applied,
                    InboxEvent.id != ev.id,
                )
            )
        ).all()
        for old in stale:
            dl = await s.scalar(
                select(DeadLetter).where(DeadLetter.origin == "inbox", DeadLetter.origin_id == old.id)
            )
            if dl is not None and dl.reason_code != "sequence_gap_timeout":
                continue  # a real processing failure stays visible
            old.status = InboxStatus.SUPERSEDED
            await resolve_dead_letter(
                s, origin="inbox", origin_id=old.id, resolution="superseded", now=now,
                statuses=(DeadLetterStatus.OPEN, DeadLetterStatus.REPLAYING),
            )

    # ------------------------------------------------------------ handlers

    async def _apply_order(self, s: AsyncSession, ev: InboxEvent, now: datetime) -> None:
        data = (ev.payload or {}).get("data") or {}
        if data.get("id") != ev.resource_id:
            raise PayloadError("data.id does not match resource.id")
        store_id = _require(data, "store_id", str)
        pos_status = _require(data, "status", str)
        if pos_status not in ("placed", "cancelled"):
            raise PayloadError(f"unknown POS order status {pos_status!r}")
        total = _require(data, "total_minor", int)
        if total < 0:
            raise PayloadError("data.total_minor is negative")
        currency = _require(data, "currency", str)
        member = data.get("loyalty_member_id")
        if member is not None and not isinstance(member, str):
            raise PayloadError("data.loyalty_member_id is not a string")

        order = await s.get(Order, ev.resource_id, with_for_update=True)
        if order is None:
            order = Order(id=ev.resource_id, created_at=now, total_minor=total, currency=currency, store_id=store_id,
                          pos_status=pos_status, loyalty_status=LoyaltyStatus.NOT_APPLICABLE)
            s.add(order)
        order.store_id = store_id
        order.pos_status = pos_status
        order.total_minor = total
        order.currency = currency
        order.loyalty_member_id = member
        if ev.sequence is not None:
            order.pos_version = max(order.pos_version or 0, ev.sequence)
        order.pos_created_at = _parse_dt(data.get("created_at")) or order.pos_created_at
        order.updated_at = now
        order.last_event_id = ev.event_id
        await s.flush()
        await self._refresh(s, order, ev, now)

    async def _apply_payment(self, s: AsyncSession, ev: InboxEvent, now: datetime) -> None:
        data = (ev.payload or {}).get("data") or {}
        if data.get("id") != ev.resource_id:
            raise PayloadError("data.id does not match resource.id")
        order_ref = _require(data, "order_ref", str)
        amount = _require(data, "amount_minor", int)
        captured = data.get("captured_minor", 0)
        if not isinstance(captured, int) or captured < 0 or amount <= 0:
            raise PayloadError("invalid payment amounts")
        status = _require(data, "status", str)
        currency = _require(data, "currency", str)

        payment = await s.get(OrderPayment, ev.resource_id, with_for_update=True)
        if payment is None:
            payment = OrderPayment(payment_id=ev.resource_id, order_id=order_ref, amount_minor=amount,
                                   currency=currency, status=status)
            s.add(payment)
        payment.order_id = order_ref
        payment.amount_minor = amount
        payment.captured_minor = captured
        payment.currency = currency
        payment.status = status
        payment.failure_code = data.get("failure_code")
        payment.version = ev.sequence if ev.sequence is not None else payment.version
        payment.updated_at = now
        await s.flush()

        order = await s.get(Order, order_ref, with_for_update=True)
        if order is None:
            logger.info("payment for an order the hub has not seen yet; kept until it arrives",
                        extra={"payment_id": ev.resource_id})
            return
        order.last_event_id = ev.event_id
        await self._refresh(s, order, ev, now)

    async def _apply_accrual(self, s: AsyncSession, ev: InboxEvent, now: datetime) -> None:
        data = (ev.payload or {}).get("data") or {}
        accrual_id = _require(data, "id", str)
        order_ref = _require(data, "order_ref", str)
        points = _require(data, "points", int)
        order = await s.get(Order, order_ref, with_for_update=True)
        if order is None:
            logger.warning("points accrued for an order the hub does not know", extra={"accrual_id": accrual_id})
            return
        if order.loyalty_accrual_id and order.loyalty_accrual_id != accrual_id:
            # Double points at the provider. Reconciliation reports it as duplicate_accrual.
            logger.warning(
                "second accrual for one order",
                extra={"accrual_id": accrual_id, "first_accrual_id": order.loyalty_accrual_id},
            )
            return
        order.loyalty_accrual_id = accrual_id
        order.points_awarded = points
        order.loyalty_status = LoyaltyStatus.ACCRUED
        order.updated_at = now

    # ------------------------------------------------------------ projection

    async def _refresh(self, s: AsyncSession, order: Order, ev: InboxEvent, now: datetime) -> None:
        payments = (await s.scalars(select(OrderPayment).where(OrderPayment.order_id == order.id))).all()
        order.authorized_minor = sum(p.amount_minor for p in payments if p.status in AUTHORIZED_STATUSES)
        order.captured_minor = sum(p.captured_minor for p in payments if p.status in CAPTURED_STATUSES)

        if order.pos_status == "cancelled":
            status = OrderStatus.CANCELLED
        elif order.captured_minor == 0:
            failed = payments and all(p.status == "failed" for p in payments)
            status = OrderStatus.PAYMENT_FAILED if failed else OrderStatus.AWAITING_PAYMENT
        elif order.captured_minor < order.total_minor:
            status = OrderStatus.PARTIALLY_PAID
        elif order.captured_minor == order.total_minor:
            status = OrderStatus.PAID
        else:
            status = OrderStatus.OVERPAID
        if status != order.status:
            logger.info("order status %s → %s", order.status, status, extra={"order_id": order.id})
        order.status = status
        order.updated_at = now

        if not order.loyalty_member_id:
            if order.loyalty_status in (LoyaltyStatus.NOT_DUE, LoyaltyStatus.NOT_APPLICABLE):
                order.loyalty_status = LoyaltyStatus.NOT_APPLICABLE
            return
        if order.loyalty_status not in (LoyaltyStatus.NOT_APPLICABLE, LoyaltyStatus.NOT_DUE):
            return  # already queued, accrued or in the DLQ
        if status != OrderStatus.PAID:
            order.loyalty_status = LoyaltyStatus.NOT_DUE
            return

        key = accrual_key(order.id)
        if await s.scalar(select(OutboxMessage.id).where(OutboxMessage.idempotency_key == key)) is None:
            s.add(
                OutboxMessage(
                    kind="loyalty.accrue",
                    target="loyalty",
                    method="POST",
                    path="/accruals",
                    payload={
                        "member_id": order.loyalty_member_id,
                        "order_ref": order.id,
                        "amount_minor": order.total_minor,
                        "currency": order.currency,
                    },
                    idempotency_key=key,
                    order_id=order.id,
                    correlation_id=ev.event_id,
                    status=OutboxStatus.PENDING,
                    next_attempt_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            logger.info("loyalty accrual queued in outbox", extra={"order_id": order.id})
        order.loyalty_status = LoyaltyStatus.PENDING
