"""Hub tables. Diagram and rationale: docs/data-model.md.

M1: inbox_events, webhook_receipts
M2: orders, order_payments, resource_versions, outbox_messages, dead_letters, replay_requests
M3: reconciliation_runs, mismatches, alerts
"""

from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import ForeignKey, Index, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from irlab.db import Base, JSONType, UTCDateTime, utcnow


class InboxStatus(StrEnum):
    RECEIVED = "received"  # verified + stored; waits for the processor
    IGNORED = "ignored"  # verified, but an event type the hub does not consume
    PROCESSING = "processing"  # claimed by a worker (lease in claimed_until)
    PROCESSED = "processed"  # applied to the canonical order
    PARKED = "parked"  # arrived ahead of a sequence gap; resumes when the gap closes
    SUPERSEDED = "superseded"  # older than the state already applied; skipped
    DEAD = "dead"  # dead-lettered (see dead_letters)


class ReceiptOutcome(StrEnum):
    ACCEPTED = "accepted"  # new event stored
    DUPLICATE = "duplicate"  # already stored; acknowledged with 2xx so the sender stops retrying
    IN_PROGRESS = "in_progress"  # same event is being stored by a concurrent delivery; sender retries
    REJECTED = "rejected"  # failed authentication or validation; nothing stored
    ERROR = "error"  # our side failed (storage); sender retries


class InboxEvent(Base):
    """One row per (source, event_id): the deduplicated, verified record of a webhook."""

    __tablename__ = "inbox_events"
    __table_args__ = (
        # The correctness guarantee for duplicate handling. Redis only short-circuits.
        UniqueConstraint("source", "event_id", name="uq_inbox_source_event"),
        Index("ix_inbox_resource", "resource_type", "resource_id", "sequence"),
        Index("ix_inbox_status", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(String(32))
    event_id: Mapped[str] = mapped_column(String(128))
    event_type: Mapped[str] = mapped_column(String(64))
    resource_type: Mapped[str] = mapped_column(String(32))
    resource_id: Mapped[str] = mapped_column(String(128))
    # Provider's per-resource version; drives out-of-order handling.
    sequence: Mapped[int | None] = mapped_column(default=None)
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    received_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    last_received_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    delivery_count: Mapped[int] = mapped_column(default=1)
    # Redeliveries that reused this event id with a different payload (provider bug signal).
    conflicting_deliveries: Mapped[int] = mapped_column(default=0)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType)
    payload_sha256: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), default=InboxStatus.RECEIVED)
    # "webhook" for deliveries; "reconciliation" for snapshots backfilled from a provider's REST API.
    origin: Mapped[str] = mapped_column(String(16), default="webhook")

    # ---- processing (M2) ----
    processing_attempts: Mapped[int] = mapped_column(default=0)
    claimed_until: Mapped[datetime | None] = mapped_column(UTCDateTime(), default=None)
    parked_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), default=None)
    processed_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), default=None)
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
    # Set by an operator replay after a sequence-gap timeout: apply this snapshot even
    # though an earlier version never arrived.
    skip_sequence_check: Mapped[bool] = mapped_column(default=False)


class WebhookReceipt(Base):
    """Every HTTP delivery attempt, including rejected ones. Never stores an unverified body."""

    __tablename__ = "webhook_receipts"
    __table_args__ = (
        Index("ix_receipts_source_outcome", "source", "outcome"),
        Index("ix_receipts_received_at", "received_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    received_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    source: Mapped[str] = mapped_column(String(32))
    # Only set once the signature verified; a forged request cannot plant ids here.
    event_id: Mapped[str | None] = mapped_column(String(128), default=None)
    outcome: Mapped[str] = mapped_column(String(16))
    reason: Mapped[str | None] = mapped_column(Text, default=None)
    http_status: Mapped[int] = mapped_column()
    body_bytes: Mapped[int] = mapped_column()
    body_sha256: Mapped[str] = mapped_column(String(64))
    duration_ms: Mapped[float] = mapped_column()


# ---------------------------------------------------------------- M2: orders


class OrderStatus(StrEnum):
    AWAITING_PAYMENT = "awaiting_payment"
    PAID = "paid"
    PARTIALLY_PAID = "partially_paid"
    OVERPAID = "overpaid"
    PAYMENT_FAILED = "payment_failed"
    CANCELLED = "cancelled"


class LoyaltyStatus(StrEnum):
    NOT_APPLICABLE = "not_applicable"  # no member on the order
    NOT_DUE = "not_due"  # member present, order not fully paid yet
    PENDING = "pending"  # accrual queued in the outbox
    ACCRUED = "accrued"  # provider confirmed the accrual
    MEMBER_MISSING = "member_missing"  # provider has no such member → DLQ
    FAILED = "failed"  # retries exhausted or rejected → DLQ
    SKIPPED = "skipped"  # order cancelled before the accrual was sent


class Order(Base):
    """Canonical order: the POS snapshot joined with payment and loyalty state."""

    __tablename__ = "orders"
    __table_args__ = (Index("ix_orders_status", "status"),)

    id: Mapped[str] = mapped_column(String(64), primary_key=True)  # POS order id
    store_id: Mapped[str] = mapped_column(String(32))
    pos_status: Mapped[str] = mapped_column(String(16))  # placed | cancelled (POS's view)
    pos_version: Mapped[int] = mapped_column(default=0)
    status: Mapped[str] = mapped_column(String(24), default=OrderStatus.AWAITING_PAYMENT)
    total_minor: Mapped[int] = mapped_column()
    authorized_minor: Mapped[int] = mapped_column(default=0)
    captured_minor: Mapped[int] = mapped_column(default=0)
    currency: Mapped[str] = mapped_column(String(3))
    loyalty_member_id: Mapped[str | None] = mapped_column(String(32), default=None)
    loyalty_status: Mapped[str] = mapped_column(String(24), default=LoyaltyStatus.NOT_APPLICABLE)
    points_awarded: Mapped[int | None] = mapped_column(default=None)
    loyalty_accrual_id: Mapped[str | None] = mapped_column(String(64), default=None)
    pos_created_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), default=None)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    # Last inbox event that changed this order (correlation for logs and triage).
    last_event_id: Mapped[str | None] = mapped_column(String(128), default=None)


class OrderPayment(Base):
    """Payment snapshot. order_id is the provider's order_ref and may arrive before the order."""

    __tablename__ = "order_payments"
    __table_args__ = (Index("ix_order_payments_order", "order_id"),)

    payment_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(64))
    amount_minor: Mapped[int] = mapped_column()
    captured_minor: Mapped[int] = mapped_column(default=0)
    currency: Mapped[str] = mapped_column(String(3))
    status: Mapped[str] = mapped_column(String(24))
    failure_code: Mapped[str | None] = mapped_column(String(64), default=None)
    version: Mapped[int] = mapped_column(default=0)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)


class ResourceVersion(Base):
    """Highest provider sequence applied per resource (out-of-order handling)."""

    __tablename__ = "resource_versions"

    source: Mapped[str] = mapped_column(String(32), primary_key=True)
    resource_type: Mapped[str] = mapped_column(String(32), primary_key=True)
    resource_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    last_sequence: Mapped[int] = mapped_column(default=0)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)


class OutboxStatus(StrEnum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    SENT = "sent"
    DEAD = "dead"
    SKIPPED = "skipped"


class OutboxMessage(Base):
    """An outbound call, written in the same transaction as the state change that needs it."""

    __tablename__ = "outbox_messages"
    __table_args__ = (
        # Reused on every retry and replay: the provider deduplicates on it.
        UniqueConstraint("idempotency_key", name="uq_outbox_idempotency_key"),
        Index("ix_outbox_due", "status", "next_attempt_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    kind: Mapped[str] = mapped_column(String(32))  # loyalty.accrue
    target: Mapped[str] = mapped_column(String(32))  # provider name
    method: Mapped[str] = mapped_column(String(8))
    path: Mapped[str] = mapped_column(String(256))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType)
    idempotency_key: Mapped[str] = mapped_column(String(128))
    order_id: Mapped[str | None] = mapped_column(String(64), default=None)
    correlation_id: Mapped[str | None] = mapped_column(String(128), default=None)
    status: Mapped[str] = mapped_column(String(16), default=OutboxStatus.PENDING)
    attempts: Mapped[int] = mapped_column(default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    claimed_until: Mapped[datetime | None] = mapped_column(UTCDateTime(), default=None)
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
    last_http_status: Mapped[int | None] = mapped_column(default=None)
    response: Mapped[dict[str, Any] | None] = mapped_column(JSONType, default=None)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    sent_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), default=None)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)


class DeadLetterStatus(StrEnum):
    OPEN = "open"
    REPLAYING = "replaying"  # replayed; waiting for the outcome (e.g. retry scheduled)
    RESOLVED = "resolved"
    DISCARDED = "discarded"


class DeadLetter(Base):
    """An inbox event or outbox message that cannot proceed without a human. One row per item."""

    __tablename__ = "dead_letters"
    __table_args__ = (
        UniqueConstraint("origin", "origin_id", name="uq_dead_letter_origin"),
        Index("ix_dead_letters_status_reason", "status", "reason_code"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    origin: Mapped[str] = mapped_column(String(16))  # inbox | outbox
    origin_id: Mapped[int] = mapped_column()
    reason_code: Mapped[str] = mapped_column(String(64))
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
    attempts: Mapped[int] = mapped_column(default=0)
    status: Mapped[str] = mapped_column(String(16), default=DeadLetterStatus.OPEN)
    order_id: Mapped[str | None] = mapped_column(String(64), default=None)
    source: Mapped[str | None] = mapped_column(String(32), default=None)
    correlation_id: Mapped[str | None] = mapped_column(String(128), default=None)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONType, default=None)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    last_failed_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    times_dead_lettered: Mapped[int] = mapped_column(default=1)
    replay_count: Mapped[int] = mapped_column(default=0)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), default=None)
    resolution: Mapped[str | None] = mapped_column(String(32), default=None)


class ReplayRequest(Base):
    """Audit trail: who replayed or discarded which dead letter, why, and what happened."""

    __tablename__ = "replay_requests"

    id: Mapped[int] = mapped_column(primary_key=True)
    dead_letter_id: Mapped[int] = mapped_column(ForeignKey("dead_letters.id"))
    action: Mapped[str] = mapped_column(String(16))  # replay | discard
    requested_by: Mapped[str] = mapped_column(String(64))
    note: Mapped[str | None] = mapped_column(Text, default=None)
    requested_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    # succeeded | failed_again | retry_scheduled | discarded
    result: Mapped[str | None] = mapped_column(String(24), default=None)
    result_detail: Mapped[str | None] = mapped_column(Text, default=None)


# ------------------------------------------------------- M3: reconciliation


class ReconciliationRun(Base):
    __tablename__ = "reconciliation_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), default=None)
    window_since: Mapped[datetime | None] = mapped_column(UTCDateTime(), default=None)
    triggered_by: Mapped[str] = mapped_column(String(64), default="worker")
    status: Mapped[str] = mapped_column(String(16), default="running")  # running | ok | partial | failed
    orders_checked: Mapped[int] = mapped_column(default=0)
    payments_checked: Mapped[int] = mapped_column(default=0)
    accruals_checked: Mapped[int] = mapped_column(default=0)
    skipped_in_grace: Mapped[int] = mapped_column(default=0)
    # Provider records whose webhook is already in the hub's inbox, not yet applied.
    skipped_in_flight: Mapped[int] = mapped_column(default=0)
    mismatches_found: Mapped[int] = mapped_column(default=0)
    new_mismatches: Mapped[int] = mapped_column(default=0)
    resolved_mismatches: Mapped[int] = mapped_column(default=0)
    repairs_queued: Mapped[int] = mapped_column(default=0)
    error: Mapped[str | None] = mapped_column(Text, default=None)


class Mismatch(Base):
    """A typed disagreement between the hub and a provider. One open row per (kind, entity)."""

    __tablename__ = "mismatches"
    __table_args__ = (Index("ix_mismatches_status_kind", "status", "kind"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    kind: Mapped[str] = mapped_column(String(32))
    entity_type: Mapped[str] = mapped_column(String(16))  # order | payment
    entity_id: Mapped[str] = mapped_column(String(64))
    order_id: Mapped[str | None] = mapped_column(String(64), default=None)
    severity: Mapped[str] = mapped_column(String(16), default="warning")
    expected: Mapped[dict[str, Any] | None] = mapped_column(JSONType, default=None)
    actual: Mapped[dict[str, Any] | None] = mapped_column(JSONType, default=None)
    status: Mapped[str] = mapped_column(String(16), default="open")  # open | resolved
    first_run_id: Mapped[int | None] = mapped_column(ForeignKey("reconciliation_runs.id"), default=None)
    last_run_id: Mapped[int | None] = mapped_column(ForeignKey("reconciliation_runs.id"), default=None)
    first_detected_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    seen_count: Mapped[int] = mapped_column(default=1)
    repair: Mapped[str | None] = mapped_column(String(32), default=None)  # backfill_queued | manual
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), default=None)
    resolution: Mapped[str | None] = mapped_column(String(32), default=None)


class Alert(Base):
    """One row per firing of a rule; resolved_at is set when the condition clears."""

    __tablename__ = "alerts"
    __table_args__ = (Index("ix_alerts_rule_open", "rule", "resolved_at"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    rule: Mapped[str] = mapped_column(String(32))
    severity: Mapped[str] = mapped_column(String(16))
    summary: Mapped[str] = mapped_column(Text)
    value: Mapped[float] = mapped_column()
    threshold: Mapped[float] = mapped_column()
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONType, default=None)
    fired_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    last_evaluated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), default=None)
