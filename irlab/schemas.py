"""Wire formats: the webhook envelope every provider sends, and the hub's read models."""

from datetime import datetime
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class ResourceRef(BaseModel):
    type: str = Field(min_length=1, max_length=32)
    id: str = Field(min_length=1, max_length=128)


class EventEnvelope(BaseModel):
    """Common envelope for POS, payments and loyalty webhooks.

    `sequence` is the provider's per-resource version (order v1, v2, …). Providers
    do not guarantee delivery order; the hub uses `sequence` to detect and repair
    out-of-order delivery (M2).
    """

    id: str = Field(min_length=1, max_length=128)
    type: str = Field(min_length=3, max_length=64, pattern=r"^[a-z][a-z_]*\.[a-z][a-z_]*$")
    occurred_at: AwareDatetime
    resource: ResourceRef
    sequence: int | None = Field(default=None, ge=0)
    data: dict[str, Any] = Field(default_factory=dict)


class InboxEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source: str
    event_id: str
    event_type: str
    resource_type: str
    resource_id: str
    sequence: int | None
    occurred_at: datetime
    received_at: datetime
    last_received_at: datetime
    delivery_count: int
    conflicting_deliveries: int
    status: str


class InboxEventDetail(InboxEventOut):
    payload: dict[str, Any]
    payload_sha256: str


class ReceiptOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    received_at: datetime
    source: str
    event_id: str | None
    outcome: str
    reason: str | None
    http_status: int
    body_bytes: int
    duration_ms: float


# ------------------------------------------------------------------ M2


class OrderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    store_id: str
    status: str
    pos_status: str
    pos_version: int
    total_minor: int
    authorized_minor: int
    captured_minor: int
    currency: str
    loyalty_member_id: str | None
    loyalty_status: str
    points_awarded: int | None
    loyalty_accrual_id: str | None
    pos_created_at: datetime | None
    created_at: datetime
    updated_at: datetime
    last_event_id: str | None


class OrderPaymentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: str
    order_id: str
    amount_minor: int
    captured_minor: int
    currency: str
    status: str
    failure_code: str | None
    version: int
    updated_at: datetime


class OutboxOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    kind: str
    target: str
    method: str
    path: str
    payload: dict[str, Any]
    idempotency_key: str
    order_id: str | None
    correlation_id: str | None
    status: str
    attempts: int
    next_attempt_at: datetime
    last_error: str | None
    last_http_status: int | None
    response: dict[str, Any] | None
    created_at: datetime
    sent_at: datetime | None


class DeadLetterOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    origin: str
    origin_id: int
    reason_code: str
    last_error: str | None
    attempts: int
    status: str
    order_id: str | None
    source: str | None
    correlation_id: str | None
    details: dict[str, Any] | None
    created_at: datetime
    last_failed_at: datetime
    times_dead_lettered: int
    replay_count: int
    resolved_at: datetime | None
    resolution: str | None


class ReplayRequestOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    dead_letter_id: int
    action: str
    requested_by: str
    note: str | None
    requested_at: datetime
    result: str | None
    result_detail: str | None


class OrderDetail(OrderOut):
    payments: list[OrderPaymentOut]
    outbox: list[OutboxOut]
    dead_letters: list[DeadLetterOut]
    events: list[InboxEventOut]


class DeadLetterDetail(DeadLetterOut):
    replays: list[ReplayRequestOut]
    origin_record: dict[str, Any] | None


class OperatorAction(BaseModel):
    requested_by: str = Field(min_length=1, max_length=64, description="Who is doing this (name or ticket)")
    note: str | None = Field(default=None, max_length=500)


class BulkReplay(OperatorAction):
    reason_code: str | None = Field(default=None, max_length=64, description="Only dead letters with this reason")
    limit: int = Field(default=100, ge=1, le=500)
