"""Read-only views of what the hub has received. The M3 dashboard builds on these."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from irlab.db import get_session
from irlab.models import InboxEvent, WebhookReceipt
from irlab.schemas import InboxEventDetail, InboxEventOut, ReceiptOut

router = APIRouter(prefix="/v1", tags=["inbox"])
Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("/inbox", response_model=list[InboxEventOut], summary="Deduplicated, verified webhook events")
async def list_inbox(
    session: Session,
    source: str | None = None,
    status: str | None = None,
    event_type: str | None = None,
    resource_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    stmt = select(InboxEvent).order_by(InboxEvent.id.desc()).limit(limit)
    if source:
        stmt = stmt.where(InboxEvent.source == source)
    if status:
        stmt = stmt.where(InboxEvent.status == status)
    if event_type:
        stmt = stmt.where(InboxEvent.event_type == event_type)
    if resource_id:
        stmt = stmt.where(InboxEvent.resource_id == resource_id)
    return (await session.scalars(stmt)).all()


@router.get("/inbox/{inbox_id}", response_model=InboxEventDetail)
async def get_inbox_event(inbox_id: int, session: Session):
    row = await session.get(InboxEvent, inbox_id)
    if row is None:
        raise HTTPException(404, detail={"code": "inbox_event_not_found", "message": f"No inbox event {inbox_id}"})
    return row


@router.get(
    "/webhook-receipts",
    response_model=list[ReceiptOut],
    summary="Every delivery attempt, including duplicates and rejections",
)
async def list_receipts(
    session: Session,
    source: str | None = None,
    outcome: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    stmt = select(WebhookReceipt).order_by(WebhookReceipt.id.desc()).limit(limit)
    if source:
        stmt = stmt.where(WebhookReceipt.source == source)
    if outcome:
        stmt = stmt.where(WebhookReceipt.outcome == outcome)
    return (await session.scalars(stmt)).all()
