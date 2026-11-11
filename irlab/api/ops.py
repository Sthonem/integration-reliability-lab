"""Operator API: canonical orders, outbox, dead letters, replay/discard, worker tick.

Reads are open (lab scope; see docs/runbook.md). Every write requires the
`X-Ops-Key` header to match OPS_API_KEY. With no key configured the write
endpoints answer 503 — fail closed, like unconfigured webhook sources.
"""

import secrets
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from irlab.db import get_session
from irlab.models import (
    DeadLetter,
    InboxEvent,
    Order,
    OrderPayment,
    OutboxMessage,
    ReplayRequest,
)
from irlab.processing import order_id_for
from irlab.replay import ReplayError
from irlab.schemas import (
    BulkReplay,
    DeadLetterDetail,
    DeadLetterOut,
    InboxEventDetail,
    InboxEventOut,
    OperatorAction,
    OrderDetail,
    OrderOut,
    OutboxOut,
    ReplayRequestOut,
)

router = APIRouter(prefix="/v1", tags=["operations"])
Session = Annotated[AsyncSession, Depends(get_session)]


def _err(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status, detail={"code": code, "message": message})


async def require_ops_key(request: Request, x_ops_key: Annotated[str | None, Header()] = None) -> None:
    expected = request.app.state.settings.ops_api_key.get_secret_value()
    if not expected:
        raise _err(503, "ops_key_not_configured", "OPS_API_KEY is not set; operator writes are disabled")
    if not x_ops_key or not secrets.compare_digest(x_ops_key.encode(), expected.encode()):
        raise _err(401, "invalid_ops_key", "Missing or wrong X-Ops-Key header")


OpsKey = Depends(require_ops_key)


# ------------------------------------------------------------------ orders


@router.get("/orders", response_model=list[OrderOut], summary="Canonical orders (POS + payments + loyalty)")
async def list_orders(
    session: Session,
    status: str | None = None,
    loyalty_status: str | None = None,
    store_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    stmt = select(Order).order_by(Order.created_at.desc(), Order.id.desc()).limit(limit)
    if status:
        stmt = stmt.where(Order.status == status)
    if loyalty_status:
        stmt = stmt.where(Order.loyalty_status == loyalty_status)
    if store_id:
        stmt = stmt.where(Order.store_id == store_id)
    return (await session.scalars(stmt)).all()


@router.get("/orders/{order_id}", response_model=OrderDetail)
async def get_order(order_id: str, session: Session):
    order = await session.get(Order, order_id)
    if order is None:
        raise _err(404, "order_not_found", f"No order {order_id}")
    payments = (await session.scalars(select(OrderPayment).where(OrderPayment.order_id == order_id))).all()
    outbox = (await session.scalars(select(OutboxMessage).where(OutboxMessage.order_id == order_id))).all()
    dead = (await session.scalars(select(DeadLetter).where(DeadLetter.order_id == order_id))).all()
    payment_ids = [p.payment_id for p in payments]
    events = (
        await session.scalars(
            select(InboxEvent)
            .where(
                ((InboxEvent.source == "pos") & (InboxEvent.resource_id == order_id))
                | ((InboxEvent.source == "payments") & (InboxEvent.resource_id.in_(payment_ids or [""])))
            )
            .order_by(InboxEvent.id)
        )
    ).all()
    out = OrderOut.model_validate(order).model_dump()
    return OrderDetail(
        **out,
        payments=payments,
        outbox=outbox,
        dead_letters=dead,
        events=[InboxEventOut.model_validate(e) for e in events],
    )


@router.get("/outbox", response_model=list[OutboxOut], summary="Outbound calls and their retry state")
async def list_outbox(
    session: Session,
    status: str | None = None,
    order_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    stmt = select(OutboxMessage).order_by(OutboxMessage.id.desc()).limit(limit)
    if status:
        stmt = stmt.where(OutboxMessage.status == status)
    if order_id:
        stmt = stmt.where(OutboxMessage.order_id == order_id)
    return (await session.scalars(stmt)).all()


# ------------------------------------------------------------ dead letters


@router.get("/dead-letters", response_model=list[DeadLetterOut], summary="Dead-letter queue")
async def list_dead_letters(
    session: Session,
    status: str | None = Query(None, description="open | replaying | resolved | discarded"),
    reason_code: str | None = None,
    origin: str | None = None,
    order_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    stmt = select(DeadLetter).order_by(DeadLetter.id.desc()).limit(limit)
    if status:
        stmt = stmt.where(DeadLetter.status == status)
    if reason_code:
        stmt = stmt.where(DeadLetter.reason_code == reason_code)
    if origin:
        stmt = stmt.where(DeadLetter.origin == origin)
    if order_id:
        stmt = stmt.where(DeadLetter.order_id == order_id)
    return (await session.scalars(stmt)).all()


async def origin_record(session: AsyncSession, dl: DeadLetter) -> dict[str, Any] | None:
    if dl.origin == "outbox":
        msg = await session.get(OutboxMessage, dl.origin_id)
        return OutboxOut.model_validate(msg).model_dump(mode="json") if msg else None
    ev = await session.get(InboxEvent, dl.origin_id)
    if ev is None:
        return None
    return InboxEventDetail.model_validate(ev).model_dump(mode="json") | {"order_id": order_id_for(ev)}


@router.get("/dead-letters/{dead_letter_id}", response_model=DeadLetterDetail)
async def get_dead_letter(dead_letter_id: int, session: Session):
    dl = await session.get(DeadLetter, dead_letter_id)
    if dl is None:
        raise _err(404, "dead_letter_not_found", f"No dead letter {dead_letter_id}")
    replays = (
        await session.scalars(
            select(ReplayRequest).where(ReplayRequest.dead_letter_id == dead_letter_id).order_by(ReplayRequest.id)
        )
    ).all()
    return DeadLetterDetail(
        **DeadLetterOut.model_validate(dl).model_dump(),
        replays=[ReplayRequestOut.model_validate(r) for r in replays],
        origin_record=await origin_record(session, dl),
    )


def _replay_error(exc: ReplayError) -> HTTPException:
    return _err(exc.status, exc.code, exc.message)


@router.post("/dead-letters/replay", dependencies=[OpsKey], summary="Bulk replay (e.g. every member_not_found)")
async def replay_bulk(body: BulkReplay, request: Request):
    return await request.app.state.runtime.replay.replay_bulk(
        body.requested_by, reason_code=body.reason_code, note=body.note, limit=body.limit
    )


@router.post("/dead-letters/{dead_letter_id}/replay", dependencies=[OpsKey], summary="Replay one dead letter now")
async def replay_one(dead_letter_id: int, body: OperatorAction, request: Request):
    try:
        outcome = await request.app.state.runtime.replay.replay(dead_letter_id, body.requested_by, body.note)
    except ReplayError as exc:
        raise _replay_error(exc) from exc
    return outcome.as_dict()


@router.post("/dead-letters/{dead_letter_id}/discard", dependencies=[OpsKey], summary="Close without replaying")
async def discard_one(dead_letter_id: int, body: OperatorAction, request: Request):
    try:
        outcome = await request.app.state.runtime.replay.discard(dead_letter_id, body.requested_by, body.note)
    except ReplayError as exc:
        raise _replay_error(exc) from exc
    return outcome.as_dict()


@router.get("/replays", response_model=list[ReplayRequestOut], summary="Replay / discard audit trail")
async def list_replays(session: Session, limit: Annotated[int, Query(ge=1, le=500)] = 50):
    return (await session.scalars(select(ReplayRequest).order_by(ReplayRequest.id.desc()).limit(limit))).all()


# ------------------------------------------------------------------ worker


@router.post("/worker/tick", dependencies=[OpsKey], summary="Run one worker pass now (or drain the backlog)")
async def worker_tick(
    request: Request,
    drain: bool = False,
    max_wait_s: Annotated[float, Query(ge=0, le=60)] = 10.0,
):
    worker = request.app.state.runtime.worker
    if drain:
        return await worker.drain(max_wait_s)
    return await worker.tick(periodic=False) | await worker.backlog()
