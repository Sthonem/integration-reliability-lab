"""Dead-letter bookkeeping shared by the processor and the outbox dispatcher.

One `dead_letters` row per failed item (`UNIQUE(origin, origin_id)`): if a
replayed item fails again, the same row is reopened and `times_dead_lettered`
goes up, so the queue never shows the same problem twice.
"""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from irlab.models import DeadLetter, DeadLetterStatus

logger = logging.getLogger("irlab.dlq")


async def record_dead_letter(
    session: AsyncSession,
    *,
    origin: str,
    origin_id: int,
    reason_code: str,
    last_error: str | None,
    attempts: int,
    now: datetime,
    order_id: str | None = None,
    source: str | None = None,
    correlation_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> DeadLetter:
    dl = await session.scalar(
        select(DeadLetter).where(DeadLetter.origin == origin, DeadLetter.origin_id == origin_id).with_for_update()
    )
    if dl is None:
        dl = DeadLetter(
            origin=origin,
            origin_id=origin_id,
            reason_code=reason_code,
            last_error=last_error,
            attempts=attempts,
            status=DeadLetterStatus.OPEN,
            order_id=order_id,
            source=source,
            correlation_id=correlation_id,
            details=details,
            created_at=now,
            last_failed_at=now,
            times_dead_lettered=1,
        )
        session.add(dl)
    else:
        dl.reason_code = reason_code
        dl.last_error = last_error
        dl.attempts = attempts
        dl.status = DeadLetterStatus.OPEN
        dl.details = details
        dl.last_failed_at = now
        dl.times_dead_lettered += 1
        dl.resolved_at = None
        dl.resolution = None
    await session.flush()
    logger.error(
        "dead-lettered",
        extra={
            "dead_letter_id": dl.id,
            "origin": origin,
            "origin_id": origin_id,
            "reason_code": reason_code,
            "attempts": attempts,
            "order_id": order_id,
            "error": (last_error or "")[:300],
        },
    )
    return dl


async def resolve_dead_letter(
    session: AsyncSession,
    *,
    origin: str,
    origin_id: int,
    resolution: str,
    now: datetime,
    statuses: tuple[str, ...] = (DeadLetterStatus.REPLAYING,),
) -> DeadLetter | None:
    """Close the item's dead letter if it is in one of `statuses` (default: being replayed)."""
    dl = await session.scalar(
        select(DeadLetter).where(DeadLetter.origin == origin, DeadLetter.origin_id == origin_id).with_for_update()
    )
    if dl is None or dl.status not in statuses:
        return None
    dl.status = DeadLetterStatus.RESOLVED
    dl.resolved_at = now
    dl.resolution = resolution
    logger.info(
        "dead letter resolved",
        extra={"dead_letter_id": dl.id, "origin": origin, "origin_id": origin_id, "resolution": resolution},
    )
    return dl
