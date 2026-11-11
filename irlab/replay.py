"""Operator replay and discard of dead letters, with an audit trail.

A replay resets the failed item (outbox message → `pending` with a fresh retry
budget and the *same* idempotency key; inbox event → `received`) and runs one
attempt immediately, so the operator sees the outcome in the response:

* `succeeded`        — the item went through; dead letter resolved
* `failed_again`     — same or new permanent error; dead letter reopened
* `retry_scheduled`  — a retryable error; the dispatcher keeps trying, and the
                       dead letter resolves or reopens when it finishes

Inbox events dead-lettered by `sequence_gap_timeout` are replayed with
`skip_sequence_check`: the operator has decided the missing version is not
coming and the event (a full snapshot) should be applied as is.

Every replay and discard writes a `replay_requests` row (who, when, why, result).
"""

import logging
from dataclasses import asdict, dataclass

from sqlalchemy import select

from irlab.clock import Clock
from irlab.db import session_factory
from irlab.models import DeadLetter, DeadLetterStatus, InboxEvent, InboxStatus, OutboxMessage, ReplayRequest
from irlab.outbox import Dispatcher, requeue_outbox
from irlab.processing import Processor

logger = logging.getLogger("irlab.replay")


class ReplayError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


@dataclass
class ReplayOutcome:
    dead_letter_id: int
    replay_request_id: int
    result: str
    dead_letter_status: str
    reason_code: str
    detail: str | None = None

    def as_dict(self) -> dict:
        return asdict(self)


class ReplayService:
    def __init__(self, processor: Processor, dispatcher: Dispatcher, clock: Clock):
        self.processor = processor
        self.dispatcher = dispatcher
        self.clock = clock

    async def replay(self, dead_letter_id: int, requested_by: str, note: str | None = None) -> ReplayOutcome:
        now = self.clock.now()
        async with session_factory()() as s, s.begin():
            dl = await s.get(DeadLetter, dead_letter_id, with_for_update=True)
            if dl is None:
                raise ReplayError(404, "dead_letter_not_found", f"No dead letter {dead_letter_id}")
            if dl.status != DeadLetterStatus.OPEN:
                raise ReplayError(409, "dead_letter_not_open", f"Dead letter {dead_letter_id} is {dl.status}")
            dl.status = DeadLetterStatus.REPLAYING
            dl.replay_count += 1
            origin, origin_id = dl.origin, dl.origin_id
            if origin == "outbox":
                msg = await s.get(OutboxMessage, origin_id, with_for_update=True)
                if msg is None:
                    raise ReplayError(409, "origin_missing", "Outbox message no longer exists")
                await requeue_outbox(s, msg, now)
            else:
                ev = await s.get(InboxEvent, origin_id, with_for_update=True)
                if ev is None:
                    raise ReplayError(409, "origin_missing", "Inbox event no longer exists")
                ev.status = InboxStatus.RECEIVED
                ev.processing_attempts = 0
                ev.claimed_until = None
                ev.last_error = None
                if dl.reason_code == "sequence_gap_timeout":
                    ev.skip_sequence_check = True
            rr = ReplayRequest(
                dead_letter_id=dl.id, action="replay", requested_by=requested_by, note=note, requested_at=now
            )
            s.add(rr)
            await s.flush()
            rr_id = rr.id

        logger.info(
            "replay requested",
            extra={"dead_letter_id": dead_letter_id, "origin": origin, "origin_id": origin_id, "by": requested_by},
        )
        if origin == "outbox":
            await self.dispatcher.dispatch_one(origin_id)
        else:
            await self.processor.process_one(origin_id)
            await self.processor.run_once()  # events woken from parking by this one

        async with session_factory()() as s, s.begin():
            dl = await s.get(DeadLetter, dead_letter_id)
            rr = await s.get(ReplayRequest, rr_id)
            assert dl is not None and rr is not None
            if dl.status == DeadLetterStatus.RESOLVED:
                result, detail = "succeeded", dl.resolution
            elif dl.status == DeadLetterStatus.OPEN:
                result, detail = "failed_again", f"{dl.reason_code}: {dl.last_error}"
            else:
                result, detail = "retry_scheduled", "retryable error; the dispatcher keeps retrying"
            rr.result = result
            rr.result_detail = detail
            outcome = ReplayOutcome(dl.id, rr_id, result, dl.status, dl.reason_code, detail)
        logger.info("replay %s", result, extra={"dead_letter_id": dead_letter_id, "detail": detail})
        return outcome

    async def replay_bulk(
        self, requested_by: str, reason_code: str | None = None, note: str | None = None, limit: int = 100
    ) -> dict:
        async with session_factory()() as s:
            stmt = select(DeadLetter.id).where(DeadLetter.status == DeadLetterStatus.OPEN).order_by(DeadLetter.id)
            if reason_code:
                stmt = stmt.where(DeadLetter.reason_code == reason_code)
            ids = (await s.scalars(stmt.limit(limit))).all()
        results = []
        for dl_id in ids:
            try:
                results.append((await self.replay(dl_id, requested_by, note)).as_dict())
            except ReplayError as exc:  # e.g. replayed concurrently by someone else
                results.append({"dead_letter_id": dl_id, "result": "not_replayed", "detail": exc.code})
        summary: dict = {"requested": len(ids), "reason_code": reason_code}
        for r in results:
            summary[r["result"]] = summary.get(r["result"], 0) + 1
        summary["results"] = results
        return summary

    async def discard(self, dead_letter_id: int, requested_by: str, note: str | None = None) -> ReplayOutcome:
        now = self.clock.now()
        async with session_factory()() as s, s.begin():
            dl = await s.get(DeadLetter, dead_letter_id, with_for_update=True)
            if dl is None:
                raise ReplayError(404, "dead_letter_not_found", f"No dead letter {dead_letter_id}")
            if dl.status != DeadLetterStatus.OPEN:
                raise ReplayError(409, "dead_letter_not_open", f"Dead letter {dead_letter_id} is {dl.status}")
            dl.status = DeadLetterStatus.DISCARDED
            dl.resolved_at = now
            dl.resolution = "discarded"
            rr = ReplayRequest(
                dead_letter_id=dl.id, action="discard", requested_by=requested_by, note=note, requested_at=now,
                result="discarded", result_detail=note,
            )
            s.add(rr)
            await s.flush()
            outcome = ReplayOutcome(dl.id, rr.id, "discarded", dl.status, dl.reason_code, note)
        logger.warning("dead letter discarded", extra={"dead_letter_id": dead_letter_id, "by": requested_by})
        return outcome
