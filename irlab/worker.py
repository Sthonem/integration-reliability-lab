"""Background worker: inbox processor, outbox dispatcher, parked-event timeouts,
and (when configured) periodic reconciliation and alert evaluation.

Runs inside the API process by default (RUN_WORKER=true) or standalone:

    python -m irlab.worker

Claims are compare-and-set updates with leases, so several workers (or a worker
plus an operator's inline replay) never send the same outbox message twice at
the same time; the idempotency key covers the rest.
"""

import asyncio
import contextlib
import logging
import signal
import time
from typing import Any, Protocol

from sqlalchemy import func, select

from irlab.config import Settings
from irlab.db import session_factory
from irlab.models import InboxEvent, InboxStatus, OutboxMessage, OutboxStatus
from irlab.outbox import Dispatcher
from irlab.processing import Processor

logger = logging.getLogger("irlab.worker")


class Periodic(Protocol):
    async def __call__(self) -> Any: ...


class Worker:
    def __init__(
        self,
        settings: Settings,
        processor: Processor,
        dispatcher: Dispatcher,
        reconcile: Periodic | None = None,
        evaluate_alerts: Periodic | None = None,
    ):
        self.settings = settings
        self.processor = processor
        self.dispatcher = dispatcher
        self.reconcile = reconcile
        self.evaluate_alerts = evaluate_alerts
        self._last_reconcile = time.monotonic()
        self._last_alerts = 0.0

    async def tick(self, *, periodic: bool = True) -> dict[str, int]:
        totals = {"processed": 0, "dispatched": 0, "expired_parked": 0, "recovered": 0}
        totals["recovered"] += await self.processor.recover_expired_claims()
        # Processing can queue outbox messages; sending them can trigger webhooks
        # (e.g. points.accrued) that need processing. A few rounds settle a chain.
        for _ in range(5):
            processed = await self.processor.run_once()
            dispatched = await self.dispatcher.run_once()
            totals["processed"] += processed
            totals["dispatched"] += dispatched
            if not processed and not dispatched:
                break
        totals["expired_parked"] = await self.processor.expire_parked()
        if periodic:
            await self._periodic()
        return totals

    async def _periodic(self) -> None:
        now = time.monotonic()
        interval = self.settings.reconcile_interval_s
        if self.reconcile is not None and interval > 0 and now - self._last_reconcile >= interval:
            self._last_reconcile = now
            try:
                await self.reconcile()
            except Exception:
                logger.exception("periodic reconciliation failed")
        if self.evaluate_alerts is not None and now - self._last_alerts >= self.settings.alert_interval_s:
            self._last_alerts = now
            try:
                await self.evaluate_alerts()
            except Exception:
                logger.exception("alert evaluation failed")

    async def backlog(self) -> dict[str, int]:
        async with session_factory()() as s:
            received = await s.scalar(
                select(func.count()).select_from(InboxEvent).where(
                    InboxEvent.status.in_([InboxStatus.RECEIVED, InboxStatus.PROCESSING])
                )
            )
            pending = await s.scalar(
                select(func.count()).select_from(OutboxMessage).where(
                    OutboxMessage.status.in_([OutboxStatus.PENDING, OutboxStatus.IN_FLIGHT])
                )
            )
        return {"inbox_unprocessed": received or 0, "outbox_unsent": pending or 0}

    async def drain(self, max_wait_s: float = 10.0) -> dict[str, int]:
        """Tick until nothing is waiting (waiting out backoff delays), or until max_wait_s."""
        deadline = time.monotonic() + max_wait_s
        totals: dict[str, int] = {}
        while True:
            for k, v in (await self.tick(periodic=False)).items():
                totals[k] = totals.get(k, 0) + v
            backlog = await self.backlog()
            if not backlog["inbox_unprocessed"] and not backlog["outbox_unsent"]:
                return totals | backlog
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return totals | backlog
            due = await self.dispatcher.next_due_in()
            await asyncio.sleep(min(remaining, max(due or 0.0, 0.01), 0.5))

    async def run_forever(self, stop: asyncio.Event) -> None:
        logger.info("worker started", extra={"poll_interval_s": self.settings.worker_poll_interval_s})
        while not stop.is_set():
            try:
                await self.tick()
            except Exception:
                logger.exception("worker tick failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.settings.worker_poll_interval_s)
        logger.info("worker stopped")


async def _main() -> None:
    from irlab.config import get_settings
    from irlab.db import configure_engine, dispose_engine, init_db
    from irlab.logs import configure_logging
    from irlab.runtime import build_runtime

    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    configure_engine(settings.database_url)
    if settings.db_auto_create:
        await init_db()
    runtime = build_runtime(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await runtime.worker.run_forever(stop)
    finally:
        await runtime.aclose()
        await dispose_engine()


if __name__ == "__main__":
    asyncio.run(_main())
