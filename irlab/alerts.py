"""Alert rules over the hub's own tables, with log and webhook sinks.

Each evaluation computes every rule; a rule that starts firing opens an `alerts`
row and notifies the sinks once, a rule that stops firing resolves it and
notifies again. Re-evaluating a firing rule only updates its value, so sinks are
not spammed every interval.

| rule                 | fires when                                                   | severity           |
|----------------------|--------------------------------------------------------------|--------------------|
| dlq_depth            | open dead letters ≥ ALERT_DLQ_DEPTH                          | warning (≥10: critical) |
| retries_exhausted    | any open dead letter with reason retries_exhausted           | critical           |
| signature_failures   | ≥ ALERT_SIGNATURE_FAILURES bad signatures/timestamps within ALERT_SIGNATURE_WINDOW_S | critical |
| open_mismatches      | open mismatches ≥ ALERT_OPEN_MISMATCHES                      | warning (critical kinds: critical) |
| stale_parked_events  | an event parked longer than ALERT_PARKED_AGE_S               | warning            |
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import httpx
from sqlalchemy import func, select

from irlab.clock import Clock
from irlab.config import Settings
from irlab.db import session_factory
from irlab.models import Alert, DeadLetter, InboxEvent, InboxStatus, Mismatch, WebhookReceipt

logger = logging.getLogger("irlab.alerts")

SIGNATURE_REASONS = ("invalid_signature", "timestamp_out_of_tolerance")


@dataclass
class RuleResult:
    rule: str
    firing: bool
    value: float
    threshold: float
    severity: str
    summary: str
    details: dict[str, Any]


Sink = Callable[[str, Alert], Awaitable[None]]  # (event "firing" | "resolved", alert)


def alert_payload(event: str, alert: Alert) -> dict[str, Any]:
    return {
        "event": event,
        "rule": alert.rule,
        "severity": alert.severity,
        "summary": alert.summary,
        "value": alert.value,
        "threshold": alert.threshold,
        "fired_at": alert.fired_at.isoformat() if alert.fired_at else None,
        "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
        "details": alert.details,
    }


async def log_sink(event: str, alert: Alert) -> None:
    log = logger.error if event == "firing" and alert.severity == "critical" else logger.warning
    if event == "resolved":
        log = logger.info
    log("alert %s: %s", event, alert.rule, extra={"rule": alert.rule, "severity": alert.severity,
                                                     "value": alert.value, "summary": alert.summary})


class WebhookSink:
    """POSTs alert JSON to ALERT_WEBHOOK_URL (chat / on-call tooling). Best effort, never raises."""

    def __init__(self, url: str, timeout_s: float = 3.0, client: httpx.AsyncClient | None = None):
        self.url = url
        self.timeout_s = timeout_s
        self._client = client

    async def __call__(self, event: str, alert: Alert) -> None:
        client = self._client or httpx.AsyncClient(timeout=self.timeout_s)
        try:
            async with asyncio.timeout(self.timeout_s):
                resp = await client.post(self.url, json=alert_payload(event, alert))
            if resp.status_code >= 300:
                logger.warning("alert webhook rejected", extra={"http_status": resp.status_code})
        except Exception as exc:  # noqa: BLE001 - alerting must not break the worker
            logger.warning("alert webhook failed", extra={"error": type(exc).__name__})
        finally:
            if self._client is None:
                await client.aclose()


class AlertEvaluator:
    def __init__(self, settings: Settings, clock: Clock, sinks: list[Sink] | None = None):
        self.settings = settings
        self.clock = clock
        self.sinks: list[Sink] = sinks if sinks is not None else [log_sink]
        if settings.alert_webhook_url and sinks is None:
            self.sinks.append(WebhookSink(settings.alert_webhook_url))

    async def rules(self) -> list[RuleResult]:
        st = self.settings
        now = self.clock.now()
        async with session_factory()() as s:
            dlq = dict((await s.execute(
                select(DeadLetter.reason_code, func.count()).where(DeadLetter.status == "open")
                .group_by(DeadLetter.reason_code))).all())
            window_start = now - timedelta(seconds=st.alert_signature_window_s)
            sig_failures = await s.scalar(
                select(func.count()).select_from(WebhookReceipt).where(
                    WebhookReceipt.outcome == "rejected",
                    WebhookReceipt.reason.in_(SIGNATURE_REASONS),
                    WebhookReceipt.received_at >= window_start,
                )) or 0
            sig_by_source = dict((await s.execute(
                select(WebhookReceipt.source, func.count()).where(
                    WebhookReceipt.outcome == "rejected",
                    WebhookReceipt.reason.in_(SIGNATURE_REASONS),
                    WebhookReceipt.received_at >= window_start,
                ).group_by(WebhookReceipt.source))).all())
            mismatches = dict((await s.execute(
                select(Mismatch.kind, func.count()).where(Mismatch.status == "open").group_by(Mismatch.kind))).all())
            critical_mismatches = await s.scalar(
                select(func.count()).select_from(Mismatch).where(Mismatch.status == "open",
                                                                  Mismatch.severity == "critical")) or 0
            parked_cutoff = now - timedelta(seconds=st.alert_parked_age_s)
            stale_parked = await s.scalar(
                select(func.count()).select_from(InboxEvent).where(
                    InboxEvent.status == InboxStatus.PARKED, InboxEvent.parked_at < parked_cutoff)) or 0

        depth = sum(dlq.values())
        exhausted = dlq.get("retries_exhausted", 0)
        open_mm = sum(mismatches.values())
        return [
            RuleResult("dlq_depth", depth >= st.alert_dlq_depth, depth, st.alert_dlq_depth,
                       "critical" if depth >= 10 else "warning",
                       f"{depth} open dead letter(s)", {"by_reason": dlq}),
            RuleResult("retries_exhausted", exhausted >= 1, exhausted, 1, "critical",
                       f"{exhausted} outbound call(s) exhausted their retry budget (provider outage?)",
                       {"count": exhausted}),
            RuleResult("signature_failures", sig_failures >= st.alert_signature_failures, sig_failures,
                       st.alert_signature_failures, "critical",
                       f"{sig_failures} webhook signature/timestamp failures in {st.alert_signature_window_s}s "
                       "(secret rotation gone wrong, clock skew, or forged traffic)",
                       {"by_source": sig_by_source, "window_s": st.alert_signature_window_s}),
            RuleResult("open_mismatches", open_mm >= st.alert_open_mismatches, open_mm, st.alert_open_mismatches,
                       "critical" if critical_mismatches else "warning",
                       f"{open_mm} open reconciliation mismatch(es)", {"by_kind": mismatches}),
            RuleResult("stale_parked_events", stale_parked >= 1, stale_parked, 1, "warning",
                       f"{stale_parked} event(s) parked longer than {st.alert_parked_age_s}s waiting for a sequence gap",
                       {"count": stale_parked}),
        ]

    async def evaluate(self) -> list[dict[str, Any]]:
        now = self.clock.now()
        results = await self.rules()
        notifications: list[tuple[str, Alert]] = []
        async with session_factory()() as s, s.begin():
            open_alerts = {a.rule: a for a in (await s.scalars(select(Alert).where(Alert.resolved_at.is_(None)))).all()}
            for r in results:
                current = open_alerts.get(r.rule)
                if r.firing and current is None:
                    alert = Alert(rule=r.rule, severity=r.severity, summary=r.summary, value=r.value,
                                  threshold=r.threshold, details=r.details, fired_at=now, last_evaluated_at=now)
                    s.add(alert)
                    notifications.append(("firing", alert))
                elif r.firing and current is not None:
                    current.value, current.summary, current.details = r.value, r.summary, r.details
                    current.severity = r.severity
                    current.last_evaluated_at = now
                elif current is not None:
                    current.resolved_at = now
                    current.last_evaluated_at = now
                    current.value = r.value
                    notifications.append(("resolved", current))
        for event, alert in notifications:
            for sink in self.sinks:
                try:
                    await sink(event, alert)
                except Exception:  # noqa: BLE001
                    logger.exception("alert sink failed")
        return [{"rule": r.rule, "firing": r.firing, "value": r.value, "threshold": r.threshold,
                 "severity": r.severity, "summary": r.summary} for r in results]
