"""Reconciliation, mismatches, alerts, the summary feed and the HTML dashboard."""

import html
import json
from datetime import datetime, timedelta
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from irlab import __version__
from irlab.api.ops import OpsKey, Session
from irlab.models import (
    Alert,
    DeadLetter,
    InboxEvent,
    InboxStatus,
    Mismatch,
    Order,
    OutboxMessage,
    ReconciliationRun,
    ReplayRequest,
    WebhookReceipt,
)
from irlab.api.ops import origin_record
from irlab.schemas import OperatorAction, OrderOut
from irlab.triage import TriageResult

router = APIRouter(tags=["monitoring"])


class MismatchOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    kind: str
    entity_type: str
    entity_id: str
    order_id: str | None
    severity: str
    expected: dict[str, Any] | None
    actual: dict[str, Any] | None
    status: str
    first_detected_at: datetime
    last_seen_at: datetime
    seen_count: int
    repair: str | None
    resolved_at: datetime | None
    resolution: str | None


class RunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    started_at: datetime
    finished_at: datetime | None
    window_since: datetime | None
    triggered_by: str
    status: str
    orders_checked: int
    payments_checked: int
    accruals_checked: int
    skipped_in_grace: int
    skipped_in_flight: int
    mismatches_found: int
    new_mismatches: int
    resolved_mismatches: int
    repairs_queued: int
    error: str | None


class AlertOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    rule: str
    severity: str
    summary: str
    value: float
    threshold: float
    details: dict[str, Any] | None
    fired_at: datetime
    last_evaluated_at: datetime
    resolved_at: datetime | None


class RunRequest(BaseModel):
    since: datetime | None = Field(default=None, description="Only provider records created at/after this time")
    auto_repair: bool | None = Field(default=None, description="Default: RECONCILE_AUTO_REPAIR")
    requested_by: str = Field(default="api", min_length=1, max_length=64)


def _err(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status, detail={"code": code, "message": message})


# ------------------------------------------------------------ summary


async def _grouped(session: AsyncSession, column, *where) -> dict[str, int]:
    stmt = select(column, func.count()).group_by(column)
    for clause in where:
        stmt = stmt.where(clause)
    return {str(k): v for k, v in (await session.execute(stmt)).all()}


async def build_summary(session: AsyncSession, now: datetime) -> dict[str, Any]:
    hour_ago = now - timedelta(hours=1)
    last_run = await session.scalar(select(ReconciliationRun).order_by(ReconciliationRun.id.desc()).limit(1))
    p50 = p95 = None
    durations = sorted(d for (d,) in (await session.execute(
        select(WebhookReceipt.duration_ms).order_by(WebhookReceipt.id.desc()).limit(5000))).all())
    if durations:
        p50 = durations[int(0.50 * (len(durations) - 1))]
        p95 = durations[int(0.95 * (len(durations) - 1))]
    return {
        "generated_at": now.isoformat(),
        "inbox_by_status": await _grouped(session, InboxEvent.status),
        "receipts_by_outcome": await _grouped(session, WebhookReceipt.outcome),
        "receipts_last_hour_by_outcome": await _grouped(session, WebhookReceipt.outcome,
                                                        WebhookReceipt.received_at >= hour_ago),
        "rejections_by_reason": await _grouped(session, WebhookReceipt.reason, WebhookReceipt.outcome == "rejected"),
        "receiver_latency_ms": {"p50": p50, "p95": p95, "samples": len(durations)},
        "orders_by_status": await _grouped(session, Order.status),
        "orders_by_loyalty_status": await _grouped(session, Order.loyalty_status),
        "outbox_by_status": await _grouped(session, OutboxMessage.status),
        "dead_letters_open_by_reason": await _grouped(session, DeadLetter.reason_code,
                                                      DeadLetter.status.in_(["open", "replaying"])),
        "mismatches_open_by_kind": await _grouped(session, Mismatch.kind, Mismatch.status == "open"),
        "alerts_firing": [a.rule for a in (await session.scalars(
            select(Alert).where(Alert.resolved_at.is_(None)).order_by(Alert.id))).all()],
        "parked_events": (await _grouped(session, InboxEvent.status, InboxEvent.status == InboxStatus.PARKED)).get(
            "parked", 0),
        "replays_by_result": await _grouped(session, ReplayRequest.result),
        "last_reconciliation": RunOut.model_validate(last_run).model_dump(mode="json") if last_run else None,
    }


@router.get("/v1/summary", summary="Counts behind the dashboard (JSON)")
async def summary(request: Request, session: Session):
    return await build_summary(session, request.app.state.runtime.clock.now())


# -------------------------------------------------------- reconciliation


@router.post("/v1/reconciliation/runs", dependencies=[OpsKey], summary="Run reconciliation now")
async def run_reconciliation(body: RunRequest, request: Request):
    return await request.app.state.runtime.reconciler.run(
        since=body.since, triggered_by=f"api:{body.requested_by}", auto_repair=body.auto_repair
    )


@router.get("/v1/reconciliation/runs", response_model=list[RunOut])
async def list_runs(session: Session, limit: Annotated[int, Query(ge=1, le=200)] = 20):
    return (await session.scalars(select(ReconciliationRun).order_by(ReconciliationRun.id.desc()).limit(limit))).all()


@router.get("/v1/mismatches", response_model=list[MismatchOut])
async def list_mismatches(
    session: Session,
    status: str | None = Query(None, description="open | resolved"),
    kind: str | None = None,
    order_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    stmt = select(Mismatch).order_by(Mismatch.id.desc()).limit(limit)
    if status:
        stmt = stmt.where(Mismatch.status == status)
    if kind:
        stmt = stmt.where(Mismatch.kind == kind)
    if order_id:
        stmt = stmt.where(Mismatch.order_id == order_id)
    return (await session.scalars(stmt)).all()


@router.get("/v1/mismatches/{mismatch_id}", response_model=MismatchOut)
async def get_mismatch(mismatch_id: int, session: Session):
    m = await session.get(Mismatch, mismatch_id)
    if m is None:
        raise _err(404, "mismatch_not_found", f"No mismatch {mismatch_id}")
    return m


@router.post("/v1/mismatches/{mismatch_id}/resolve", dependencies=[OpsKey], response_model=MismatchOut,
             summary="Mark a mismatch handled outside the hub (refund issued, points reversed, ...)")
async def resolve_mismatch(mismatch_id: int, body: OperatorAction, request: Request, session: Session):
    m = await session.get(Mismatch, mismatch_id)
    if m is None:
        raise _err(404, "mismatch_not_found", f"No mismatch {mismatch_id}")
    if m.status != "open":
        raise _err(409, "mismatch_not_open", f"Mismatch {mismatch_id} is {m.status}")
    m.status = "resolved"
    m.resolved_at = request.app.state.runtime.clock.now()
    m.resolution = f"manual:{body.requested_by}"[:32]
    await session.commit()
    return m


# ---------------------------------------------------------------- triage


async def _order_brief(session: AsyncSession, order_id: str | None) -> dict[str, Any] | None:
    if not order_id:
        return None
    order = await session.get(Order, order_id)
    if order is None:
        return None
    keep = ("id", "status", "pos_status", "pos_version", "total_minor", "captured_minor", "currency",
            "loyalty_member_id", "loyalty_status", "points_awarded")
    return {k: v for k, v in OrderOut.model_validate(order).model_dump(mode="json").items() if k in keep}


@router.get("/v1/dead-letters/{dead_letter_id}/triage", response_model=TriageResult, tags=["triage"],
            summary="AI-assisted triage of one dead letter (read-only suggestion)")
async def triage_dead_letter(dead_letter_id: int, request: Request, session: Session):
    dl = await session.get(DeadLetter, dead_letter_id)
    if dl is None:
        raise _err(404, "dead_letter_not_found", f"No dead letter {dead_letter_id}")
    record = await origin_record(session, dl)
    if record:
        record = {k: record.get(k) for k in ("kind", "path", "payload", "idempotency_key", "attempts",
                                             "last_http_status", "event_type", "source", "sequence", "resource_id")
                  if record.get(k) is not None}
    context = {
        "type": "dead_letter", "id": dl.id, "reason_code": dl.reason_code, "last_error": dl.last_error,
        "attempts": dl.attempts, "times_dead_lettered": dl.times_dead_lettered, "replay_count": dl.replay_count,
        "status": dl.status, "origin": dl.origin, "order_id": dl.order_id, "source": dl.source,
        "details": dl.details, "origin_record": record, "order": await _order_brief(session, dl.order_id),
    }
    return await request.app.state.runtime.triage.triage(context)


@router.get("/v1/mismatches/{mismatch_id}/triage", response_model=TriageResult, tags=["triage"],
            summary="AI-assisted triage of one mismatch (read-only suggestion)")
async def triage_mismatch(mismatch_id: int, request: Request, session: Session):
    m = await session.get(Mismatch, mismatch_id)
    if m is None:
        raise _err(404, "mismatch_not_found", f"No mismatch {mismatch_id}")
    context = {
        "type": "mismatch", "id": m.id, "kind": m.kind, "severity": m.severity, "entity_type": m.entity_type,
        "entity_id": m.entity_id, "order_id": m.order_id, "expected": m.expected, "actual": m.actual,
        "repair": m.repair, "seen_count": m.seen_count, "status": m.status,
        "order": await _order_brief(session, m.order_id),
    }
    return await request.app.state.runtime.triage.triage(context)


# ---------------------------------------------------------------- alerts


@router.get("/v1/alerts", response_model=list[AlertOut])
async def list_alerts(session: Session, firing: bool | None = None,
                      limit: Annotated[int, Query(ge=1, le=500)] = 50):
    stmt = select(Alert).order_by(Alert.id.desc()).limit(limit)
    if firing is True:
        stmt = stmt.where(Alert.resolved_at.is_(None))
    elif firing is False:
        stmt = stmt.where(Alert.resolved_at.is_not(None))
    return (await session.scalars(stmt)).all()


@router.post("/v1/alerts/evaluate", dependencies=[OpsKey], summary="Evaluate alert rules now")
async def evaluate_alerts(request: Request):
    return await request.app.state.runtime.alerts.evaluate()


# ------------------------------------------------------------- dashboard

_CSS = """
:root{--bg:#f7f7f5;--fg:#1d1d1b;--muted:#6b6b66;--card:#fff;--line:#e3e3de;--ok:#1f7a4d;--warn:#a15c00;--bad:#b42318}
@media (prefers-color-scheme: dark){:root{--bg:#161615;--fg:#ecece8;--muted:#a3a39c;--card:#1f1f1d;--line:#33332f;
--ok:#4cc38a;--warn:#f0a03c;--bad:#f97066}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif}
main{max-width:1200px;margin:0 auto;padding:20px 16px 40px}h1{font-size:20px;margin:0 0 4px}
h2{font-size:15px;margin:28px 0 8px}.muted{color:var(--muted)}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-top:16px}
.tile{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px}
.tile b{display:block;font-size:24px;font-variant-numeric:tabular-nums}.ok b{color:var(--ok)}
.warn b{color:var(--warn)}.bad b{color:var(--bad)}
.scroll{overflow-x:auto;background:var(--card);border:1px solid var(--line);border-radius:8px}
table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:6px 10px;border-bottom:1px solid var(--line);
white-space:nowrap;font-variant-numeric:tabular-nums}th{color:var(--muted);font-weight:600}
code{font-size:12px}.pill{padding:1px 6px;border-radius:9px;border:1px solid var(--line);font-size:12px}
.critical{color:var(--bad)}.warning{color:var(--warn)}
"""


def _e(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _table(headers: list[str], rows: list[list[Any]], empty: str) -> str:
    if not rows:
        return f'<p class="muted">{_e(empty)}</p>'
    head = "".join(f"<th>{_e(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows)
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def _kv(d: dict[str, int]) -> str:
    return ", ".join(f"{_e(k)}: <b>{_e(v)}</b>" for k, v in sorted(d.items())) or '<span class="muted">none</span>'


def _ts(dt: datetime | None) -> str:
    return _e(dt.strftime("%Y-%m-%d %H:%M:%S")) if dt else ""


@router.get("/dashboard", response_class=HTMLResponse, summary="Mismatch dashboard (read-only HTML)")
async def dashboard(request: Request, session: Session):
    now = request.app.state.runtime.clock.now()
    s = await build_summary(session, now)
    mismatches = (await session.scalars(
        select(Mismatch).where(Mismatch.status == "open").order_by(Mismatch.id.desc()).limit(100))).all()
    dead = (await session.scalars(
        select(DeadLetter).where(DeadLetter.status.in_(["open", "replaying"])).order_by(DeadLetter.id.desc())
        .limit(100))).all()
    parked = (await session.scalars(
        select(InboxEvent).where(InboxEvent.status == InboxStatus.PARKED).order_by(InboxEvent.id).limit(50))).all()
    alerts = (await session.scalars(select(Alert).order_by(Alert.id.desc()).limit(20))).all()
    replays = (await session.scalars(select(ReplayRequest).order_by(ReplayRequest.id.desc()).limit(20))).all()

    open_dlq = sum(s["dead_letters_open_by_reason"].values())
    open_mm = sum(s["mismatches_open_by_kind"].values())
    firing = len(s["alerts_firing"])
    pending = s["outbox_by_status"].get("pending", 0) + s["outbox_by_status"].get("in_flight", 0)

    def tile(label: str, value: Any, level: str) -> str:
        return f'<div class="tile {level}"><b>{_e(value)}</b>{_e(label)}</div>'

    tiles = "".join([
        tile("open dead letters", open_dlq, "bad" if open_dlq else "ok"),
        tile("open mismatches", open_mm, "warn" if open_mm else "ok"),
        tile("alerts firing", firing, "bad" if firing else "ok"),
        tile("parked events", s["parked_events"], "warn" if s["parked_events"] else "ok"),
        tile("outbox unsent", pending, "warn" if pending else "ok"),
        tile("orders", sum(s["orders_by_status"].values()), ""),
    ])
    last = s["last_reconciliation"]
    last_line = (f'last run #{_e(last["id"])} at {_e(last["started_at"])}: {_e(last["status"])}, '
                 f'{_e(last["orders_checked"])} orders checked, {_e(last["mismatches_found"])} mismatches, '
                 f'{_e(last["repairs_queued"])} backfills queued' if last else "no run yet")

    mm_rows = [[_e(m.id), f'<span class="{_e(m.severity)}">{_e(m.kind)}</span>', _e(m.order_id or m.entity_id),
                f"<code>{_e(json.dumps(m.expected, ensure_ascii=False))}</code>",
                f"<code>{_e(json.dumps(m.actual, ensure_ascii=False))}</code>", _e(m.repair),
                _ts(m.first_detected_at), _e(m.seen_count)] for m in mismatches]
    dl_rows = [[_e(d.id), _e(d.reason_code), _e(d.origin), _e(d.order_id), _e(d.attempts), _e(d.status),
                _e((d.last_error or "")[:80]), _ts(d.created_at), _e(d.replay_count)] for d in dead]
    parked_rows = [[_e(p.id), _e(p.source), _e(f"{p.resource_type}/{p.resource_id}"), _e(p.event_type),
                    _e(p.sequence), _ts(p.parked_at)] for p in parked]
    alert_rows = [[_e(a.rule), f'<span class="{_e(a.severity)}">{_e(a.severity)}</span>', _e(a.summary),
                   _ts(a.fired_at), _ts(a.resolved_at) or '<span class="pill">firing</span>'] for a in alerts]
    replay_rows = [[_e(r.id), _e(r.dead_letter_id), _e(r.action), _e(r.requested_by), _e(r.result),
                    _ts(r.requested_at), _e((r.note or "")[:60])] for r in replays]
    lat = s["receiver_latency_ms"]
    lat_line = (f"p50 {lat['p50']:.1f} ms · p95 {lat['p95']:.1f} ms over the last {lat['samples']} deliveries"
                if lat["samples"] else "no deliveries yet")

    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="10">
<title>Integration health</title><style>{_CSS}</style></head><body><main>
<h1>Integration health — Demo Bistro ordering hub</h1>
<div class="muted">v{_e(__version__)} · {_e(now.strftime('%Y-%m-%d %H:%M:%S UTC'))} · refreshes every 10 s ·
read-only; act through the operator API (see docs/runbook.md)</div>
<div class="tiles">{tiles}</div>
<h2>Open mismatches</h2><div class="muted">{last_line}</div>
{_table(["#", "kind", "order / entity", "expected", "actual", "repair", "first detected", "seen"], mm_rows,
        "No open mismatches.")}
<h2>Dead-letter queue</h2>
{_table(["#", "reason", "origin", "order", "attempts", "status", "last error", "since", "replays"], dl_rows,
        "Dead-letter queue is empty.")}
<h2>Parked events (waiting for a sequence gap)</h2>
{_table(["inbox #", "source", "resource", "type", "sequence", "parked at"], parked_rows, "Nothing parked.")}
<h2>Alerts</h2>
{_table(["rule", "severity", "summary", "fired", "resolved"], alert_rows, "No alerts yet.")}
<h2>Webhook receipts</h2>
<p>All time: {_kv(s["receipts_by_outcome"])}<br>Last hour: {_kv(s["receipts_last_hour_by_outcome"])}<br>
Rejections: {_kv(s["rejections_by_reason"])}<br>Receiver latency: {_e(lat_line)}</p>
<h2>Pipeline</h2>
<p>Inbox: {_kv(s["inbox_by_status"])}<br>Orders: {_kv(s["orders_by_status"])}<br>
Loyalty: {_kv(s["orders_by_loyalty_status"])}<br>Outbox: {_kv(s["outbox_by_status"])}</p>
<h2>Replay audit</h2>
{_table(["#", "dead letter", "action", "by", "result", "at", "note"], replay_rows, "No replays yet.")}
</main></body></html>"""
    return HTMLResponse(page)
