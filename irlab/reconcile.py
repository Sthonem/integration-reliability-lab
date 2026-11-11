"""Reconciliation: compare the hub with what each provider's REST API says.

Webhooks are a notification channel, not a ledger: they get dropped, delayed or
lost in a deploy. The reconciler periodically reads the providers' own records
and records typed **mismatches** (one open row per kind + entity; re-detection
updates it, disappearance resolves it).

| kind                    | meaning                                                  | repair            |
|-------------------------|----------------------------------------------------------|-------------------|
| missing_in_hub          | POS has an order the hub never received                  | backfill snapshot |
| status_mismatch         | POS is at a newer version/status than the hub            | backfill snapshot |
| payment_missing_in_hub  | provider has a payment the hub never received            | backfill snapshot |
| payment_status_mismatch | provider's payment is newer than the hub's               | backfill snapshot |
| partial_payment         | captured < order total                                    | manual            |
| overpayment             | captured > order total (e.g. charged twice)               | manual (refund)   |
| orphan_payment          | payment for an order no system knows                      | manual            |
| duplicate_accrual       | more than one accrual for one order (double points)       | manual (reverse)  |
| points_mismatch         | accrued points ≠ policy (1 point per full TRY)            | manual            |
| points_on_unpaid_order  | points for a cancelled or not fully paid order            | manual (reverse)  |
| points_missing          | hub says accrued, provider has no accrual                 | manual            |

Backfill writes the provider's current snapshot into the inbox (origin
`reconciliation`, `skip_sequence_check`), so it flows through the same
processor as webhooks. The event id `backfill:<entity>:v<version>` is
deterministic: repeated runs never queue the same snapshot twice.

Orders younger than `reconcile_grace_s` are skipped: their webhooks may still
be in flight, and flagging them would be noise.
"""

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select

from irlab.clock import Clock
from irlab.config import Settings
from irlab.db import session_factory
from irlab.models import (
    InboxEvent,
    InboxStatus,
    Mismatch,
    Order,
    OrderPayment,
    ReconciliationRun,
)
from irlab.providers import ProviderClients, ProviderNotConfigured

logger = logging.getLogger("irlab.reconcile")

CAPTURED = ("captured", "partially_captured")
CRITICAL_KINDS = {"missing_in_hub", "duplicate_accrual", "overpayment", "points_missing"}
FETCH_LIMIT = 1000


@dataclass
class Finding:
    kind: str
    entity_type: str
    entity_id: str
    order_id: str | None
    expected: dict[str, Any] | None
    actual: dict[str, Any] | None
    repair: str | None = None  # backfill_order | backfill_payment
    snapshot: dict[str, Any] | None = None

    @property
    def severity(self) -> str:
        return "critical" if self.kind in CRITICAL_KINDS else "warning"


@dataclass
class ProviderData:
    orders: list[dict[str, Any]] | None = None
    payments: list[dict[str, Any]] | None = None
    accruals: list[dict[str, Any]] | None = None
    errors: dict[str, str] = field(default_factory=dict)


def _dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


class Reconciler:
    def __init__(self, settings: Settings, providers: ProviderClients, clock: Clock):
        self.settings = settings
        self.providers = providers
        self.clock = clock
        self._lock = asyncio.Lock()

    async def run(
        self, *, since: datetime | None = None, triggered_by: str = "worker", auto_repair: bool | None = None
    ) -> dict[str, Any]:
        async with self._lock:  # one run at a time per process
            return await self._run(since, triggered_by, self.settings.reconcile_auto_repair if auto_repair is None
                                   else auto_repair)

    async def _run(self, since: datetime | None, triggered_by: str, auto_repair: bool) -> dict[str, Any]:
        started = self.clock.now()
        async with session_factory()() as s:
            run = ReconciliationRun(started_at=started, window_since=since, triggered_by=triggered_by[:64])
            s.add(run)
            await s.commit()
            run_id = run.id

        data = await self._fetch(since)
        now = self.clock.now()
        cutoff = now - timedelta(seconds=self.settings.reconcile_grace_s)

        async with session_factory()() as s, s.begin():
            run = await s.get(ReconciliationRun, run_id)
            assert run is not None
            findings, scope, skipped, in_flight = await self._compare(s, data, cutoff)
            new, resolved = await self._record(s, run_id, findings, scope, now, auto_repair)
            repairs = await self._repair(s, findings, now) if auto_repair else 0
            run.finished_at = self.clock.now()
            run.orders_checked = len([o for o in data.orders or [] if o["id"] in scope])
            run.payments_checked = len(data.payments or [])
            run.accruals_checked = len(data.accruals or [])
            run.skipped_in_grace = skipped
            run.skipped_in_flight = in_flight
            run.mismatches_found = len(findings)
            run.new_mismatches = new
            run.resolved_mismatches = resolved
            run.repairs_queued = repairs
            run.status = "partial" if data.errors else "ok"
            run.error = json.dumps(data.errors) if data.errors else None
            summary = {
                "run_id": run_id,
                "status": run.status,
                "orders_checked": run.orders_checked,
                "payments_checked": run.payments_checked,
                "accruals_checked": run.accruals_checked,
                "skipped_in_grace": skipped,
                "skipped_in_flight": in_flight,
                "mismatches_found": len(findings),
                "new_mismatches": new,
                "resolved_mismatches": resolved,
                "repairs_queued": repairs,
                "by_kind": _count(f.kind for f in findings),
                "errors": data.errors,
            }
        log = logger.warning if findings or data.errors else logger.info
        log("reconciliation run finished", extra={k: v for k, v in summary.items() if k != "by_kind"}
            | {"by_kind": json.dumps(summary["by_kind"])})
        return summary

    # ----------------------------------------------------------- fetch

    async def _fetch(self, since: datetime | None) -> ProviderData:
        data = ProviderData()
        params: dict[str, Any] = {"limit": FETCH_LIMIT}
        if since is not None:
            params["created_after"] = since.isoformat()
        timeout = max(self.settings.outbound_timeout_s * 5, 5.0)

        async def get(provider: str, path: str) -> list[dict[str, Any]] | None:
            try:
                client = self.providers.get(provider)
                async with asyncio.timeout(timeout):
                    resp = await client.get(path, params=params)
                resp.raise_for_status()
                body = resp.json()
                if not isinstance(body, list):
                    raise ValueError("expected a JSON list")
                return body
            except (ProviderNotConfigured, TimeoutError, ValueError, Exception) as exc:  # noqa: BLE001
                data.errors[provider] = f"{type(exc).__name__}: {exc}"[:300]
                logger.warning("reconciliation could not read provider", extra={"provider": provider,
                                                                              "error": data.errors[provider]})
                return None

        data.orders, data.payments, data.accruals = await asyncio.gather(
            get("pos", "/orders"), get("payments", "/payments"), get("loyalty", "/accruals")
        )
        return data

    # ----------------------------------------------------------- compare

    async def _compare(
        self, s, data: ProviderData, cutoff: datetime
    ) -> tuple[list[Finding], set[str], int, int]:
        findings: list[Finding] = []
        scope: set[str] = set()  # entity ids whose state this run actually checked
        skipped = 0
        # Resources with an event already in our inbox but not applied yet: the hub
        # knows, it just has not caught up. Not a mismatch (yet).
        in_flight_keys = set(
            (await s.execute(
                select(InboxEvent.source, InboxEvent.resource_id).where(
                    InboxEvent.status.in_([InboxStatus.RECEIVED, InboxStatus.PROCESSING, InboxStatus.PARKED])
                )
            )).all()
        )
        in_flight = 0

        pos_orders = {o["id"]: o for o in data.orders or []}
        settled = {}
        for oid, o in pos_orders.items():
            created = _dt(o.get("created_at"))
            if created is not None and created > cutoff:
                skipped += 1
                continue
            settled[oid] = o
        payments = data.payments or []
        accruals = data.accruals or []

        hub_orders: dict[str, Order] = {}
        ref_ids = set(settled) | {p["order_ref"] for p in payments} | {a["order_ref"] for a in accruals}
        if ref_ids:
            for row in (await s.scalars(select(Order).where(Order.id.in_(ref_ids)))).all():
                hub_orders[row.id] = row
        hub_payments: dict[str, OrderPayment] = {}
        if payments:
            ids = [p["id"] for p in payments]
            for row in (await s.scalars(select(OrderPayment).where(OrderPayment.payment_id.in_(ids)))).all():
                hub_payments[row.payment_id] = row

        # --- orders: hub vs POS
        if data.orders is not None:
            for oid, o in settled.items():
                h = hub_orders.get(oid)
                if ("pos", oid) in in_flight_keys:
                    in_flight += 1
                    continue
                scope.add(oid)
                if h is None:
                    findings.append(Finding(
                        "missing_in_hub", "order", oid, oid,
                        expected={"pos_status": o["status"], "version": o["version"], "total_minor": o["total_minor"]},
                        actual=None, repair="backfill_order", snapshot=o))
                elif h.pos_version < o["version"] or h.pos_status != o["status"]:
                    findings.append(Finding(
                        "status_mismatch", "order", oid, oid,
                        expected={"pos_status": o["status"], "version": o["version"]},
                        actual={"pos_status": h.pos_status, "version": h.pos_version},
                        repair="backfill_order", snapshot=o))

        # --- payments: hub vs provider, and money vs order total
        by_order: dict[str, list[dict[str, Any]]] = {}
        if data.payments is not None:
            for p in payments:
                created = _dt(p.get("created_at"))
                if created is not None and created > cutoff:
                    continue
                by_order.setdefault(p["order_ref"], []).append(p)
                if ("payments", p["id"]) in in_flight_keys:
                    in_flight += 1
                    continue
                scope.add(p["id"])
                hp = hub_payments.get(p["id"])
                if hp is None:
                    findings.append(Finding(
                        "payment_missing_in_hub", "payment", p["id"], p["order_ref"],
                        expected={"status": p["status"], "version": p["version"], "captured_minor": p["captured_minor"]},
                        actual=None, repair="backfill_payment", snapshot=p))
                elif hp.version < p["version"] or hp.status != p["status"]:
                    findings.append(Finding(
                        "payment_status_mismatch", "payment", p["id"], p["order_ref"],
                        expected={"status": p["status"], "version": p["version"]},
                        actual={"status": hp.status, "version": hp.version},
                        repair="backfill_payment", snapshot=p))
                if p["order_ref"] not in pos_orders and p["order_ref"] not in hub_orders and data.orders is not None:
                    findings.append(Finding(
                        "orphan_payment", "payment", p["id"], p["order_ref"],
                        expected={"order_exists": True}, actual={"order_ref": p["order_ref"]}))

        accruals_by_order: dict[str, list[dict[str, Any]]] = {}
        for a in accruals:
            accruals_by_order.setdefault(a["order_ref"], []).append(a)

        for oid, o in settled.items():
            total = o["total_minor"]
            captured = sum(p["captured_minor"] for p in by_order.get(oid, []) if p["status"] in CAPTURED)
            cancelled = o["status"] == "cancelled"
            if data.payments is not None and not cancelled:
                if 0 < captured < total:
                    findings.append(Finding("partial_payment", "order", oid, oid,
                                            expected={"captured_minor": total}, actual={"captured_minor": captured}))
                elif captured > total:
                    findings.append(Finding("overpayment", "order", oid, oid,
                                            expected={"captured_minor": total}, actual={"captured_minor": captured,
                                            "payments": [p["id"] for p in by_order.get(oid, [])]}))
            if data.accruals is None:
                continue
            accs = accruals_by_order.get(oid, [])
            expected_points = total // 100
            if len(accs) > 1:
                findings.append(Finding("duplicate_accrual", "order", oid, oid,
                                        expected={"accruals": 1, "points": expected_points},
                                        actual={"accruals": len(accs), "points": sum(a["points"] for a in accs),
                                                "accrual_ids": [a["id"] for a in accs]}))
            elif len(accs) == 1:
                fully_paid = data.payments is not None and captured >= total
                if cancelled or (data.payments is not None and not fully_paid):
                    findings.append(Finding("points_on_unpaid_order", "order", oid, oid,
                                            expected={"points": 0},
                                            actual={"points": accs[0]["points"], "pos_status": o["status"],
                                                    "captured_minor": captured}))
                elif accs[0]["points"] != expected_points:
                    findings.append(Finding("points_mismatch", "order", oid, oid,
                                            expected={"points": expected_points}, actual={"points": accs[0]["points"]}))
            else:
                h = hub_orders.get(oid)
                if h is not None and h.loyalty_status == "accrued":
                    findings.append(Finding("points_missing", "order", oid, oid,
                                            expected={"points": expected_points}, actual={"accruals": 0}))
        return findings, scope, skipped, in_flight

    # ----------------------------------------------------------- persist

    async def _record(
        self, s, run_id: int, findings: list[Finding], scope: set[str], now: datetime, auto_repair: bool
    ) -> tuple[int, int]:
        open_rows = (await s.scalars(select(Mismatch).where(Mismatch.status == "open"))).all()
        existing = {(m.kind, m.entity_id): m for m in open_rows}
        found_keys = set()
        new = 0
        for f in findings:
            key = (f.kind, f.entity_id)
            found_keys.add(key)
            m = existing.get(key)
            if m is None:
                s.add(Mismatch(kind=f.kind, entity_type=f.entity_type, entity_id=f.entity_id, order_id=f.order_id,
                               severity=f.severity, expected=f.expected, actual=f.actual, status="open",
                               first_run_id=run_id, last_run_id=run_id, first_detected_at=now, last_seen_at=now,
                               seen_count=1,
                               repair=("backfill_queued" if auto_repair else "backfill_available") if f.repair
                               else "manual"))
                new += 1
                logger.warning("mismatch detected", extra={"kind": f.kind, "entity_id": f.entity_id,
                                                           "order_id": f.order_id, "severity": f.severity})
            else:
                m.expected, m.actual = f.expected, f.actual
                m.last_seen_at = now
                m.last_run_id = run_id
                m.seen_count += 1
                if f.repair and auto_repair:
                    m.repair = "backfill_queued"
        resolved = 0
        for key, m in existing.items():
            if key not in found_keys and m.entity_id in scope:
                m.status = "resolved"
                m.resolved_at = now
                m.resolution = "repaired" if m.repair == "backfill_queued" else "cleared"
                resolved += 1
        return new, resolved

    async def _repair(self, s, findings: list[Finding], now: datetime) -> int:
        queued = 0
        for f in findings:
            if f.repair is None or f.snapshot is None:
                continue
            snap = f.snapshot
            source, rtype, etype = (("pos", "order", "order.snapshot") if f.repair == "backfill_order"
                                    else ("payments", "payment", "payment.snapshot"))
            event_id = f"backfill:{snap['id']}:v{snap['version']}"
            exists = await s.scalar(select(InboxEvent.id).where(InboxEvent.source == source,
                                                                  InboxEvent.event_id == event_id))
            if exists is not None:
                continue
            payload = {
                "id": event_id,
                "type": etype,
                "occurred_at": snap.get("updated_at") or snap.get("created_at") or now.isoformat(),
                "resource": {"type": rtype, "id": snap["id"]},
                "sequence": snap["version"],
                "data": snap,
            }
            canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            s.add(InboxEvent(
                source=source, event_id=event_id, event_type=etype, resource_type=rtype, resource_id=snap["id"],
                sequence=snap["version"], occurred_at=_dt(payload["occurred_at"]) or now, received_at=now,
                last_received_at=now, delivery_count=1, conflicting_deliveries=0, payload=payload,
                payload_sha256=hashlib.sha256(canonical.encode()).hexdigest(), status=InboxStatus.RECEIVED,
                origin="reconciliation", skip_sequence_check=True,
            ))
            queued += 1
            logger.info("backfill queued", extra={"event_id": event_id, "kind": f.kind})
        return queued


def _count(items) -> dict[str, int]:
    out: dict[str, int] = {}
    for item in items:
        out[item] = out.get(item, 0) + 1
    return out
