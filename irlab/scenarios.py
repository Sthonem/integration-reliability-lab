"""Failure-injection scenario runner for the six required failure scenarios.

    python -m irlab.scenarios                     # in-process, fresh SQLite hub per scenario
    python -m irlab.scenarios --target http       # against Docker Compose (see docs/docker.md)
    python -m irlab.scenarios --orders 50 --markdown docs/scenario-results.md --json results.json

Every scenario injects its fault through the mocks' control plane, drives
traffic through the public APIs only (POS, payments, loyalty, hub), then checks
the expected outcome and records metrics. Nothing here reaches into the hub's
database, so the same code runs in-process and against the Compose stack.

Scenarios:

1. duplicate_webhook  — every webhook delivered 2-3×
2. timeout            — loyalty does the work, answers after the hub's deadline
                        (plus a naive client with fresh keys per retry, for contrast)
3. partial_payment    — half the orders captured at 70 %
4. out_of_order       — cancellations before creations, payments before orders,
                        and gaps that never close
5. missing_customer   — orders for a member deleted at the loyalty platform,
                        then the record is restored and the DLQ replayed
6. data_mismatch      — dropped webhooks, double charges and double points,
                        found by reconciliation
"""

import argparse
import asyncio
import contextlib
import json
import os
import platform
import secrets
import statistics
import sys
import tempfile
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from irlab import __version__
from irlab.mocks.seed import loyalty_members

LATTE_AND_SIMIT = [{"sku": "SKU-LATTE", "qty": 1}, {"sku": "SKU-SIMIT", "qty": 2}]  # 165.00 TRY
MEMBERS = ("LM-1001", "LM-1002", "LM-1003")
MISSING_MEMBER = "LM-1005"


# ------------------------------------------------------------------ target


@dataclass
class Target:
    hub: httpx.AsyncClient
    mocks: dict[str, httpx.AsyncClient]
    ops_key: str
    mode: str
    config: dict[str, Any] = field(default_factory=dict)
    database: str = ""
    idempotency_store: str = ""

    @property
    def ops(self) -> dict[str, str]:
        return {"X-Ops-Key": self.ops_key}

    async def load_config(self) -> None:
        health = (await self.hub.get("/health")).json()
        self.config = health["worker_config"]
        self.idempotency_store = health["idempotency_store"]["backend"]

    async def drain(self, max_wait_s: float = 30.0) -> dict[str, Any]:
        resp = await self.hub.post("/v1/worker/tick", params={"drain": "true", "max_wait_s": max_wait_s},
                                   headers=self.ops, timeout=max_wait_s + 30)
        resp.raise_for_status()
        return resp.json()

    async def reconcile(self, since: datetime) -> dict[str, Any]:
        grace = float(self.config.get("reconcile_grace_s", 0))
        if grace:
            await asyncio.sleep(grace + 0.2)
        resp = await self.hub.post("/v1/reconciliation/runs", json={"since": since.isoformat(),
                                                                    "requested_by": "scenario-runner"},
                                   headers=self.ops, timeout=60)
        resp.raise_for_status()
        return resp.json()

    async def faults(self, provider: str, **faults: Any) -> None:
        resp = await self.mocks[provider].put("/_admin/faults", json=faults)
        resp.raise_for_status()

    async def reset_faults(self) -> None:
        for client in self.mocks.values():
            (await client.delete("/_admin/faults")).raise_for_status()


# Per-scenario settings for in-process runs. Only the timeout scenario needs a short
# deadline; elsewhere a tight deadline would turn CPU hiccups into accidental timeouts.
IN_PROCESS_OVERRIDES: dict[str, dict[str, Any]] = {"timeout": {"outbound_timeout_s": 0.25}}


@asynccontextmanager
async def in_process_target(overrides: dict[str, Any] | None = None, verbose: bool = False) -> AsyncIterator[Target]:
    """A fresh hub (temp SQLite, in-memory idempotency store, mocks mounted) per call."""
    from irlab.config import Settings
    from irlab.main import create_app

    with tempfile.TemporaryDirectory() as tmp:
        ops_key = secrets.token_urlsafe(24)
        settings = Settings(
            _env_file=None,
            database_url=f"sqlite+aiosqlite:///{Path(tmp) / 'scenario.db'}",
            redis_url="",
            pos_webhook_secrets=secrets.token_urlsafe(32),
            payments_webhook_secrets=secrets.token_urlsafe(32),
            loyalty_webhook_secrets=secrets.token_urlsafe(32),
            ops_api_key=ops_key,
            mount_mocks=True,
            run_worker=False,  # the runner drives the worker through POST /v1/worker/tick
            **({
                "park_timeout_s": 1.0,
                "outbound_timeout_s": 2.0,
                "retry_base_s": 0.05,
                "retry_cap_s": 0.5,
                "retry_max_attempts": 5,
                "reconcile_grace_s": 0,
                "log_level": "INFO" if verbose else "CRITICAL",
                "log_format": "kv",
            } | (overrides or {})),
        )
        app = create_app(settings)
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://hub") as hub:
                mocks = {name: httpx.AsyncClient(transport=transport, base_url=f"http://hub/mock/{name}")
                         for name in ("pos", "payments", "loyalty")}
                try:
                    target = Target(hub, mocks, ops_key, "in-process", database="SQLite (aiosqlite)")
                    await target.load_config()
                    yield target
                finally:
                    for client in mocks.values():
                        await client.aclose()


@asynccontextmanager
async def http_target(hub_url: str, pos_url: str, payments_url: str, loyalty_url: str,
                      ops_key: str) -> AsyncIterator[Target]:
    timeout = httpx.Timeout(30.0)
    async with httpx.AsyncClient(base_url=hub_url, timeout=timeout) as hub:
        mocks = {name: httpx.AsyncClient(base_url=url, timeout=timeout)
                 for name, url in (("pos", pos_url), ("payments", payments_url), ("loyalty", loyalty_url))}
        try:
            target = Target(hub, mocks, ops_key, "http", database="PostgreSQL (via the hub)")
            await target.load_config()
            await target.reset_faults()
            yield target
        finally:
            for client in mocks.values():
                await client.aclose()


# ----------------------------------------------------------------- helpers


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class ScenarioResult:
    name: str
    title: str
    orders: int
    checks: list[Check]
    metrics: dict[str, Any]
    duration_s: float
    receiver_latency_ms: dict[str, Any]

    @property
    def passed(self) -> bool:
        return all(c.ok for c in self.checks)


def _dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _ms(later: str, earlier: str) -> float:
    return round((_dt(later) - _dt(earlier)).total_seconds() * 1000, 1)


def _pct(numerator: float, denominator: float) -> float | None:
    return round(100.0 * numerator / denominator, 1) if denominator else None


def _stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    return {"n": len(ordered), "p50": round(statistics.median(ordered), 1),
            "p95": round(ordered[int(0.95 * (len(ordered) - 1))], 1), "max": round(ordered[-1], 1)}


async def _json(resp: Awaitable[httpx.Response]) -> Any:
    r = await resp
    r.raise_for_status()
    return r.json()


async def create_order(t: Target, member: str | None, items: list | None = None) -> dict[str, Any]:
    return await _json(t.mocks["pos"].post("/orders", json={
        "store_id": "ST-IST-01", "items": items or LATTE_AND_SIMIT, "loyalty_member_id": member}))


async def pay(t: Target, order: dict[str, Any], *, capture_amount: int | None = None,
              key: str | None = None) -> dict[str, Any]:
    payment = await _json(t.mocks["payments"].post(
        "/payments",
        json={"order_ref": order["id"], "amount_minor": order["total_minor"], "capture": capture_amount is None},
        headers={"Idempotency-Key": key or f"terminal-{order['id']}"}))
    if capture_amount is not None:
        payment = await _json(t.mocks["payments"].post(
            f"/payments/{payment['id']}/capture", json={"amount_minor": capture_amount},
            headers={"Idempotency-Key": f"capture-{payment['id']}"}))
    return payment


async def accruals(t: Target, order_ref: str) -> list[dict[str, Any]]:
    return await _json(t.mocks["loyalty"].get("/accruals", params={"order_ref": order_ref, "limit": 100}))


async def hub_order(t: Target, order_id: str) -> dict[str, Any] | None:
    resp = await t.hub.get(f"/v1/orders/{order_id}")
    return resp.json() if resp.status_code == 200 else None


async def emitted(t: Target, provider: str) -> list[dict[str, Any]]:
    return await _json(t.mocks[provider].get("/_admin/webhooks"))


async def open_mismatches(t: Target, order_ids: set[str]) -> list[dict[str, Any]]:
    rows = await _json(t.hub.get("/v1/mismatches", params={"status": "open", "limit": 500}))
    return [m for m in rows if m["order_id"] in order_ids]


async def naive_accrual(t: Target, member: str, order_ref: str, amount: int, deadline_s: float,
                        attempts: int = 3) -> None:
    """What the outbox prevents: retrying a timed-out call with a *new* Idempotency-Key each time."""
    for _ in range(attempts):
        try:
            async with asyncio.timeout(deadline_s):
                resp = await t.mocks["loyalty"].post("/accruals", json={
                    "member_id": member, "order_ref": order_ref, "amount_minor": amount},
                    headers={"Idempotency-Key": str(uuid.uuid4())})
            if resp.status_code < 300:
                return
        except (TimeoutError, httpx.HTTPError):
            continue


async def ensure_member(t: Target, member_id: str) -> dict[str, Any]:
    resp = await t.mocks["loyalty"].get(f"/members/{member_id}")
    if resp.status_code == 200:
        return resp.json()
    seed = loyalty_members()[member_id]
    body = {k: seed[k] for k in ("full_name", "phone", "email", "tier", "points_balance")}
    return await _json(t.mocks["loyalty"].put(f"/_admin/members/{member_id}", json=body))


def _start() -> datetime:
    return datetime.now(UTC) - timedelta(seconds=1)


# --------------------------------------------------------------- scenarios


async def duplicate_webhook(t: Target, n: int) -> tuple[list[Check], dict[str, Any]]:
    await t.faults("pos", webhook_copies=3)
    await t.faults("payments", webhook_copies=3)
    await t.faults("loyalty", webhook_copies=2)
    orders = []
    for i in range(n):
        order = await create_order(t, MEMBERS[i % len(MEMBERS)])
        await pay(t, order)
        orders.append(order)
    await t.drain()
    await t.reset_faults()

    ids = {o["id"] for o in orders}
    events = [e for e in await emitted(t, "pos") if e["event"]["resource"]["id"] in ids]
    events += [e for e in await emitted(t, "payments") if e["event"]["data"].get("order_ref") in ids]
    events += [e for e in await emitted(t, "loyalty") if e["event"]["data"].get("order_ref") in ids]
    deliveries = [d for e in events for d in e["deliveries"]]
    outcome = [((d.get("response") or {}).get("status")) for d in deliveries]
    accepted, duplicate = outcome.count("accepted"), outcome.count("duplicate")
    redeliveries = len(deliveries) - len(events)

    hub = [await hub_order(t, oid) for oid in ids]
    paid = sum(1 for h in hub if h and h["status"] == "paid")
    credits = [len(await accruals(t, oid)) for oid in ids]
    double = sum(1 for c in credits if c > 1)
    checks = [
        Check("each event stored exactly once", accepted == len(events), f"{accepted} accepted / {len(events)} events"),
        Check("every redelivery answered 2xx duplicate", duplicate == redeliveries,
              f"{duplicate} duplicate / {redeliveries} redeliveries"),
        Check("all orders paid in the hub", paid == n, f"{paid}/{n}"),
        Check("exactly one loyalty accrual per order", all(c == 1 for c in credits), f"double credits: {double}"),
    ]
    return checks, {
        "webhook_deliveries": len(deliveries), "unique_events": len(events), "redeliveries": redeliveries,
        "duplicates_suppressed": duplicate, "duplicate_suppression_pct": _pct(duplicate, redeliveries),
        "double_credits": double, "orders_paid": paid,
    }


async def timeout(t: Target, n: int) -> tuple[list[Check], dict[str, Any]]:
    deadline = float(t.config["outbound_timeout_s"])
    slow_ms = int(deadline * 1000 * 3) + 100
    orders = []
    for i in range(n):
        order = await create_order(t, MEMBERS[i % len(MEMBERS)])
        orders.append(order)
    await t.faults("loyalty", slow_next=n, slow_response_ms=slow_ms)
    for order in orders:
        await pay(t, order)
    started = time.perf_counter()
    await t.drain(max_wait_s=60)
    drain_s = time.perf_counter() - started
    await t.reset_faults()

    ids = {o["id"] for o in orders}
    outbox = [m for m in await _json(t.hub.get("/v1/outbox", params={"limit": 500})) if m["order_id"] in ids]
    sent = [m for m in outbox if m["status"] == "sent"]
    attempts = [m["attempts"] for m in outbox]
    credits = [len(await accruals(t, oid)) for oid in ids]
    hub_double = sum(1 for c in credits if c > 1)

    # Same fault (each call's first attempt answered late), naive client: new Idempotency-Key per retry.
    run = uuid.uuid4().hex[:6]
    naive_refs = [f"NAIVE-{run}-{i}" for i in range(n)]
    for ref in naive_refs:
        await t.faults("loyalty", slow_next=1, slow_response_ms=slow_ms)
        await naive_accrual(t, "LM-1003", ref, 16500, deadline)
    await t.reset_faults()
    naive_credits = [len(await accruals(t, ref)) for ref in naive_refs]
    naive_double = sum(1 for c in naive_credits if c > 1)

    checks = [
        Check("every accrual eventually sent", len(sent) == n, f"{len(sent)}/{n}"),
        Check("timeouts were retried", sum(attempts) > n, f"{sum(attempts)} attempts for {n} messages"),
        Check("no double points with a stable Idempotency-Key", hub_double == 0 and all(c == 1 for c in credits),
              f"double credits: {hub_double}"),
        Check("naive client (fresh key per retry) double-credits", naive_double > 0,
              f"naive double credits: {naive_double}/{n}"),
    ]
    return checks, {
        "outbound_deadline_s": deadline, "provider_reply_delay_ms": slow_ms,
        "attempts_per_message": _stats([float(a) for a in attempts]),
        "retries_per_success": round((sum(attempts) - len(sent)) / len(sent), 2) if sent else None,
        "delivery_success_pct": _pct(len(sent), n), "hub_double_credits": hub_double,
        "naive_double_credits": naive_double, "naive_extra_points_pct": _pct(sum(naive_credits) - n, n),
        "drain_seconds": round(drain_s, 2),
    }


async def partial_payment(t: Target, n: int) -> tuple[list[Check], dict[str, Any]]:
    since = _start()
    partial, clean = set(), set()
    created: dict[str, str] = {}
    for i in range(n):
        order = await create_order(t, MEMBERS[i % len(MEMBERS)])
        created[order["id"]] = order["created_at"]
        if i % 2 == 0:
            await pay(t, order, capture_amount=order["total_minor"] * 70 // 100)
            partial.add(order["id"])
        else:
            await pay(t, order)
            clean.add(order["id"])
    await t.drain()
    run = await t.reconcile(since)
    found = await open_mismatches(t, partial | clean)
    detected = {m["order_id"] for m in found if m["kind"] == "partial_payment"}
    false_pos = [m for m in found if m["order_id"] in clean or m["kind"] != "partial_payment"]
    statuses = {oid: await hub_order(t, oid) for oid in partial | clean}
    points_on_partial = sum([len(await accruals(t, oid)) for oid in partial])
    detect_ms = [_ms(m["first_detected_at"], created[m["order_id"]]) for m in found if m["order_id"] in partial]
    checks = [
        Check("every partial capture detected", detected == partial, f"{len(detected)}/{len(partial)}"),
        Check("no false positives on fully paid orders", not false_pos, f"{len(false_pos)} false positives"),
        Check("hub marks them partially_paid", all(statuses[o]["status"] == "partially_paid" for o in partial)),
        Check("no loyalty points for partially paid orders", points_on_partial == 0),
        Check("fully paid orders accrued", all(statuses[o]["loyalty_status"] == "accrued" for o in clean)),
    ]
    return checks, {
        "injected": len(partial), "detected": len(detected), "detection_pct": _pct(len(detected), len(partial)),
        "false_positives": len(false_pos), "time_to_detect_ms": _stats(detect_ms),
        "reconciliation_run": {k: run[k] for k in ("orders_checked", "payments_checked", "mismatches_found")},
    }


async def out_of_order(t: Target, n: int) -> tuple[list[Check], dict[str, Any]]:
    since = _start()
    k = max(n // 2, 1)
    gaps = max(n // 10, 1)

    def by_seq(events: list[dict], oid: str) -> dict[int, str]:
        return {e["event"]["sequence"]: e["event"]["id"] for e in events if e["event"]["resource"]["id"] == oid}

    # A: created + cancelled, delivered cancelled-first.  C: same, but v1 is lost for good.
    await t.faults("pos", webhook_copies=0)
    a_orders = [await create_order(t, None) for _ in range(k)]
    c_orders = [await create_order(t, None) for _ in range(gaps)]
    for o in a_orders + c_orders:
        await _json(t.mocks["pos"].post(f"/orders/{o['id']}/cancel"))
    # B: order webhook held while the payment's webhooks arrive first.
    b_orders = [await create_order(t, MEMBERS[i % len(MEMBERS)]) for i in range(k)]
    await t.faults("pos", webhook_copies=1)
    for o in b_orders:
        await pay(t, o)

    pos_events = await emitted(t, "pos")
    for o in a_orders + c_orders:
        await t.mocks["pos"].post(f"/_admin/webhooks/{by_seq(pos_events, o['id'])[2]}/deliver")
    await t.drain()
    parked_before = len(await _json(t.hub.get("/v1/inbox", params={"status": "parked", "limit": 500})))
    for o in a_orders:
        await t.mocks["pos"].post(f"/_admin/webhooks/{by_seq(pos_events, o['id'])[1]}/deliver")
    for o in b_orders:
        await t.mocks["pos"].post(f"/_admin/webhooks/{by_seq(pos_events, o['id'])[1]}/deliver")
    await t.drain()
    parked_after = len(await _json(t.hub.get("/v1/inbox", params={"status": "parked", "limit": 500})))

    await asyncio.sleep(float(t.config["park_timeout_s"]) + 0.3)
    await t.drain()
    c_ids = {o["id"] for o in c_orders}
    gap_dlq = [d for d in await _json(t.hub.get("/v1/dead-letters", params={
        "reason_code": "sequence_gap_timeout", "limit": 500})) if d["order_id"] in c_ids]
    run = await t.reconcile(since)
    await t.drain()
    resolved_dlq = [d for d in await _json(t.hub.get("/v1/dead-letters", params={
        "reason_code": "sequence_gap_timeout", "limit": 500})) if d["order_id"] in c_ids and d["status"] == "resolved"]

    agree = 0
    all_orders = a_orders + b_orders + c_orders
    for o in all_orders:
        pos = await _json(t.mocks["pos"].get(f"/orders/{o['id']}"))
        h = await hub_order(t, o["id"])
        agree += bool(h and h["pos_status"] == pos["status"] and h["pos_version"] == pos["version"])
    b_ok = [await hub_order(t, o["id"]) for o in b_orders]
    checks = [
        Check("cancellation-before-creation parked, then applied in order", parked_before == k + gaps
              and parked_after == gaps, f"parked {parked_before} → {parked_after}"),
        Check("payment-before-order folded in (paid + points)",
              all(h and h["status"] == "paid" and h["loyalty_status"] == "accrued" for h in b_ok)),
        Check("gap that never closes → DLQ after park timeout", len(gap_dlq) == gaps, f"{len(gap_dlq)}/{gaps}"),
        Check("reconciliation backfill resolved those DLQ entries", len(resolved_dlq) == gaps,
              f"{len(resolved_dlq)}/{gaps}"),
        Check("hub agrees with POS on every order", agree == len(all_orders), f"{agree}/{len(all_orders)}"),
    ]
    return checks, {
        "reordered_pairs": k, "payment_first_orders": k, "never_closing_gaps": gaps,
        "parked_peak": parked_before, "resumed": parked_before - parked_after,
        "gap_timeouts_dead_lettered": len(gap_dlq), "auto_recovered_by_reconciliation": len(resolved_dlq),
        "final_state_agreement_pct": _pct(agree, len(all_orders)), "backfills_queued": run["repairs_queued"],
    }


async def missing_customer(t: Target, n: int) -> tuple[list[Check], dict[str, Any]]:
    member = await ensure_member(t, MISSING_MEMBER)
    await _json(t.mocks["loyalty"].delete(f"/_admin/members/{MISSING_MEMBER}"))
    orders = []
    try:
        for _ in range(n):
            order = await create_order(t, MISSING_MEMBER)
            await pay(t, order)
            orders.append(order)
        await t.drain()
        ids = {o["id"] for o in orders}
        created = {o["id"]: o["created_at"] for o in orders}
        dlq = [d for d in await _json(t.hub.get("/v1/dead-letters", params={
            "status": "open", "reason_code": "member_not_found", "limit": 500})) if d["order_id"] in ids]
        statuses = [(await hub_order(t, oid))["loyalty_status"] for oid in ids]
    finally:
        body = {k: member[k] for k in ("full_name", "phone", "email", "tier", "points_balance")}
        await _json(t.mocks["loyalty"].put(f"/_admin/members/{MISSING_MEMBER}", json=body))
    replay = await _json(t.hub.post("/v1/dead-letters/replay", json={
        "requested_by": "scenario-runner", "reason_code": "member_not_found",
        "note": "member restored at the loyalty platform", "limit": 500}, headers=t.ops, timeout=120))
    ours = [r for r in replay["results"] if r["dead_letter_id"] in {d["id"] for d in dlq}]
    ok = sum(1 for r in ours if r["result"] == "succeeded")
    credits = [len(await accruals(t, oid)) for oid in ids]
    still_open = [d for d in await _json(t.hub.get("/v1/dead-letters", params={"status": "open", "limit": 500}))
                  if d["order_id"] in ids]
    detect_ms = [_ms(d["created_at"], created[d["order_id"]]) for d in dlq]
    checks = [
        Check("every order dead-lettered as member_not_found", len(dlq) == n, f"{len(dlq)}/{n}"),
        Check("no retries wasted on a permanent 404", all(d["attempts"] == 1 for d in dlq)),
        Check("hub shows loyalty_status member_missing", all(s == "member_missing" for s in statuses)),
        Check("replay after the fix succeeds", ok == n, f"{ok}/{n}"),
        Check("points credited exactly once after replay", all(c == 1 for c in credits)),
        Check("DLQ empty afterwards", not still_open),
    ]
    return checks, {
        "dead_lettered": len(dlq), "wasted_retries": sum(d["attempts"] - 1 for d in dlq),
        "replay_success_pct": _pct(ok, len(ours)), "time_to_dead_letter_ms": _stats(detect_ms),
        "double_credits_after_replay": sum(1 for c in credits if c > 1),
    }


async def data_mismatch(t: Target, n: int) -> tuple[list[Check], dict[str, Any]]:
    since = _start()
    n = max(n, 6)
    expected: set[tuple[str, str]] = set()
    clean: set[str] = set()
    created: dict[str, str] = {}
    for i in range(n):
        kind = i % 6
        if kind == 0:  # POS order webhook dropped
            await t.faults("pos", webhook_copies=0)
            order = await create_order(t, "LM-1002")
            await t.faults("pos", webhook_copies=1)
            await pay(t, order)
            expected.add(("missing_in_hub", order["id"]))
        elif kind == 1:  # cancellation webhook dropped
            order = await create_order(t, None)
            await pay(t, order)
            await t.faults("pos", webhook_copies=0)
            await _json(t.mocks["pos"].post(f"/orders/{order['id']}/cancel"))
            await t.faults("pos", webhook_copies=1)
            expected.add(("status_mismatch", order["id"]))
        elif kind == 2:  # payment webhooks dropped
            order = await create_order(t, None)
            await t.faults("payments", webhook_copies=0)
            await pay(t, order)
            await t.faults("payments", webhook_copies=1)
            expected.add(("payment_missing_in_hub", order["id"]))
        elif kind == 3:  # charged twice
            order = await create_order(t, None)
            await pay(t, order, key=f"t1-{uuid.uuid4().hex[:8]}")
            await pay(t, order, key=f"t2-{uuid.uuid4().hex[:8]}")
            expected.add(("overpayment", order["id"]))
        elif kind == 4:  # double points from another client
            order = await create_order(t, "LM-1001")
            await pay(t, order)
            await t.drain()
            await t.mocks["loyalty"].post("/accruals", json={
                "member_id": "LM-1001", "order_ref": order["id"], "amount_minor": order["total_minor"]},
                headers={"Idempotency-Key": str(uuid.uuid4())})
            expected.add(("duplicate_accrual", order["id"]))
        else:
            order = await create_order(t, MEMBERS[i % len(MEMBERS)])
            await pay(t, order)
            clean.add(order["id"])
        created[order["id"]] = order["created_at"]
    await t.drain()
    first = await t.reconcile(since)
    ids = set(created)
    found = {(m["kind"], m["order_id"]) for m in await open_mismatches(t, ids)}
    detail = await open_mismatches(t, ids)
    detected = expected & found
    false_pos = found - expected
    detect_ms = [_ms(m["first_detected_at"], created[m["order_id"]]) for m in detail
                 if (m["kind"], m["order_id"]) in expected]

    await t.drain()  # apply backfills
    second = await t.reconcile(since)
    remaining = {(m["kind"], m["order_id"]) for m in await open_mismatches(t, ids)}
    repairable = {e for e in expected if e[0] in ("missing_in_hub", "status_mismatch", "payment_missing_in_hub")}
    repaired = repairable - remaining
    manual = expected - repairable
    by_kind: dict[str, dict[str, int]] = {}
    for kind, oid in expected:
        row = by_kind.setdefault(kind, {"injected": 0, "detected": 0})
        row["injected"] += 1
        row["detected"] += (kind, oid) in found
    checks = [
        Check("every injected mismatch detected", detected == expected, f"{len(detected)}/{len(expected)}"),
        Check("no false positives", not false_pos, f"{sorted(false_pos)}"),
        Check("dropped webhooks repaired by backfill", repaired == repairable, f"{len(repaired)}/{len(repairable)}"),
        Check("money and points issues stay open for a human", manual <= remaining,
              f"{len(manual & remaining)}/{len(manual)} open"),
    ]
    return checks, {
        "injected": len(expected), "detected": len(detected), "detection_pct": _pct(len(detected), len(expected)),
        "false_positives": len(false_pos), "by_kind": by_kind, "clean_orders": len(clean),
        "auto_repaired": len(repaired), "auto_repairable": len(repairable),
        "left_for_humans": len(manual & remaining), "time_to_detect_ms": _stats(detect_ms),
        "reconciliation_runs": [{k: r[k] for k in ("orders_checked", "mismatches_found", "repairs_queued",
                                                    "resolved_mismatches")} for r in (first, second)],
    }


SCENARIOS: dict[str, tuple[str, Callable[[Target, int], Awaitable[tuple[list[Check], dict[str, Any]]]]]] = {
    "duplicate_webhook": ("Duplicate webhook", duplicate_webhook),
    "timeout": ("Timeout (provider did the work, replied late)", timeout),
    "partial_payment": ("Partial payment", partial_payment),
    "out_of_order": ("Out-of-order events", out_of_order),
    "missing_customer": ("Missing customer record", missing_customer),
    "data_mismatch": ("Data mismatch between systems", data_mismatch),
}


async def run_scenario(name: str, target_factory, n: int) -> ScenarioResult:
    """`target_factory(overrides)` returns an async context manager yielding a Target."""
    title, fn = SCENARIOS[name]
    async with target_factory(IN_PROCESS_OVERRIDES.get(name, {})) as t:
        started = time.perf_counter()
        try:
            checks, metrics = await fn(t, n)
        except Exception as exc:  # report, don't hide
            checks, metrics = [Check("scenario ran to completion", False, f"{type(exc).__name__}: {exc}")], {}
        finally:
            with contextlib.suppress(Exception):
                await t.reset_faults()
        duration = time.perf_counter() - started
        latency = (await _json(t.hub.get("/v1/summary")))["receiver_latency_ms"]
    return ScenarioResult(name, title, n, checks, metrics, round(duration, 2), latency)


async def run_all(names: list[str], target_factory, n: int) -> list[ScenarioResult]:
    return [await run_scenario(name, target_factory, n) for name in names]


# ------------------------------------------------------------------ report


def to_markdown(results: list[ScenarioResult], meta: dict[str, Any]) -> str:
    lines = [
        "# Scenario results",
        "",
        f"Generated by `python -m irlab.scenarios` on {meta['generated_at']} — target: **{meta['target']}**, "
        f"database: {meta['database']}, idempotency store: {meta['idempotency_store']}, "
        f"{meta['orders']} orders per scenario, Python {meta['python']} on {meta['platform']}.",
        "",
        "Numbers below are measured by this run, not estimated. Latencies are wall-clock inside this run "
        "(in-process runs have no network hop); see the README for how to read them.",
        "",
        "| Scenario | Result | Checks | Duration |",
        "|---|---|---|---|",
    ]
    for r in results:
        ok = sum(c.ok for c in r.checks)
        lines.append(f"| {r.title} | {'PASS' if r.passed else '**FAIL**'} | {ok}/{len(r.checks)} | {r.duration_s} s |")
    for r in results:
        lines += ["", f"## {r.title}", ""]
        for c in r.checks:
            lines.append(f"- {'✅' if c.ok else '❌'} {c.name}" + (f" — {c.detail}" if c.detail else ""))
        lines += ["", "```json", json.dumps(r.metrics, indent=2, ensure_ascii=False), "```",
                  "", f"Receiver latency during this scenario: {json.dumps(r.receiver_latency_ms)}"]
    return "\n".join(lines) + "\n"


def _meta(target: str, n: int, database: str, store: str) -> dict[str, Any]:
    return {"generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"), "target": target, "orders": n,
            "database": database, "idempotency_store": store, "python": platform.python_version(),
            "platform": f"{platform.system()} {platform.machine()}", "version": __version__}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--orders", type=int, default=20, help="orders per scenario (default 20)")
    parser.add_argument("--only", action="append", choices=list(SCENARIOS), help="run only these scenarios")
    parser.add_argument("--target", choices=["in-process", "http"], default="in-process")
    parser.add_argument("--hub-url", default=os.environ.get("IRLAB_HUB_URL", "http://127.0.0.1:8100"))
    parser.add_argument("--pos-url", default=os.environ.get("IRLAB_POS_URL", "http://127.0.0.1:8101"))
    parser.add_argument("--payments-url", default=os.environ.get("IRLAB_PAYMENTS_URL", "http://127.0.0.1:8102"))
    parser.add_argument("--loyalty-url", default=os.environ.get("IRLAB_LOYALTY_URL", "http://127.0.0.1:8103"))
    parser.add_argument("--json", type=Path, help="write raw results as JSON")
    parser.add_argument("--markdown", type=Path, help="write a Markdown report")
    parser.add_argument("--verbose", action="store_true", help="show hub logs (in-process)")
    args = parser.parse_args(argv)
    names = args.only or list(SCENARIOS)

    if args.target == "http":
        from irlab.config import get_settings

        ops_key = get_settings().ops_api_key.get_secret_value()
        if not ops_key:
            print("OPS_API_KEY is not set (run `python -m irlab.devtools init-env`)", file=sys.stderr)
            return 2

        def factory(_overrides):  # the stack's own settings apply
            return http_target(args.hub_url, args.pos_url, args.payments_url, args.loyalty_url, ops_key)
        database, store = "PostgreSQL 16", "Redis 7"
    else:
        def factory(overrides):
            return in_process_target(overrides, args.verbose)
        database, store = "SQLite (aiosqlite)", "in-memory"

    results = asyncio.run(run_all(names, factory, args.orders))
    meta = _meta(args.target, args.orders, database, store)
    for r in results:
        mark = "PASS" if r.passed else "FAIL"
        print(f"[{mark}] {r.title} ({r.duration_s} s)")
        for c in r.checks:
            print(f"    {'ok ' if c.ok else 'BAD'} {c.name}" + (f" — {c.detail}" if c.detail else ""))
    if args.json:
        args.json.write_text(json.dumps({"meta": meta, "results": [asdict(r) | {"passed": r.passed} for r in results]},
                                        indent=2, ensure_ascii=False))
    if args.markdown:
        args.markdown.write_text(to_markdown(results, meta))
    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
