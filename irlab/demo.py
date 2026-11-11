"""End-to-end walkthrough, fully in-process (temp SQLite, fresh random secrets, no .env needed).

    python -m irlab.demo

One order through the happy path, then the six failure scenarios one at a time,
each with what the hub did about it. Everything goes through the same public
APIs the Docker Compose stack exposes; see docs/demo-script.md for the narrated
version.
"""

import asyncio
import json
import time
import uuid
from typing import Any

from irlab.scenarios import Target, accruals, create_order, emitted, hub_order, in_process_target, naive_accrual, pay


def say(text: str = "") -> None:
    print(text)


def show(label: str, value: Any) -> None:
    print(f"   {label:<34} {value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)}")


async def happy_path(t: Target) -> None:
    say("1) Happy path: POS order → signed webhook → payment → loyalty points (via the outbox)")
    order = await create_order(t, "LM-1001", [{"sku": "SKU-LATTE", "qty": 2}])
    await pay(t, order)
    await t.drain()
    h = await hub_order(t, order["id"])
    show("POS order", f"{order['id']} total {order['total_minor'] / 100:.2f} TRY")
    show("hub", {k: h[k] for k in ("status", "loyalty_status", "points_awarded")})
    show("events", [f"{e['event_type']}:{e['status']}" for e in h["events"]])
    show("outbox", [f"{m['kind']} key={m['idempotency_key']} {m['status']}" for m in h["outbox"]])


async def duplicates(t: Target) -> None:
    say("\n2) Duplicate webhooks: every POS webhook delivered 3×")
    await t.faults("pos", webhook_copies=3)
    order = await create_order(t, None)
    await t.reset_faults()
    [event] = [e for e in await emitted(t, "pos") if e["event"]["resource"]["id"] == order["id"]]
    show("hub answers", [f"{d['status']} {d['response']['status']}" for d in event["deliveries"]])
    forged = await t.hub.post("/webhooks/pos", content=json.dumps(event["event"]),
                              headers={"webhook-id": event["event"]["id"], "webhook-timestamp": str(int(time.time())),
                                       "webhook-signature": "v1,Zm9yZ2VkIHNpZ25hdHVyZQ=="})
    show("forged copy", f"{forged.status_code} {forged.json()['reason']}")


async def timeout(t: Target) -> None:
    say("\n3) Timeout: loyalty does the work but replies after the hub's 0.5 s deadline")
    await t.faults("loyalty", slow_next=1, slow_response_ms=1500)
    order = await create_order(t, "LM-1002")
    await pay(t, order)
    await t.drain()
    [msg] = (await hub_order(t, order["id"]))["outbox"]
    show("hub outbox (stable key)", f"{msg['status']} after {msg['attempts']} attempts → "
                                    f"{len(await accruals(t, order['id']))} accrual at the provider")
    ref = f"NAIVE-{uuid.uuid4().hex[:6]}"
    await t.faults("loyalty", slow_next=1, slow_response_ms=1500)
    await naive_accrual(t, "LM-1003", ref, order["total_minor"], 0.5)
    show("naive client (new key per retry)", f"{len(await accruals(t, ref))} accruals → double points")
    await t.reset_faults()


async def partial_payment(t: Target, since) -> str:
    say("\n4) Partial payment: 190.00 TRY order, 133.00 TRY captured")
    order = await create_order(t, "LM-1001", [{"sku": "SKU-LATTE", "qty": 2}])
    await pay(t, order, capture_amount=13300)
    await t.drain()
    h = await hub_order(t, order["id"])
    show("hub", {k: h[k] for k in ("status", "captured_minor", "loyalty_status")})
    return order["id"]


async def out_of_order(t: Target) -> None:
    say("\n5) Out of order: order.cancelled (v2) arrives before order.created (v1)")
    await t.faults("pos", webhook_copies=0)
    order = await create_order(t, None)
    await t.mocks["pos"].post(f"/orders/{order['id']}/cancel")
    await t.reset_faults()
    events = {e["event"]["sequence"]: e["event"]["id"] for e in await emitted(t, "pos")
              if e["event"]["resource"]["id"] == order["id"]}
    await t.mocks["pos"].post(f"/_admin/webhooks/{events[2]}/deliver")
    await t.drain()
    parked = (await t.hub.get("/v1/inbox", params={"resource_id": order["id"]})).json()
    show("after v2 only", [f"v{e['sequence']}:{e['status']}" for e in parked])
    await t.mocks["pos"].post(f"/_admin/webhooks/{events[1]}/deliver")
    await t.drain()
    h = await hub_order(t, order["id"])
    show("after v1 arrives", [f"v{e['sequence']}:{e['status']}" for e in h["events"]]
         + [f"order {h['status']} v{h['pos_version']}"])


async def missing_member(t: Target) -> None:
    say("\n6) Missing customer record: member LM-1005 deleted at the loyalty platform")
    member = (await t.mocks["loyalty"].delete("/_admin/members/LM-1005")).json()
    order = await create_order(t, "LM-1005")
    await pay(t, order)
    await t.drain()
    [dl] = [d for d in (await t.hub.get("/v1/dead-letters", params={"status": "open"})).json()
            if d["order_id"] == order["id"]]
    show("dead letter", f"#{dl['id']} {dl['reason_code']} after {dl['attempts']} attempt (no wasted retries)")
    triage = (await t.hub.get(f"/v1/dead-letters/{dl['id']}/triage")).json()
    show("triage (mock, read-only)", f"{triage['runbook_url']} — {triage['next_steps'][0]}")
    await t.mocks["loyalty"].put("/_admin/members/LM-1005", json={
        k: member[k] for k in ("full_name", "phone", "email", "tier", "points_balance")})
    replay = (await t.hub.post(f"/v1/dead-letters/{dl['id']}/replay", headers=t.ops,
                               json={"requested_by": "demo", "note": "member restored"})).json()
    show("replay after the fix", f"{replay['result']} → {len(await accruals(t, order['id']))} accrual")


async def data_mismatch(t: Target, since, partial_id: str) -> None:
    say("\n7) Data mismatch: a POS webhook is dropped; reconciliation compares every system")
    await t.faults("pos", webhook_copies=0)
    order = await create_order(t, None)
    await t.reset_faults()
    await pay(t, order)
    await t.drain()
    show("hub knows the order?", "no" if await hub_order(t, order["id"]) is None else "yes")
    run = await t.reconcile(since)
    show("reconciliation", {k: run[k] for k in ("orders_checked", "mismatches_found", "repairs_queued", "by_kind")})
    await t.drain()
    h = await hub_order(t, order["id"])
    show("after backfill", f"order {h['status']} (from the POS REST snapshot)")
    run = await t.reconcile(since)
    show("next run", {k: run[k] for k in ("mismatches_found", "resolved_mismatches")})
    found = (await t.hub.get("/v1/mismatches", params={"status": "open"})).json()
    show("still open for a human", [f"{m['kind']} {m['order_id']}" for m in found])
    assert any(m["order_id"] == partial_id for m in found)


async def wrap_up(t: Target) -> None:
    say("\n8) Alerts and the dashboard")
    resp = await t.hub.post("/v1/alerts/evaluate", headers=t.ops)
    show("firing", [r["rule"] for r in resp.json() if r["firing"]])
    summary = (await t.hub.get("/v1/summary")).json()
    show("webhook receipts", summary["receipts_by_outcome"])
    show("dead letters open", summary["dead_letters_open_by_reason"] or "none")
    show("replays", summary["replays_by_result"])
    say("   dashboard: /dashboard (uvicorn irlab.main:app, or Docker Compose on :8100)")


async def run() -> None:
    from datetime import UTC, datetime, timedelta

    async with in_process_target({"outbound_timeout_s": 0.5}) as t:
        since = datetime.now(UTC) - timedelta(seconds=1)
        await happy_path(t)
        await duplicates(t)
        await timeout(t)
        partial_id = await partial_payment(t, since)
        await out_of_order(t)
        await missing_member(t)
        await data_mismatch(t, since, partial_id)
        await wrap_up(t)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
