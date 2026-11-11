"""Reconciliation: typed mismatches between the hub and each provider, backfill repair."""

import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
from sqlalchemy import func, select

from irlab.db import session_factory
from irlab.models import InboxEvent
from tests.labkit import Lab, open_lab


@pytest.fixture(params=["memory", "redis"])
async def lab(request, tmp_path) -> AsyncIterator[Lab]:
    async with open_lab(tmp_path, request.param) as lab:
        yield lab


def kinds(mismatches: list[dict]) -> list[str]:
    return sorted(m["kind"] for m in mismatches)


async def test_clean_systems_reconcile_without_findings(lab):
    for member in ("LM-1001", None, "LM-1003"):
        await lab.paid_order(member)
    await lab.tick()
    run = await lab.reconcile()
    assert (run["status"], run["orders_checked"], run["payments_checked"], run["accruals_checked"]) == ("ok", 3, 3, 2)
    assert run["mismatches_found"] == 0
    assert await lab.mismatches() == []


async def test_dropped_order_webhook_is_detected_and_repaired_by_backfill(lab):
    """Failure scenario 6 (data mismatch): the POS never delivered order.created."""
    await lab.faults("pos", webhook_copies=0)
    order = await lab.paid_order()
    await lab.tick()
    assert (await lab.client.get(f"/v1/orders/{order['id']}")).status_code == 404

    run = await lab.reconcile()
    assert (run["new_mismatches"], run["repairs_queued"]) == (1, 1)
    [m] = await lab.mismatches(status="open")
    assert (m["kind"], m["severity"], m["entity_id"], m["repair"]) == (
        "missing_in_hub", "critical", order["id"], "backfill_queued")

    await lab.tick()  # processes the backfilled snapshot (+ the waiting payment), sends the accrual
    hub_order = await lab.get(f"/v1/orders/{order['id']}")
    assert (hub_order["status"], hub_order["loyalty_status"]) == ("paid", "accrued")
    assert f"backfill:{order['id']}:v1" in [e["event_id"] for e in hub_order["events"]]

    run = await lab.reconcile()
    assert run["resolved_mismatches"] == 1 and run["mismatches_found"] == 0
    [m] = await lab.mismatches()
    assert (m["status"], m["resolution"]) == ("resolved", "repaired")


async def test_dropped_cancellation_is_a_status_mismatch(lab):
    order = await lab.paid_order(member=None)
    await lab.tick()
    await lab.faults("pos", webhook_copies=0)
    await lab.client.post(f"/mock/pos/orders/{order['id']}/cancel")
    await lab.tick()
    await lab.reconcile()
    [m] = await lab.mismatches(status="open")
    assert m["kind"] == "status_mismatch"
    assert (m["expected"], m["actual"]) == ({"pos_status": "cancelled", "version": 2},
                                            {"pos_status": "placed", "version": 1})
    await lab.tick()
    assert (await lab.get(f"/v1/orders/{order['id']}"))["status"] == "cancelled"


async def test_dropped_payment_webhooks_are_backfilled(lab):
    order = await lab.pos_order(member=None)
    await lab.faults("payments", webhook_copies=0)
    payment = await lab.pay(order)
    await lab.tick()
    assert (await lab.get(f"/v1/orders/{order['id']}"))["status"] == "awaiting_payment"
    await lab.reconcile()
    [m] = await lab.mismatches(status="open")
    assert (m["kind"], m["entity_id"], m["order_id"]) == ("payment_missing_in_hub", payment["id"], order["id"])
    await lab.tick()
    assert (await lab.get(f"/v1/orders/{order['id']}"))["status"] == "paid"


async def test_partial_payment_is_flagged_for_a_human_and_stays_open(lab):
    """Failure scenario 3: 190.00 TRY order, 140.00 TRY captured."""
    order = await lab.pos_order()
    await lab.pay(order, capture_amount=14000)
    await lab.tick()
    hub_order = await lab.get(f"/v1/orders/{order['id']}")
    assert (hub_order["status"], hub_order["loyalty_status"]) == ("partially_paid", "not_due")
    await lab.reconcile()
    await lab.reconcile()
    [m] = await lab.mismatches(status="open")
    assert (m["kind"], m["expected"], m["actual"], m["repair"], m["seen_count"]) == (
        "partial_payment", {"captured_minor": 19000}, {"captured_minor": 14000}, "manual", 2)

    resp = await lab.client.post(f"/v1/mismatches/{m['id']}/resolve",
                                 json={"requested_by": "finance", "note": "remaining 50 TRY collected in cash"},
                                 headers=lab.ops())
    assert resp.status_code == 200 and resp.json()["resolution"] == "manual:finance"
    assert (await lab.client.post(f"/v1/mismatches/{m['id']}/resolve", json={"requested_by": "x"},
                                  headers=lab.ops())).status_code == 409


async def test_charged_twice_is_an_overpayment(lab):
    order = await lab.pos_order(member=None)
    await lab.pay(order, key="first")
    await lab.pay(order, key="second-terminal-retry")
    await lab.tick()
    assert (await lab.get(f"/v1/orders/{order['id']}"))["status"] == "overpaid"
    await lab.reconcile()
    [m] = await lab.mismatches(status="open")
    assert (m["kind"], m["severity"], m["actual"]["captured_minor"]) == ("overpayment", "critical", 38000)


async def test_double_points_from_a_naive_client_are_detected(lab):
    order = await lab.paid_order(member="LM-1001")
    await lab.tick()
    # Some other integration retried with a fresh key:
    await lab.client.post("/mock/loyalty/accruals", json={"member_id": "LM-1001", "order_ref": order["id"],
                                                          "amount_minor": order["total_minor"]},
                          headers={"Idempotency-Key": str(uuid.uuid4())})
    await lab.reconcile()
    [m] = await lab.mismatches(status="open")
    assert (m["kind"], m["actual"]["accruals"], m["actual"]["points"]) == ("duplicate_accrual", 2, 380)


async def test_points_on_a_cancelled_order(lab):
    order = await lab.paid_order(member="LM-1001")
    await lab.tick()
    await lab.client.post(f"/mock/pos/orders/{order['id']}/cancel")
    await lab.tick()
    await lab.reconcile()
    assert kinds(await lab.mismatches(status="open")) == ["points_on_unpaid_order"]


async def test_points_mismatch(lab):
    order = await lab.pos_order(member="LM-1003", items=[{"sku": "SKU-TEA", "qty": 1}])  # 25.00 TRY
    await lab.pay(order)
    await lab.faults("loyalty", webhook_copies=1)
    await lab.client.post("/mock/loyalty/accruals", json={"member_id": "LM-1003", "order_ref": order["id"],
                                                          "amount_minor": 99900},
                          headers={"Idempotency-Key": "wrong-amount"})
    await lab.reconcile()
    found = await lab.mismatches(status="open")
    assert "points_mismatch" in kinds(found)
    [pm] = [m for m in found if m["kind"] == "points_mismatch"]
    assert (pm["expected"], pm["actual"]) == ({"points": 25}, {"points": 999})


async def test_orphan_payment(lab):
    await lab.pay({"id": "POS-999999", "total_minor": 5000})
    await lab.tick()
    await lab.reconcile()
    assert "orphan_payment" in kinds(await lab.mismatches(status="open"))


async def test_grace_period_skips_orders_whose_webhooks_may_be_in_flight(tmp_path):
    async with open_lab(tmp_path, reconcile_grace_s=30) as lab:
        await lab.faults("pos", webhook_copies=0)
        await lab.paid_order()
        run = await lab.reconcile()
        assert (run["skipped_in_grace"], run["mismatches_found"]) == (1, 0)
        lab.clock.advance(31)
        run = await lab.reconcile()
        assert run["mismatches_found"] == 1


async def test_provider_outage_makes_a_partial_run_without_false_positives(lab):
    order = await lab.paid_order()
    await lab.tick()
    await lab.faults("loyalty", fail_next=5)
    run = await lab.reconcile()
    assert run["status"] == "partial" and "loyalty" in run["errors"]
    assert run["orders_checked"] == 1 and run["mismatches_found"] == 0
    runs = await lab.get("/v1/reconciliation/runs")
    assert runs[0]["status"] == "partial" and "loyalty" in runs[0]["error"]
    assert order


async def test_backfill_is_queued_once_across_runs(lab):
    await lab.faults("pos", webhook_copies=0)
    order = await lab.paid_order()
    await lab.reconcile()
    second = await lab.reconcile()
    # The backfilled snapshot now sits in the inbox: the hub "knows", so the order is skipped, not re-queued.
    assert second["repairs_queued"] == 0 and second["new_mismatches"] == 0 and second["skipped_in_flight"] == 2
    async with session_factory()() as s:
        n = await s.scalar(select(func.count()).select_from(InboxEvent).where(InboxEvent.origin == "reconciliation"))
    assert n == 1
    [m] = await lab.mismatches()
    assert (m["status"], m["seen_count"], m["order_id"]) == ("open", 1, order["id"])


async def test_report_only_mode_does_not_backfill(lab):
    await lab.faults("pos", webhook_copies=0)
    await lab.paid_order()
    run = await lab.reconcile(auto_repair=False)
    assert run["repairs_queued"] == 0
    [m] = await lab.mismatches()
    assert m["repair"] == "backfill_available"


async def test_since_window_limits_what_is_checked(lab):
    await lab.faults("pos", webhook_copies=0)
    await lab.paid_order()
    run = await lab.reconcile(since=lab.clock.now() + timedelta(days=1))
    assert run["orders_checked"] == 0 and run["mismatches_found"] == 0


async def test_reconciliation_api_requires_ops_key(lab):
    assert (await lab.client.post("/v1/reconciliation/runs", json={})).status_code == 401
    lab.clock.advance(1)
    resp = await lab.client.post("/v1/reconciliation/runs", json={"requested_by": "ana"}, headers=lab.ops())
    assert resp.status_code == 200 and resp.json()["status"] == "ok"
    assert (await lab.get("/v1/reconciliation/runs"))[0]["triggered_by"] == "api:ana"
