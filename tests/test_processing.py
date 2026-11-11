"""Inbox processor: projection into canonical orders, ordering, parking, DLQ for bad events."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import select, update

from irlab.clock import FakeClock
from irlab.db import session_factory
from irlab.models import InboxEvent, OutboxMessage
from tests.conftest import Sender, hub_client, make_event, make_settings, make_store, running_hub


def order_data(order_id: str, *, status: str = "placed", total: int = 19000, member: str | None = "LM-1001",
               version: int = 1) -> dict[str, Any]:
    return {
        "id": order_id,
        "store_id": "ST-IST-01",
        "status": status,
        "total_minor": total,
        "currency": "TRY",
        "loyalty_member_id": member,
        "version": version,
        "created_at": datetime.now(UTC).isoformat(),
    }


def payment_data(payment_id: str, order_id: str, *, amount: int = 19000, captured: int = 19000,
                 status: str = "captured", version: int = 2) -> dict[str, Any]:
    return {
        "id": payment_id,
        "order_ref": order_id,
        "amount_minor": amount,
        "captured_minor": captured,
        "currency": "TRY",
        "status": status,
        "version": version,
    }


class Lab:
    def __init__(self, app, client, clock: FakeClock):
        self.app = app
        self.client = client
        self.clock = clock
        self.sender = Sender(client, app.state.settings)
        self.runtime = app.state.runtime

    async def order(self, order_id: str, seq: int = 1, event_type: str = "order.created", **kw) -> dict:
        event = make_event(event_type, ("order", order_id), seq, order_data(order_id, version=seq, **kw))
        resp = await self.sender.send("pos", event)
        assert resp.status_code == 202, resp.text
        return event

    async def payment(self, payment_id: str, order_id: str, seq: int = 1, event_type: str = "payment.captured",
                      **kw) -> dict:
        event = make_event(event_type, ("payment", payment_id), seq,
                           payment_data(payment_id, order_id, version=seq, **kw))
        resp = await self.sender.send("payments", event)
        assert resp.status_code == 202, resp.text
        return event

    async def process(self) -> int:
        return await self.runtime.processor.run_once()

    async def get(self, path: str, **params) -> Any:
        resp = await self.client.get(path, params=params)
        assert resp.status_code == 200, resp.text
        return resp.json()

    async def inbox_status(self, event_id: str) -> str:
        async with session_factory()() as s:
            return await s.scalar(select(InboxEvent.status).where(InboxEvent.event_id == event_id))

    def ops(self) -> dict[str, str]:
        return {"X-Ops-Key": self.app.state.settings.ops_api_key.get_secret_value()}


@pytest.fixture(params=["memory", "redis"])
async def lab(request, tmp_path) -> AsyncIterator[Lab]:
    clock = FakeClock()
    settings = make_settings(tmp_path, park_timeout_s=60, processing_max_attempts=3)
    async with running_hub(settings, make_store(request.param), clock=clock) as app, hub_client(app) as client:
        yield Lab(app, client, clock)


async def test_order_then_payment_projects_paid_order_and_queues_accrual(lab):
    await lab.order("POS-000001")
    await lab.process()
    order = await lab.get("/v1/orders/POS-000001")
    assert (order["status"], order["loyalty_status"], order["pos_version"]) == ("awaiting_payment", "not_due", 1)

    await lab.payment("pay_a", "POS-000001")
    await lab.process()
    order = await lab.get("/v1/orders/POS-000001")
    assert (order["status"], order["captured_minor"], order["loyalty_status"]) == ("paid", 19000, "pending")
    [msg] = order["outbox"]
    assert msg["idempotency_key"] == "loyalty-accrual:POS-000001"
    assert msg["payload"] == {"member_id": "LM-1001", "order_ref": "POS-000001", "amount_minor": 19000,
                              "currency": "TRY"}
    assert [e["status"] for e in order["events"]] == ["processed", "processed"]


async def test_payment_before_order_is_folded_in_when_order_arrives(lab):
    await lab.payment("pay_early", "POS-000002")
    await lab.process()
    assert (await lab.client.get("/v1/orders/POS-000002")).status_code == 404

    await lab.order("POS-000002")
    await lab.process()
    order = await lab.get("/v1/orders/POS-000002")
    assert order["status"] == "paid"
    assert [p["payment_id"] for p in order["payments"]] == ["pay_early"]


async def test_partial_capture_is_partially_paid_and_earns_no_points(lab):
    await lab.order("POS-000003")
    await lab.payment("pay_p", "POS-000003", captured=14000, status="partially_captured")
    await lab.process()
    order = await lab.get("/v1/orders/POS-000003")
    assert (order["status"], order["captured_minor"], order["loyalty_status"]) == ("partially_paid", 14000, "not_due")
    assert order["outbox"] == []


async def test_two_full_payments_are_overpaid(lab):
    await lab.order("POS-000004", member=None)
    await lab.payment("pay_1", "POS-000004")
    await lab.payment("pay_2", "POS-000004")
    await lab.process()
    order = await lab.get("/v1/orders/POS-000004")
    assert (order["status"], order["captured_minor"], order["loyalty_status"]) == ("overpaid", 38000, "not_applicable")


async def test_declined_payment(lab):
    await lab.order("POS-000005")
    await lab.payment("pay_d", "POS-000005", seq=1, event_type="payment.failed", captured=0, status="failed")
    await lab.process()
    assert (await lab.get("/v1/orders/POS-000005"))["status"] == "payment_failed"


async def test_out_of_order_event_is_parked_then_resumed(lab):
    """Failure scenario 4: order.cancelled (v2) arrives before order.created (v1)."""
    cancelled = await lab.order("POS-000006", seq=2, event_type="order.cancelled", status="cancelled")
    await lab.process()
    assert await lab.inbox_status(cancelled["id"]) == "parked"
    assert (await lab.client.get("/v1/orders/POS-000006")).status_code == 404

    created = await lab.order("POS-000006", seq=1)
    await lab.process()
    assert await lab.inbox_status(created["id"]) == "processed"
    assert await lab.inbox_status(cancelled["id"]) == "processed"
    order = await lab.get("/v1/orders/POS-000006")
    assert (order["status"], order["pos_status"], order["pos_version"]) == ("cancelled", "cancelled", 2)


async def test_older_snapshot_after_newer_is_superseded(lab):
    await lab.order("POS-000007", seq=1)
    await lab.order("POS-000007", seq=2, event_type="order.cancelled", status="cancelled")
    await lab.process()
    late = await lab.order("POS-000007", seq=1, event_type="order.updated")  # new event id, old version
    await lab.process()
    assert await lab.inbox_status(late["id"]) == "superseded"
    assert (await lab.get("/v1/orders/POS-000007"))["status"] == "cancelled"


async def test_gap_that_never_closes_is_dead_lettered_then_replayed_as_snapshot(lab):
    await lab.order("POS-000008", seq=1)
    v3 = await lab.order("POS-000008", seq=3, event_type="order.cancelled", status="cancelled")
    await lab.process()
    assert await lab.inbox_status(v3["id"]) == "parked"

    lab.clock.advance(59)
    assert await lab.runtime.processor.expire_parked() == 0
    lab.clock.advance(2)
    assert await lab.runtime.processor.expire_parked() == 1
    assert await lab.inbox_status(v3["id"]) == "dead"
    [dl] = await lab.get("/v1/dead-letters", status="open")
    assert (dl["reason_code"], dl["origin"], dl["order_id"]) == ("sequence_gap_timeout", "inbox", "POS-000008")
    assert dl["details"]["expected_sequence"] == 2 and dl["details"]["parked_sequence"] == 3

    resp = await lab.client.post(f"/v1/dead-letters/{dl['id']}/replay", json={"requested_by": "ops-test"},
                                 headers=lab.ops())
    assert resp.status_code == 200, resp.text
    assert resp.json()["result"] == "succeeded"
    order = await lab.get("/v1/orders/POS-000008")
    assert (order["status"], order["pos_version"]) == ("cancelled", 3)
    detail = await lab.get(f"/v1/dead-letters/{dl['id']}")
    assert (detail["status"], detail["resolution"]) == ("resolved", "replayed")
    assert [(r["action"], r["requested_by"], r["result"]) for r in detail["replays"]] == [
        ("replay", "ops-test", "succeeded")
    ]


async def test_gap_closing_after_dead_letter_supersedes_nothing_it_should_not(lab):
    """v2 arriving late applies normally; v3 (still dead) applies on replay, in order."""
    await lab.order("POS-000009", seq=1)
    v3 = await lab.order("POS-000009", seq=3, event_type="order.updated")
    await lab.process()
    lab.clock.advance(61)
    await lab.runtime.processor.expire_parked()
    await lab.order("POS-000009", seq=2, event_type="order.updated")
    await lab.process()
    assert (await lab.get("/v1/orders/POS-000009"))["pos_version"] == 2
    assert await lab.inbox_status(v3["id"]) == "dead"


async def test_backfilled_newer_snapshot_resolves_gap_dead_letter(lab):
    """A newer version (e.g. reconciliation backfill) makes a gap-timeout dead letter obsolete."""
    await lab.order("POS-000010", seq=1)
    v3 = await lab.order("POS-000010", seq=3, event_type="order.updated")
    await lab.process()
    lab.clock.advance(61)
    await lab.runtime.processor.expire_parked()
    await lab.order("POS-000010", seq=2, event_type="order.updated")
    await lab.order("POS-000010", seq=4, event_type="order.cancelled", status="cancelled")
    # v4 parks (v3 is dead, not applied); replaying v3 applies it and wakes v4.
    await lab.process()
    [dl] = await lab.get("/v1/dead-letters", status="open")
    assert dl["origin_id"] and dl["reason_code"] == "sequence_gap_timeout"
    resp = await lab.client.post(f"/v1/dead-letters/{dl['id']}/replay", json={"requested_by": "ops"},
                                 headers=lab.ops())
    assert resp.json()["result"] == "succeeded"
    assert await lab.inbox_status(v3["id"]) == "processed"
    assert (await lab.get("/v1/orders/POS-000010"))["status"] == "cancelled"


async def test_invalid_payload_is_dead_lettered_immediately(lab):
    event = make_event("order.created", ("order", "POS-000011"), 1,
                       order_data("POS-000011") | {"total_minor": "a lot"})
    assert (await lab.sender.send("pos", event)).status_code == 202
    await lab.process()
    assert await lab.inbox_status(event["id"]) == "dead"
    [dl] = await lab.get("/v1/dead-letters")
    assert (dl["reason_code"], dl["attempts"]) == ("invalid_payload", 1)
    assert "total_minor" in dl["last_error"]


async def test_unexpected_error_is_retried_then_dead_lettered(lab, monkeypatch):
    calls = 0

    async def boom(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise RuntimeError("projection bug")

    monkeypatch.setitem(lab.runtime.processor._handlers, ("pos", "order"), boom)
    event = await lab.order("POS-000012")
    for _ in range(5):
        await lab.process()
    assert calls == 3  # processing_max_attempts
    assert await lab.inbox_status(event["id"]) == "dead"
    [dl] = await lab.get("/v1/dead-letters")
    assert dl["reason_code"] == "processing_failed" and "projection bug" in dl["last_error"]


async def test_transient_error_then_success(lab, monkeypatch):
    original = lab.runtime.processor._handlers[("pos", "order")]
    state = {"fail": True}

    async def flaky(*args, **kwargs):
        if state.pop("fail", False):
            raise RuntimeError("deadlock detected")
        return await original(*args, **kwargs)

    monkeypatch.setitem(lab.runtime.processor._handlers, ("pos", "order"), flaky)
    event = await lab.order("POS-000013")
    await lab.process()
    assert await lab.inbox_status(event["id"]) == "processed"
    assert await lab.get("/v1/dead-letters") == []


async def test_failed_projection_rolls_back_atomically(lab, monkeypatch):
    """Projection + outbox + status commit together: a failure leaves no half-written order or message."""
    await lab.order("POS-000014")
    await lab.process()

    async def fail_after_write(s, order, ev, now):
        order.status = "paid"
        s.add(OutboxMessage(kind="x", target="loyalty", method="POST", path="/x", payload={},
                            idempotency_key="half-written", order_id=order.id))
        await s.flush()
        raise RuntimeError("crash mid-transaction")

    monkeypatch.setattr(lab.runtime.processor, "_refresh", fail_after_write)
    await lab.payment("pay_x", "POS-000014")
    await lab.process()
    order = await lab.get("/v1/orders/POS-000014")
    assert order["status"] == "awaiting_payment"
    assert order["outbox"] == [] and order["payments"] == []


async def test_ignored_event_types_are_not_processed(lab):
    event = make_event("order.refunded_by_magic", ("order", "POS-000015"), 1, order_data("POS-000015"))
    await lab.sender.send("pos", event)
    assert await lab.process() == 0
    assert await lab.inbox_status(event["id"]) == "ignored"


async def test_processing_twice_is_a_no_op(lab):
    event = await lab.order("POS-000016")
    await lab.process()
    async with session_factory()() as s:
        inbox_id = await s.scalar(select(InboxEvent.id).where(InboxEvent.event_id == event["id"]))
    assert await lab.runtime.processor.process_one(inbox_id) is None


async def test_expired_processing_claim_is_recovered(lab):
    event = await lab.order("POS-000017")
    async with session_factory()() as s:
        await s.execute(
            update(InboxEvent).where(InboxEvent.event_id == event["id"])
            .values(status="processing", claimed_until=lab.clock.now())
        )
        await s.commit()
    assert await lab.process() == 0  # still claimed (by a "crashed" worker)
    lab.clock.advance(1)
    assert await lab.runtime.processor.recover_expired_claims() == 1
    await lab.process()
    assert await lab.inbox_status(event["id"]) == "processed"


async def test_loyalty_webhook_confirms_accrual(lab):
    await lab.order("POS-000018")
    await lab.payment("pay_l", "POS-000018")
    await lab.process()
    accrual = {"id": "acc_1", "member_id": "LM-1001", "order_ref": "POS-000018", "amount_minor": 19000,
               "points": 190, "balance_after": 1430, "created_at": datetime.now(UTC).isoformat()}
    event = make_event("points.accrued", ("member", "LM-1001"), 2, accrual)
    await lab.sender.send("loyalty", event)
    await lab.process()
    order = await lab.get("/v1/orders/POS-000018")
    assert (order["loyalty_status"], order["points_awarded"], order["loyalty_accrual_id"]) == ("accrued", 190, "acc_1")
