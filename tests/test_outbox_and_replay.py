"""Outbox dispatcher against the real (in-process) loyalty mock: retries, backoff,
timeouts, DLQ, replay and the operator API."""

import asyncio
import random
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from sqlalchemy import select, update

from irlab.clock import FakeClock
from irlab.db import session_factory
from irlab.models import OutboxMessage
from irlab.outbox import backoff_delay, is_retryable_status
from tests.conftest import hub_client, make_settings, make_store, running_hub


class Lab:
    def __init__(self, app, client: httpx.AsyncClient, clock: FakeClock):
        self.app = app
        self.client = client
        self.clock = clock
        self.runtime = app.state.runtime
        self.settings = app.state.settings

    def ops(self) -> dict[str, str]:
        return {"X-Ops-Key": self.settings.ops_api_key.get_secret_value()}

    async def paid_order(self, member: str | None = "LM-1002", qty: int = 2) -> dict[str, Any]:
        order = (
            await self.client.post(
                "/mock/pos/orders",
                json={"store_id": "ST-IST-01", "items": [{"sku": "SKU-LATTE", "qty": qty}], "loyalty_member_id": member},
            )
        ).json()
        resp = await self.client.post(
            "/mock/payments/payments",
            json={"order_ref": order["id"], "amount_minor": order["total_minor"]},
            headers={"Idempotency-Key": f"pay-{order['id']}"},
        )
        assert resp.status_code == 201, resp.text
        return order

    async def tick(self) -> dict:
        return await self.runtime.worker.tick(periodic=False)

    async def order(self, order_id: str) -> dict:
        resp = await self.client.get(f"/v1/orders/{order_id}")
        assert resp.status_code == 200, resp.text
        return resp.json()

    async def accruals(self, order_id: str) -> list[dict]:
        return (await self.client.get("/mock/loyalty/accruals", params={"order_ref": order_id})).json()

    async def faults(self, provider: str, **faults) -> None:
        assert (await self.client.put(f"/mock/{provider}/_admin/faults", json=faults)).status_code == 200

    async def dead_letters(self, **params) -> list[dict]:
        return (await self.client.get("/v1/dead-letters", params=params)).json()

    async def replay(self, dl_id: int, by: str = "ops-test") -> httpx.Response:
        return await self.client.post(f"/v1/dead-letters/{dl_id}/replay", json={"requested_by": by}, headers=self.ops())

    async def retry_until_settled(self, max_rounds: int = 20) -> None:
        """Advance the fake clock past each backoff and tick, until nothing is pending."""
        for _ in range(max_rounds):
            await self.tick()
            due = await self.runtime.dispatcher.next_due_in()
            if due is None:
                return
            self.clock.advance(due + 0.001)
        raise AssertionError("outbox did not settle")


@pytest.fixture(params=["memory", "redis"])
async def lab(request, tmp_path) -> AsyncIterator[Lab]:
    clock = FakeClock()
    settings = make_settings(
        tmp_path, mount_mocks=True, retry_base_s=1.0, retry_cap_s=30.0, retry_max_attempts=4, outbound_timeout_s=0.5
    )
    async with (
        running_hub(settings, make_store(request.param), clock=clock, rng=random.Random(7)) as app,
        hub_client(app) as client,
    ):
        yield Lab(app, client, clock)


# ----------------------------------------------------------------- backoff


def test_backoff_is_full_jitter_bounded_by_exponential_ceiling():
    rng = random.Random(1)
    for attempt in range(1, 12):
        ceiling = min(30.0, 0.5 * 2 ** (attempt - 1))
        delays = [backoff_delay(attempt, 0.5, 30.0, rng) for _ in range(200)]
        assert all(0 <= d <= ceiling for d in delays)
        assert max(delays) > ceiling * 0.8  # actually spreads over the whole window
    assert backoff_delay(50, 0.5, 30.0, random.Random(2)) <= 30.0


def test_backoff_is_reproducible_with_a_seed():
    a = [backoff_delay(n, 1, 60, random.Random(3)) for n in range(1, 6)]
    b = [backoff_delay(n, 1, 60, random.Random(3)) for n in range(1, 6)]
    assert a == b


@pytest.mark.parametrize(("status", "retry"), [(500, True), (503, True), (429, True), (409, True), (408, True),
                                                (400, False), (404, False), (422, False), (401, False)])
def test_retryable_status_classification(status, retry):
    assert is_retryable_status(status) is retry


# -------------------------------------------------------------- happy path


async def test_paid_order_accrues_points_exactly_once(lab):
    order = await lab.paid_order()
    await lab.tick()
    hub_order = await lab.order(order["id"])
    assert (hub_order["status"], hub_order["loyalty_status"], hub_order["points_awarded"]) == ("paid", "accrued", 190)
    [msg] = hub_order["outbox"]
    assert (msg["status"], msg["attempts"], msg["last_http_status"]) == ("sent", 1, 201)
    [accrual] = await lab.accruals(order["id"])
    assert accrual["points"] == 190
    assert hub_order["loyalty_accrual_id"] == accrual["id"]
    member = (await lab.client.get("/mock/loyalty/members/LM-1002")).json()
    assert member["points_balance"] == 310 + 190


async def test_order_without_member_queues_nothing(lab):
    order = await lab.paid_order(member=None)
    await lab.tick()
    hub_order = await lab.order(order["id"])
    assert (hub_order["status"], hub_order["loyalty_status"], hub_order["outbox"]) == ("paid", "not_applicable", [])


# ------------------------------------------------------- transient failure


async def test_transient_5xx_is_retried_with_backoff_then_succeeds(lab):
    await lab.faults("loyalty", fail_next=2, fail_status=503)
    order = await lab.paid_order()
    await lab.tick()
    [msg] = (await lab.order(order["id"]))["outbox"]
    assert (msg["status"], msg["attempts"], msg["last_http_status"]) == ("pending", 1, 503)
    due_in = await lab.runtime.dispatcher.next_due_in()
    assert 0 <= due_in <= 1.0  # attempt 1 → uniform(0, base)

    # Not due yet: nothing is sent early.
    assert await lab.runtime.dispatcher.run_once() == (1 if due_in == 0 else 0)
    await lab.retry_until_settled()
    hub_order = await lab.order(order["id"])
    assert hub_order["loyalty_status"] == "accrued"
    assert hub_order["outbox"][0]["attempts"] == 3
    assert len(await lab.accruals(order["id"])) == 1


async def test_retries_exhausted_goes_to_dead_letter_queue(lab):
    await lab.faults("loyalty", fail_next=100, fail_status=500)
    order = await lab.paid_order()
    await lab.retry_until_settled()
    hub_order = await lab.order(order["id"])
    [msg] = hub_order["outbox"]
    assert (msg["status"], msg["attempts"]) == ("dead", 4)
    assert hub_order["loyalty_status"] == "failed"
    [dl] = await lab.dead_letters(status="open")
    assert (dl["reason_code"], dl["origin"], dl["attempts"], dl["order_id"]) == (
        "retries_exhausted", "outbox", 4, order["id"])
    assert dl["details"]["idempotency_key"] == f"loyalty-accrual:{order['id']}"

    # Provider recovers; operator replays; same key, points once.
    await lab.faults("loyalty", fail_next=0)
    resp = await lab.replay(dl["id"])
    assert resp.json()["result"] == "succeeded"
    assert len(await lab.accruals(order["id"])) == 1
    assert (await lab.order(order["id"]))["loyalty_status"] == "accrued"


# ------------------------------------------------------------------ timeout


async def test_timeout_after_provider_did_the_work_does_not_double_credit(lab):
    """Failure scenario 2: the provider processes the accrual but answers after our deadline."""
    await lab.faults("loyalty", slow_next=1, slow_response_ms=1500)
    order = await lab.paid_order()
    await lab.tick()
    [msg] = (await lab.order(order["id"]))["outbox"]
    assert (msg["status"], msg["last_error"]) == ("pending", "timeout after 0.5s")
    assert len(await lab.accruals(order["id"])) == 1  # the provider did do it

    await lab.retry_until_settled()
    hub_order = await lab.order(order["id"])
    assert hub_order["loyalty_status"] == "accrued" and hub_order["outbox"][0]["attempts"] == 2
    assert len(await lab.accruals(order["id"])) == 1  # retry with the same key was replayed, not re-applied
    member = (await lab.client.get("/mock/loyalty/members/LM-1002")).json()
    assert member["points_balance"] == 310 + 190


async def test_naive_retry_with_fresh_keys_double_credits(lab):
    """The bug the stable key prevents: same fault, a client that mints a new key per attempt."""
    await lab.faults("loyalty", slow_next=1, slow_response_ms=1500)
    body = {"member_id": "LM-1003", "order_ref": "NAIVE-1", "amount_minor": 19000}
    for _ in range(2):
        try:
            async with asyncio.timeout(0.5):
                resp = await lab.client.post("/mock/loyalty/accruals", json=body,
                                             headers={"Idempotency-Key": str(uuid.uuid4())})
            assert resp.status_code == 201
            break
        except TimeoutError:
            continue
    assert len(await lab.accruals("NAIVE-1")) == 2


# ---------------------------------------------------- missing customer record


async def test_missing_member_dead_letters_without_wasting_retries_then_replays(lab):
    """Failure scenario 5: the POS order names a member the loyalty platform does not have."""
    member = (await lab.client.delete("/mock/loyalty/_admin/members/LM-1004")).json()
    order = await lab.paid_order(member="LM-1004")
    await lab.tick()
    hub_order = await lab.order(order["id"])
    assert hub_order["loyalty_status"] == "member_missing"
    [dl] = await lab.dead_letters(status="open")
    assert (dl["reason_code"], dl["attempts"]) == ("member_not_found", 1)

    # Replaying before the data is fixed fails again and reopens the same dead letter.
    again = (await lab.replay(dl["id"])).json()
    assert (again["result"], again["dead_letter_status"]) == ("failed_again", "open")
    assert (await lab.dead_letters(status="open"))[0]["times_dead_lettered"] == 2

    # Fix the record at the provider, then replay: same idempotency key (404s are not cached).
    restore = {k: member[k] for k in ("full_name", "phone", "email", "tier", "points_balance")}
    assert (await lab.client.put("/mock/loyalty/_admin/members/LM-1004", json=restore)).status_code == 200
    ok = (await lab.replay(dl["id"])).json()
    assert ok["result"] == "succeeded"
    assert (await lab.order(order["id"]))["loyalty_status"] == "accrued"
    assert len(await lab.accruals(order["id"])) == 1
    detail = (await lab.client.get(f"/v1/dead-letters/{dl['id']}")).json()
    assert [r["result"] for r in detail["replays"]] == ["failed_again", "succeeded"]
    assert detail["origin_record"]["idempotency_key"] == f"loyalty-accrual:{order['id']}"


async def test_bulk_replay_by_reason(lab):
    await lab.client.delete("/mock/loyalty/_admin/members/LM-1005")
    orders = [await lab.paid_order(member="LM-1005") for _ in range(3)]
    await lab.tick()
    assert len(await lab.dead_letters(status="open", reason_code="member_not_found")) == 3
    await lab.client.put("/mock/loyalty/_admin/members/LM-1005", json={
        "full_name": "Sarah Collins", "phone": "+905550101005", "email": "lm-1005@example.com"})
    resp = await lab.client.post("/v1/dead-letters/replay", json={"requested_by": "ops", "reason_code": "member_not_found"},
                                 headers=lab.ops())
    summary = resp.json()
    assert (summary["requested"], summary["succeeded"]) == (3, 3)
    for o in orders:
        assert len(await lab.accruals(o["id"])) == 1
    assert await lab.dead_letters(status="open") == []


async def test_replay_that_hits_a_transient_error_schedules_retries(lab):
    await lab.client.delete("/mock/loyalty/_admin/members/LM-1001")
    order = await lab.paid_order(member="LM-1001")
    await lab.tick()
    [dl] = await lab.dead_letters(status="open")
    await lab.client.put("/mock/loyalty/_admin/members/LM-1001", json={
        "full_name": "Ayşe Yılmaz", "phone": "+905550101001", "email": "lm-1001@example.com"})
    await lab.faults("loyalty", fail_next=1)
    out = (await lab.replay(dl["id"])).json()
    assert (out["result"], out["dead_letter_status"]) == ("retry_scheduled", "replaying")
    await lab.retry_until_settled()
    detail = (await lab.client.get(f"/v1/dead-letters/{dl['id']}")).json()
    assert (detail["status"], detail["resolution"]) == ("resolved", "replayed")
    assert (await lab.order(order["id"]))["loyalty_status"] == "accrued"


async def test_discard_closes_with_audit_and_cannot_be_replayed(lab):
    await lab.client.delete("/mock/loyalty/_admin/members/LM-1003")
    await lab.paid_order(member="LM-1003")
    await lab.tick()
    [dl] = await lab.dead_letters(status="open")
    resp = await lab.client.post(f"/v1/dead-letters/{dl['id']}/discard",
                                 json={"requested_by": "ops", "note": "member closed their account"}, headers=lab.ops())
    assert resp.json()["result"] == "discarded"
    assert (await lab.replay(dl["id"])).status_code == 409
    [audit] = (await lab.client.get("/v1/replays")).json()
    assert (audit["action"], audit["note"]) == ("discard", "member closed their account")


# -------------------------------------------------------------- edge cases


async def test_order_cancelled_before_send_skips_accrual(lab):
    await lab.faults("loyalty", fail_next=1)
    order = await lab.paid_order()
    await lab.tick()  # attempt 1 fails, retry scheduled
    await lab.client.post(f"/mock/pos/orders/{order['id']}/cancel")
    await lab.runtime.processor.run_once()
    await lab.retry_until_settled()
    hub_order = await lab.order(order["id"])
    assert (hub_order["status"], hub_order["loyalty_status"]) == ("cancelled", "skipped")
    assert hub_order["outbox"][0]["status"] == "skipped"
    assert await lab.accruals(order["id"]) == []


async def test_concurrent_dispatch_of_one_message_sends_once(lab):
    await lab.faults("loyalty", latency_ms=50)
    order = await lab.paid_order()
    await lab.runtime.processor.run_once()
    [msg] = (await lab.order(order["id"]))["outbox"]
    results = await asyncio.gather(*(lab.runtime.dispatcher.dispatch_one(msg["id"]) for _ in range(4)))
    assert sorted(r for r in results if r is not None) == ["sent"]
    assert len(await lab.accruals(order["id"])) == 1


async def test_expired_in_flight_claim_is_recovered_and_resent_with_same_key(lab):
    order = await lab.paid_order()
    await lab.runtime.processor.run_once()
    async with session_factory()() as s:
        msg_id = await s.scalar(select(OutboxMessage.id).where(OutboxMessage.order_id == order["id"]))
        await s.execute(update(OutboxMessage).where(OutboxMessage.id == msg_id)
                        .values(status="in_flight", attempts=1, claimed_until=lab.clock.now()))
        await s.commit()
    assert await lab.runtime.dispatcher.run_once() == 0
    lab.clock.advance(1)
    assert await lab.runtime.dispatcher.run_once() == 1
    hub_order = await lab.order(order["id"])
    assert hub_order["outbox"][0]["attempts"] == 2 and hub_order["loyalty_status"] == "accrued"


async def test_unconfigured_provider_is_retried_not_lost(tmp_path):
    settings = make_settings(tmp_path, mount_mocks=False, retry_max_attempts=2)
    async with running_hub(settings) as app:
        runtime = app.state.runtime
        async with session_factory()() as s:
            s.add(OutboxMessage(kind="loyalty.accrue", target="loyalty", method="POST", path="/accruals",
                                payload={}, idempotency_key="k1"))
            await s.commit()
        await runtime.dispatcher.run_once()
        async with session_factory()() as s:
            msg = await s.scalar(select(OutboxMessage))
        assert msg.status == "pending" and "LOYALTY_API_URL" in msg.last_error


# -------------------------------------------------------------- ops auth


async def test_operator_writes_require_the_ops_key(lab):
    url = "/v1/dead-letters/1/replay"
    body = {"requested_by": "x"}
    assert (await lab.client.post(url, json=body)).json()["detail"]["code"] == "invalid_ops_key"
    assert (await lab.client.post(url, json=body, headers={"X-Ops-Key": "wrong"})).status_code == 401
    assert (await lab.client.post(url, json=body, headers=lab.ops())).status_code == 404  # authorised, no such DL
    assert (await lab.client.post("/v1/worker/tick", headers=lab.ops())).status_code == 200


async def test_operator_writes_fail_closed_without_a_configured_key(tmp_path):
    settings = make_settings(tmp_path, ops_api_key="")
    async with running_hub(settings) as app, hub_client(app) as client:
        resp = await client.post("/v1/worker/tick", headers={"X-Ops-Key": ""})
        assert resp.status_code == 503 and resp.json()["detail"]["code"] == "ops_key_not_configured"
        assert (await client.get("/health")).json()["ops_key_configured"] is False


async def test_replay_validation(lab):
    assert (await lab.client.post("/v1/dead-letters/1/replay", json={}, headers=lab.ops())).status_code == 422
    assert (await lab.client.post("/v1/dead-letters/1/replay", json={"requested_by": ""},
                                  headers=lab.ops())).status_code == 422
