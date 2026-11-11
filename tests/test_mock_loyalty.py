from collections.abc import AsyncIterator

import httpx
import pytest

from irlab.mocks import MockConfig, create_loyalty_app


@pytest.fixture
async def loyalty() -> AsyncIterator[httpx.AsyncClient]:
    app = create_loyalty_app(MockConfig(name="loyalty"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://loyalty") as client:
        yield client


ACCRUAL = {"member_id": "LM-1002", "order_ref": "POS-000010", "amount_minor": 22550}


async def test_member_lookup(loyalty):
    member = (await loyalty.get("/members/LM-1001")).json()
    assert member["email"].endswith("@example.com")
    assert (await loyalty.get("/members", params={"phone": "+905550101003"})).json()[0]["id"] == "LM-1003"
    missing = await loyalty.get("/members/LM-9999")
    assert (missing.status_code, missing.json()["detail"]["code"]) == (404, "member_not_found")


async def test_accrual_awards_one_point_per_full_try(loyalty):
    before = (await loyalty.get("/members/LM-1002")).json()["points_balance"]
    resp = await loyalty.post("/accruals", json=ACCRUAL, headers={"Idempotency-Key": "outbox-1"})
    assert resp.status_code == 201
    assert resp.json()["points"] == 225  # 225.50 TRY → 225 points
    assert (await loyalty.get("/members/LM-1002")).json()["points_balance"] == before + 225

    [emitted] = (await loyalty.get("/_admin/webhooks")).json()
    assert emitted["event"]["type"] == "points.accrued"
    assert emitted["event"]["resource"] == {"type": "member", "id": "LM-1002"}
    assert emitted["event"]["sequence"] == 2


async def test_same_key_credits_once_new_key_per_retry_double_credits(loyalty):
    """Why the outbox (M2) must reuse one key per message across retries."""
    before = (await loyalty.get("/members/LM-1002")).json()["points_balance"]

    for _ in range(3):
        await loyalty.post("/accruals", json=ACCRUAL, headers={"Idempotency-Key": "outbox-msg-7"})
    assert (await loyalty.get("/members/LM-1002")).json()["points_balance"] == before + 225

    await loyalty.post("/accruals", json=ACCRUAL, headers={"Idempotency-Key": "retry-a"})
    await loyalty.post("/accruals", json=ACCRUAL, headers={"Idempotency-Key": "retry-b"})
    assert (await loyalty.get("/members/LM-1002")).json()["points_balance"] == before + 3 * 225
    assert len((await loyalty.get("/accruals", params={"order_ref": "POS-000010"})).json()) == 3


async def test_missing_member_is_not_cached_so_retry_after_fix_succeeds(loyalty):
    body = {**ACCRUAL, "member_id": "LM-2001"}
    headers = {"Idempotency-Key": "outbox-msg-9"}

    first = await loyalty.post("/accruals", json=body, headers=headers)
    assert (first.status_code, first.json()["detail"]["code"]) == (404, "member_not_found")

    fixed = await loyalty.put(
        "/_admin/members/LM-2001",
        json={"full_name": "Deniz Arslan", "phone": "+905550102001", "email": "lm-2001@example.com"},
    )
    assert fixed.status_code == 200

    retry = await loyalty.post("/accruals", json=body, headers=headers)
    assert retry.status_code == 201
    assert retry.json()["points"] == 225


async def test_delete_member_simulates_missing_record(loyalty):
    assert (await loyalty.delete("/_admin/members/LM-1005")).status_code == 200
    assert (await loyalty.get("/members/LM-1005")).status_code == 404
    assert (await loyalty.delete("/_admin/members/LM-1005")).status_code == 404


async def test_accrual_requires_key_and_valid_currency(loyalty):
    assert (await loyalty.post("/accruals", json=ACCRUAL)).status_code == 400
    bad = await loyalty.post("/accruals", json={**ACCRUAL, "currency": "USD"}, headers={"Idempotency-Key": "x"})
    assert bad.status_code == 422


async def test_list_accruals_by_member(loyalty):
    await loyalty.post("/accruals", json=ACCRUAL, headers={"Idempotency-Key": "a"})
    await loyalty.post("/accruals", json={**ACCRUAL, "member_id": "LM-1003"}, headers={"Idempotency-Key": "b"})
    assert len((await loyalty.get("/accruals", params={"member_id": "LM-1003"})).json()) == 1
