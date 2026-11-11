import uuid
from collections.abc import AsyncIterator

import httpx
import pytest

from irlab.mocks import MockConfig, create_payments_app


@pytest.fixture
async def psp() -> AsyncIterator[httpx.AsyncClient]:
    app = create_payments_app(MockConfig(name="payments"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://psp") as client:
        yield client


def key() -> dict[str, str]:
    return {"Idempotency-Key": f"test-{uuid.uuid4()}"}


async def event_types(client) -> list[tuple[str, int]]:
    rows = (await client.get("/_admin/webhooks")).json()
    return [(r["event"]["type"], r["event"]["sequence"]) for r in reversed(rows)]


async def test_idempotency_key_is_required(psp):
    resp = await psp.post("/payments", json={"order_ref": "POS-000001", "amount_minor": 22500})
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "idempotency_key_required"


async def test_auto_capture_emits_authorized_then_captured(psp):
    resp = await psp.post("/payments", json={"order_ref": "POS-000001", "amount_minor": 22500}, headers=key())
    assert resp.status_code == 201
    p = resp.json()
    assert p["id"].startswith("pay_")
    assert (p["status"], p["captured_minor"], p["version"]) == ("captured", 22500, 2)
    assert await event_types(psp) == [("payment.authorized", 1), ("payment.captured", 2)]


async def test_partial_capture(psp):
    p = (
        await psp.post(
            "/payments", json={"order_ref": "POS-000002", "amount_minor": 20000, "capture": False}, headers=key()
        )
    ).json()
    assert p["status"] == "authorized"

    resp = await psp.post(f"/payments/{p['id']}/capture", json={"amount_minor": 15000}, headers=key())
    assert resp.status_code == 200
    assert (resp.json()["status"], resp.json()["captured_minor"]) == ("partially_captured", 15000)
    events = (await psp.get("/_admin/webhooks")).json()
    assert events[0]["event"]["data"]["captured_minor"] == 15000
    assert events[0]["event"]["data"]["amount_minor"] == 20000


async def test_capture_rules(psp):
    p = (
        await psp.post("/payments", json={"order_ref": "O", "amount_minor": 1000, "capture": False}, headers=key())
    ).json()
    too_much = await psp.post(f"/payments/{p['id']}/capture", json={"amount_minor": 1001}, headers=key())
    assert too_much.status_code == 422
    assert (await psp.post(f"/payments/{p['id']}/capture", json={}, headers=key())).status_code == 200
    twice = await psp.post(f"/payments/{p['id']}/capture", json={}, headers=key())
    assert (twice.status_code, twice.json()["detail"]["code"]) == (409, "payment_not_capturable")
    missing = await psp.post("/payments/pay_nope/capture", json={}, headers=key())
    assert missing.status_code == 404


async def test_declined_card_creates_failed_payment(psp):
    resp = await psp.post(
        "/payments",
        json={"order_ref": "POS-000003", "amount_minor": 5000, "payment_method": "test_card_declined"},
        headers=key(),
    )
    assert resp.status_code == 201
    assert (resp.json()["status"], resp.json()["failure_code"]) == ("failed", "card_declined")
    assert await event_types(psp) == [("payment.failed", 1)]


async def test_retry_with_same_key_does_not_charge_twice(psp):
    headers = key()
    body = {"order_ref": "POS-000004", "amount_minor": 9500}
    first = await psp.post("/payments", json=body, headers=headers)
    second = await psp.post("/payments", json=body, headers=headers)
    assert first.json()["id"] == second.json()["id"]
    assert second.headers["idempotent-replayed"] == "true"
    assert len((await psp.get("/payments", params={"order_ref": "POS-000004"})).json()) == 1


async def test_get_and_list(psp):
    p = (await psp.post("/payments", json={"order_ref": "A", "amount_minor": 100}, headers=key())).json()
    await psp.post("/payments", json={"order_ref": "B", "amount_minor": 100}, headers=key())
    assert (await psp.get(f"/payments/{p['id']}")).json()["order_ref"] == "A"
    assert (await psp.get("/payments/pay_missing")).status_code == 404
    assert len((await psp.get("/payments")).json()) == 2
    assert len((await psp.get("/payments", params={"status": "captured", "order_ref": "B"})).json()) == 1
