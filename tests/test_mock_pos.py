from collections.abc import AsyncIterator

import httpx
import pytest

from irlab.mocks import MockConfig, create_pos_app

ORDER = {"store_id": "ST-IST-01", "items": [{"sku": "SKU-LATTE", "qty": 2}, {"sku": "SKU-SIMIT", "qty": 1}]}


@pytest.fixture
async def pos() -> AsyncIterator[httpx.AsyncClient]:
    app = create_pos_app(MockConfig(name="pos"))  # no webhook target: events are recorded only
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pos") as client:
        yield client


async def webhooks(client):
    return (await client.get("/_admin/webhooks")).json()


async def test_reference_data(pos):
    stores = (await pos.get("/stores")).json()
    menu = (await pos.get("/menu")).json()
    assert {s["id"] for s in stores} == {"ST-IST-01", "ST-IST-02", "ST-ANK-01"}
    assert all(isinstance(p["price_minor"], int) for p in menu)


async def test_create_order_prices_from_menu_and_emits_event(pos):
    resp = await pos.post("/orders", json={**ORDER, "loyalty_member_id": "LM-1001"})
    assert resp.status_code == 201
    order = resp.json()
    assert order["id"] == "POS-000001"
    assert order["total_minor"] == 2 * 9500 + 3500
    assert order["currency"] == "TRY"
    assert (order["status"], order["version"]) == ("placed", 1)
    assert order["loyalty_member_id"] == "LM-1001"

    [emitted] = await webhooks(pos)
    env = emitted["event"]
    assert env["type"] == "order.created"
    assert env["resource"] == {"type": "order", "id": "POS-000001"}
    assert env["sequence"] == 1
    assert env["data"]["total_minor"] == order["total_minor"]
    assert emitted["deliveries"] == [{"at": emitted["deliveries"][0]["at"], "status": None, "error": "webhook_not_configured"}]


async def test_client_cannot_set_prices(pos):
    resp = await pos.post("/orders", json={**ORDER, "items": [{"sku": "SKU-LATTE", "qty": 1, "unit_price_minor": 1}]})
    assert resp.status_code == 201
    assert resp.json()["total_minor"] == 9500


async def test_unknown_store_and_sku(pos):
    store = await pos.post("/orders", json={**ORDER, "store_id": "ST-XXX"})
    sku = await pos.post("/orders", json={**ORDER, "items": [{"sku": "SKU-PIZZA", "qty": 1}]})
    assert (store.status_code, store.json()["detail"]["code"]) == (404, "store_not_found")
    assert (sku.status_code, sku.json()["detail"]["code"]) == (422, "unknown_sku")
    assert await webhooks(pos) == []


async def test_idempotency_key_replays_same_order(pos):
    headers = {"Idempotency-Key": "till-7-receipt-0042"}
    first = await pos.post("/orders", json=ORDER, headers=headers)
    second = await pos.post("/orders", json=ORDER, headers=headers)
    assert first.status_code == second.status_code == 201
    assert first.json() == second.json()
    assert second.headers["idempotent-replayed"] == "true"
    assert len((await pos.get("/orders")).json()) == 1
    assert len(await webhooks(pos)) == 1  # a replay never re-emits


async def test_idempotency_key_reused_with_different_body_is_422(pos):
    headers = {"Idempotency-Key": "k-1"}
    await pos.post("/orders", json=ORDER, headers=headers)
    resp = await pos.post("/orders", json={**ORDER, "store_id": "ST-ANK-01"}, headers=headers)
    assert resp.status_code == 422
    assert resp.json()["detail"]["code"] == "idempotency_key_reused"


async def test_without_key_each_post_is_a_new_order(pos):
    await pos.post("/orders", json=ORDER)
    await pos.post("/orders", json=ORDER)
    assert [o["id"] for o in (await pos.get("/orders")).json()] == ["POS-000001", "POS-000002"]


async def test_cancel_bumps_version_and_emits(pos):
    order = (await pos.post("/orders", json=ORDER)).json()
    resp = await pos.post(f"/orders/{order['id']}/cancel")
    assert resp.status_code == 200
    assert (resp.json()["status"], resp.json()["version"]) == ("cancelled", 2)

    events = [w["event"] for w in await webhooks(pos)]
    assert [(e["type"], e["sequence"]) for e in events] == [("order.cancelled", 2), ("order.created", 1)]

    again = await pos.post(f"/orders/{order['id']}/cancel")
    assert again.status_code == 409


async def test_get_and_filter_orders(pos):
    a = (await pos.post("/orders", json=ORDER)).json()
    await pos.post("/orders", json={**ORDER, "store_id": "ST-IST-02"})
    await pos.post(f"/orders/{a['id']}/cancel")

    assert (await pos.get(f"/orders/{a['id']}")).json()["status"] == "cancelled"
    assert (await pos.get("/orders/POS-999999")).status_code == 404
    assert len((await pos.get("/orders", params={"store_id": "ST-IST-02"})).json()) == 1
    assert len((await pos.get("/orders", params={"status": "cancelled"})).json()) == 1
