"""Mock providers mounted in the hub deliver real signed webhooks to the real receiver."""

from collections.abc import AsyncIterator

import httpx
import pytest

from tests.conftest import hub_client, make_settings, make_store, running_hub

ORDER = {"store_id": "ST-IST-01", "items": [{"sku": "SKU-LATTE", "qty": 2}], "loyalty_member_id": "LM-1001"}


@pytest.fixture(params=["memory", "redis"])
async def lab(request, tmp_path) -> AsyncIterator[httpx.AsyncClient]:
    settings = make_settings(tmp_path, mount_mocks=True)
    async with running_hub(settings, make_store(request.param)) as app, hub_client(app) as client:
        yield client


async def inbox(client, **params):
    return (await client.get("/v1/inbox", params=params)).json()


async def test_pos_order_reaches_hub_inbox(lab):
    order = (await lab.post("/mock/pos/orders", json=ORDER)).json()

    [row] = await inbox(lab)
    assert (row["source"], row["event_type"], row["resource_id"]) == ("pos", "order.created", order["id"])
    [emitted] = (await lab.get("/mock/pos/_admin/webhooks")).json()
    assert emitted["deliveries"][0]["status"] == 202
    assert emitted["deliveries"][0]["response"]["status"] == "accepted"


async def test_provider_retry_is_deduplicated(lab):
    """Failure scenario 1 (duplicate webhook), receiver side."""
    await lab.post("/mock/pos/orders", json=ORDER)
    [emitted] = (await lab.get("/mock/pos/_admin/webhooks")).json()

    attempt = (await lab.post(f"/mock/pos/_admin/webhooks/{emitted['event']['id']}/deliver")).json()
    assert attempt["status"] == 200
    assert attempt["response"]["status"] == "duplicate"

    [row] = await inbox(lab)
    assert row["delivery_count"] == 2


async def test_duplicate_storm_from_provider(lab):
    await lab.put("/mock/pos/_admin/faults", json={"webhook_copies": 3})
    for _ in range(4):
        await lab.post("/mock/pos/orders", json=ORDER)

    rows = await inbox(lab)
    assert len(rows) == 4
    assert all(r["delivery_count"] == 3 for r in rows)
    duplicates = (await lab.get("/v1/webhook-receipts", params={"outcome": "duplicate", "limit": 500})).json()
    assert len(duplicates) == 8


async def test_payment_and_loyalty_events_flow_in(lab):
    order = (await lab.post("/mock/pos/orders", json=ORDER)).json()
    payment = (
        await lab.post(
            "/mock/payments/payments",
            json={"order_ref": order["id"], "amount_minor": order["total_minor"], "capture": False},
            headers={"Idempotency-Key": f"capture-{order['id']}"},
        )
    ).json()
    await lab.post(
        f"/mock/payments/payments/{payment['id']}/capture",
        json={"amount_minor": order["total_minor"] - 5000},  # partial capture
        headers={"Idempotency-Key": f"capture-{payment['id']}"},
    )
    await lab.post(
        "/mock/loyalty/accruals",
        json={"member_id": "LM-1001", "order_ref": order["id"], "amount_minor": order["total_minor"]},
        headers={"Idempotency-Key": f"accrual-{order['id']}"},
    )

    payments = await inbox(lab, source="payments")
    assert [(r["event_type"], r["sequence"]) for r in reversed(payments)] == [
        ("payment.authorized", 1),
        ("payment.captured", 2),
    ]
    captured = (await lab.get(f"/v1/inbox/{payments[0]['id']}")).json()
    assert captured["payload"]["data"]["status"] == "partially_captured"
    [points] = await inbox(lab, source="loyalty")
    assert points["event_type"] == "points.accrued"


async def test_dropped_webhook_leaves_no_trace_in_hub(lab):
    """The gap the M3 reconciliation job exists to find."""
    await lab.put("/mock/pos/_admin/faults", json={"webhook_copies": 0})
    order = (await lab.post("/mock/pos/orders", json=ORDER)).json()
    assert (await lab.get(f"/mock/pos/orders/{order['id']}")).status_code == 200
    assert await inbox(lab) == []


async def test_health_with_mounted_mocks(lab):
    health = (await lab.get("/health")).json()
    assert health["mocks_mounted"] is True
    for source in ("pos", "payments", "loyalty"):
        mock_health = (await lab.get(f"/mock/{source}/health")).json()
        assert mock_health["webhooks_configured"] is True
