"""Mock control plane (failure injection) and provider-side webhook signing."""

import json
import time
from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI, Request

from irlab.mocks import MockConfig, create_payments_app, create_pos_app
from irlab.signing import HEADER_TIMESTAMP, verify

ORDER = {"store_id": "ST-IST-01", "items": [{"sku": "SKU-TEA", "qty": 2}]}
SECRET = "capture-test-secret"


@pytest.fixture
async def pos() -> AsyncIterator[httpx.AsyncClient]:
    app = create_pos_app(MockConfig(name="pos"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pos") as client:
        yield client


async def test_fail_next_injects_errors_then_recovers(pos):
    resp = await pos.put("/_admin/faults", json={"fail_next": 2, "fail_status": 503})
    assert resp.json()["fail_next"] == 2

    statuses = [(await pos.post("/orders", json=ORDER)).status_code for _ in range(3)]
    assert statuses == [503, 503, 201]
    assert (await pos.get("/_admin/faults")).json()["fail_next"] == 0


async def test_injected_fault_body_is_identifiable(pos):
    await pos.put("/_admin/faults", json={"fail_next": 1, "fail_status": 500})
    resp = await pos.get("/menu")
    assert resp.status_code == 500
    assert resp.json()["detail"]["code"] == "injected_fault"


async def test_admin_routes_are_never_faulted(pos):
    await pos.put("/_admin/faults", json={"fail_next": 5})
    assert (await pos.get("/_admin/faults")).status_code == 200
    assert (await pos.get("/_admin/webhooks")).status_code == 200
    assert (await pos.get("/health")).status_code == 200


async def test_latency_injection(pos):
    await pos.put("/_admin/faults", json={"latency_ms": 150})
    started = time.perf_counter()
    assert (await pos.get("/stores")).status_code == 200
    assert time.perf_counter() - started >= 0.14


async def test_reset_faults(pos):
    await pos.put("/_admin/faults", json={"fail_next": 3, "latency_ms": 10, "webhook_copies": 0})
    assert (await pos.delete("/_admin/faults")).json() == {
        "fail_next": 0,
        "fail_status": 503,
        "latency_ms": 0,
        "slow_next": 0,
        "slow_response_ms": 0,
        "webhook_copies": 1,
    }


async def test_fault_config_is_validated(pos):
    assert (await pos.put("/_admin/faults", json={"fail_status": 200})).status_code == 422
    assert (await pos.put("/_admin/faults", json={"webhook_copies": 9})).status_code == 422


# --------------------------------------------------------- webhook sending


def capture_app(received: list[dict]) -> FastAPI:
    """Stands in for the hub: records raw deliveries."""
    app = FastAPI()

    @app.post("/hook")
    async def hook(request: Request):
        received.append({"headers": dict(request.headers), "body": await request.body()})
        return {"ok": True}

    return app


def mock_with_capture(factory, received: list[dict]) -> FastAPI:
    target = capture_app(received)
    return factory(
        MockConfig(
            name="test",
            webhook_url="http://capture/hook",
            webhook_secret=SECRET,
            client_factory=lambda: httpx.AsyncClient(transport=httpx.ASGITransport(app=target)),
        )
    )


async def test_deliveries_are_signed_over_raw_body():
    received: list[dict] = []
    app = mock_with_capture(create_pos_app, received)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pos") as client:
        order = (await client.post("/orders", json=ORDER)).json()

    [delivery] = received
    msg_id = verify(delivery["headers"], delivery["body"], [SECRET], tolerance_s=300)
    envelope = json.loads(delivery["body"])
    assert msg_id == envelope["id"]
    assert envelope["resource"]["id"] == order["id"]
    assert delivery["headers"]["content-type"] == "application/json"


async def test_redelivery_keeps_event_id_and_is_freshly_signed():
    received: list[dict] = []
    app = mock_with_capture(create_payments_app, received)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://psp") as client:
        await client.post(
            "/payments",
            json={"order_ref": "POS-1", "amount_minor": 100, "capture": False},
            headers={"Idempotency-Key": "k"},
        )
        [emitted] = (await client.get("/_admin/webhooks")).json()
        event_id = emitted["event"]["id"]
        attempt = (await client.post(f"/_admin/webhooks/{event_id}/deliver")).json()
        assert attempt["status"] == 200
        assert (await client.post("/_admin/webhooks/evt_missing/deliver")).status_code == 404
        [emitted] = (await client.get("/_admin/webhooks")).json()
        assert len(emitted["deliveries"]) == 2

    first, second = received
    assert first["body"] == second["body"]
    assert first["headers"]["webhook-id"] == second["headers"]["webhook-id"] == event_id
    for d in received:
        verify(d["headers"], d["body"], [SECRET], tolerance_s=300)
        assert int(d["headers"][HEADER_TIMESTAMP]) > 0


@pytest.mark.parametrize("copies, expected", [(0, 0), (2, 2), (3, 3)])
async def test_webhook_copies_drops_or_duplicates(copies, expected):
    received: list[dict] = []
    app = mock_with_capture(create_pos_app, received)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pos") as client:
        await client.put("/_admin/faults", json={"webhook_copies": copies})
        await client.post("/orders", json=ORDER)
        [emitted] = (await client.get("/_admin/webhooks")).json()
    assert len(received) == expected
    assert len(emitted["deliveries"]) == expected
    assert len({d["headers"]["webhook-id"] for d in received}) == min(expected, 1)


async def test_unreachable_hub_is_recorded_not_raised():
    def refusing_client() -> httpx.AsyncClient:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    app = create_pos_app(
        MockConfig(name="pos", webhook_url="http://hub/webhooks/pos", webhook_secret=SECRET, client_factory=refusing_client)
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pos") as client:
        assert (await client.post("/orders", json=ORDER)).status_code == 201
        [emitted] = (await client.get("/_admin/webhooks")).json()
    assert emitted["deliveries"][0]["error"] == "ConnectError"
