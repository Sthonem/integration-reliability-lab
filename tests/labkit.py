"""Shared harness for tests that drive the hub with the mocks mounted in-process."""

import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx

from irlab.clock import FakeClock
from tests.conftest import hub_client, make_settings, make_store, running_hub

LATTE = [{"sku": "SKU-LATTE", "qty": 2}]  # 190.00 TRY


class Lab:
    def __init__(self, app, client: httpx.AsyncClient, clock: FakeClock):
        self.app = app
        self.client = client
        self.clock = clock
        self.runtime = app.state.runtime
        self.settings = app.state.settings

    def ops(self) -> dict[str, str]:
        return {"X-Ops-Key": self.settings.ops_api_key.get_secret_value()}

    async def faults(self, provider: str, **faults: Any) -> None:
        resp = await self.client.put(f"/mock/{provider}/_admin/faults", json=faults)
        assert resp.status_code == 200, resp.text

    async def pos_order(self, member: str | None = "LM-1002", items: list | None = None) -> dict[str, Any]:
        resp = await self.client.post("/mock/pos/orders", json={"store_id": "ST-IST-01", "items": items or LATTE,
                                                                "loyalty_member_id": member})
        assert resp.status_code == 201, resp.text
        return resp.json()

    async def pay(self, order: dict[str, Any], amount: int | None = None, capture_amount: int | None = None,
                  key: str | None = None) -> dict[str, Any]:
        amount = amount or order["total_minor"]
        resp = await self.client.post(
            "/mock/payments/payments",
            json={"order_ref": order["id"], "amount_minor": amount, "capture": capture_amount is None},
            headers={"Idempotency-Key": key or f"pay-{order['id']}-{amount}"},
        )
        assert resp.status_code == 201, resp.text
        payment = resp.json()
        if capture_amount is not None:
            resp = await self.client.post(f"/mock/payments/payments/{payment['id']}/capture",
                                          json={"amount_minor": capture_amount},
                                          headers={"Idempotency-Key": f"cap-{payment['id']}"})
            assert resp.status_code == 200, resp.text
            payment = resp.json()
        return payment

    async def paid_order(self, member: str | None = "LM-1002") -> dict[str, Any]:
        order = await self.pos_order(member)
        await self.pay(order)
        return order

    async def tick(self) -> dict:
        return await self.runtime.worker.tick(periodic=False)

    async def reconcile(self, **kw: Any) -> dict[str, Any]:
        self.clock.advance(1)  # past the (zero) grace period relative to the mocks' wall clock
        return await self.runtime.reconciler.run(triggered_by="test", **kw)

    async def get(self, path: str, **params: Any) -> Any:
        resp = await self.client.get(path, params=params)
        assert resp.status_code == 200, resp.text
        return resp.json()

    async def mismatches(self, **params: Any) -> list[dict[str, Any]]:
        return await self.get("/v1/mismatches", **params)

    async def accruals(self, order_id: str) -> list[dict[str, Any]]:
        return await self.get("/mock/loyalty/accruals", order_ref=order_id)


@asynccontextmanager
async def open_lab(tmp_path, store_kind: str = "memory", **overrides: Any) -> AsyncIterator[Lab]:
    clock = FakeClock()
    settings = make_settings(tmp_path, mount_mocks=True, **overrides)
    async with (
        running_hub(settings, make_store(store_kind), clock=clock, rng=random.Random(11)) as app,
        hub_client(app) as client,
    ):
        yield Lab(app, client, clock)
