"""Mock store POS: menu, orders, and `order.*` webhooks.

Prices are decided by the POS from its own menu (never by the caller), amounts are
integer minor units (kuruş), and every change bumps the order's `version`, which
is sent as the webhook `sequence`.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import FastAPI, Query, Request
from pydantic import BaseModel, Field

from irlab.mocks.common import (
    IdempotencyKey,
    MockConfig,
    MockState,
    build_mock_app,
    business_router,
    created_after_filter,
    error,
    get_state,
    now_iso,
)
from irlab.mocks.seed import POS_MENU, POS_STORES

CURRENCY = "TRY"


class OrderItemIn(BaseModel):
    sku: str = Field(min_length=1, max_length=32)
    qty: int = Field(ge=1, le=50)


class OrderCreate(BaseModel):
    store_id: str = Field(min_length=1, max_length=32)
    items: list[OrderItemIn] = Field(min_length=1, max_length=50)
    loyalty_member_id: str | None = Field(default=None, max_length=32)
    channel: Literal["in_store", "takeaway", "delivery"] = "in_store"


@dataclass
class PosStore:
    orders: dict[str, dict[str, Any]] = field(default_factory=dict)
    counter: int = 0


def _pos(request: Request) -> PosStore:
    return request.app.state.pos


def create_pos_app(config: MockConfig | None = None) -> FastAPI:
    state = MockState(config or MockConfig(name="pos"))
    router = business_router()

    @router.get("/stores", tags=["pos"])
    async def list_stores():
        return list(POS_STORES.values())

    @router.get("/menu", tags=["pos"])
    async def list_menu():
        return list(POS_MENU.values())

    @router.post("/orders", status_code=201, tags=["pos"], summary="Ring up an order (Idempotency-Key optional)")
    async def create_order(body: OrderCreate, request: Request, idempotency_key: IdempotencyKey = None):
        async def handler() -> tuple[int, Any]:
            if body.store_id not in POS_STORES:
                raise error(404, "store_not_found", f"Unknown store {body.store_id}")
            lines = []
            for item in body.items:
                product = POS_MENU.get(item.sku)
                if product is None:
                    raise error(422, "unknown_sku", f"Unknown SKU {item.sku}")
                lines.append(
                    {
                        "sku": item.sku,
                        "name": product["name"],
                        "qty": item.qty,
                        "unit_price_minor": product["price_minor"],
                        "line_total_minor": product["price_minor"] * item.qty,
                    }
                )
            pos = _pos(request)
            pos.counter += 1
            ts = now_iso()
            order = {
                "id": f"POS-{pos.counter:06d}",
                "store_id": body.store_id,
                "status": "placed",
                "channel": body.channel,
                "lines": lines,
                "total_minor": sum(line["line_total_minor"] for line in lines),
                "currency": CURRENCY,
                "loyalty_member_id": body.loyalty_member_id,
                "version": 1,
                "created_at": ts,
                "updated_at": ts,
            }
            pos.orders[order["id"]] = order
            await state.emitter.emit("order.created", "order", order["id"], order["version"], dict(order))
            return 201, order

        return await state.idempotency.run(
            request, idempotency_key, body.model_dump(mode="json"), handler, required=False
        )

    @router.get("/orders", tags=["pos"])
    async def list_orders(
        request: Request,
        store_id: str | None = None,
        status: Literal["placed", "cancelled"] | None = None,
        created_after: datetime | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ):
        orders = [
            o
            for o in _pos(request).orders.values()
            if (store_id is None or o["store_id"] == store_id) and (status is None or o["status"] == status)
        ]
        return created_after_filter(orders, created_after)[-limit:]

    @router.get("/orders/{order_id}", tags=["pos"])
    async def get_order(order_id: str, request: Request):
        order = _pos(request).orders.get(order_id)
        if order is None:
            raise error(404, "order_not_found", f"No order {order_id}")
        return order

    @router.post("/orders/{order_id}/cancel", tags=["pos"])
    async def cancel_order(order_id: str, request: Request):
        order = _pos(request).orders.get(order_id)
        if order is None:
            raise error(404, "order_not_found", f"No order {order_id}")
        if order["status"] == "cancelled":
            raise error(409, "order_already_cancelled", f"Order {order_id} is already cancelled")
        order["status"] = "cancelled"
        order["version"] += 1
        order["updated_at"] = now_iso()
        await get_state(request).emitter.emit("order.cancelled", "order", order_id, order["version"], dict(order))
        return order

    app = build_mock_app(
        state,
        title="Mock POS",
        description="Synthetic store POS for the Integration Reliability Lab. Not a real system.",
        routers=[router],
    )
    app.state.pos = PosStore()
    return app
