"""Mock payment provider: authorise, capture (full or partial), and `payment.*` webhooks.

Test payment methods (no real card data, ever):

    test_card_ok        approves
    test_card_declined  creates a payment in status "failed" (failure_code card_declined)

A capture for less than the authorised amount leaves the payment
`partially_captured` — the raw material for the partial-payment scenario.
"""

import uuid
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
    now_iso,
)


class PaymentCreate(BaseModel):
    order_ref: str = Field(min_length=1, max_length=64, description="The merchant's order id (POS order id)")
    amount_minor: int = Field(gt=0, le=10_000_000)
    currency: Literal["TRY"] = "TRY"
    payment_method: Literal["test_card_ok", "test_card_declined"] = "test_card_ok"
    capture: bool = True


class CaptureRequest(BaseModel):
    amount_minor: int | None = Field(default=None, gt=0, description="Omit to capture the full authorised amount")


def create_payments_app(config: MockConfig | None = None) -> FastAPI:
    state = MockState(config or MockConfig(name="payments"))
    payments: dict[str, dict[str, Any]] = {}
    router = business_router()

    async def _emit(event_type: str, payment: dict[str, Any]) -> None:
        await state.emitter.emit(event_type, "payment", payment["id"], payment["version"], dict(payment))

    @router.post("/payments", status_code=201, tags=["payments"], summary="Create a payment (Idempotency-Key required)")
    async def create_payment(body: PaymentCreate, request: Request, idempotency_key: IdempotencyKey = None):
        async def handler() -> tuple[int, Any]:
            ts = now_iso()
            declined = body.payment_method == "test_card_declined"
            payment = {
                "id": f"pay_{uuid.uuid4().hex[:16]}",
                "order_ref": body.order_ref,
                "amount_minor": body.amount_minor,
                "captured_minor": 0,
                "currency": body.currency,
                "payment_method": body.payment_method,
                "status": "failed" if declined else "authorized",
                "failure_code": "card_declined" if declined else None,
                "version": 1,
                "created_at": ts,
                "updated_at": ts,
            }
            payments[payment["id"]] = payment
            if declined:
                await _emit("payment.failed", payment)
                return 201, payment
            await _emit("payment.authorized", payment)
            if body.capture:
                _capture(payment, body.amount_minor)
                await _emit("payment.captured", payment)
            return 201, payment

        return await state.idempotency.run(
            request, idempotency_key, body.model_dump(mode="json"), handler, required=True
        )

    def _capture(payment: dict[str, Any], amount: int) -> None:
        payment["captured_minor"] = amount
        payment["status"] = "captured" if amount == payment["amount_minor"] else "partially_captured"
        payment["version"] += 1
        payment["updated_at"] = now_iso()

    @router.post("/payments/{payment_id}/capture", tags=["payments"], summary="Capture (Idempotency-Key required)")
    async def capture_payment(
        payment_id: str, body: CaptureRequest, request: Request, idempotency_key: IdempotencyKey = None
    ):
        async def handler() -> tuple[int, Any]:
            payment = payments.get(payment_id)
            if payment is None:
                raise error(404, "payment_not_found", f"No payment {payment_id}")
            if payment["status"] != "authorized":
                raise error(409, "payment_not_capturable", f"Payment is {payment['status']}")
            amount = body.amount_minor or payment["amount_minor"]
            if amount > payment["amount_minor"]:
                raise error(422, "amount_exceeds_authorized", "Capture amount is larger than the authorised amount")
            _capture(payment, amount)
            await _emit("payment.captured", payment)
            return 200, payment

        return await state.idempotency.run(
            request, idempotency_key, body.model_dump(mode="json"), handler, required=True
        )

    @router.get("/payments/{payment_id}", tags=["payments"])
    async def get_payment(payment_id: str):
        payment = payments.get(payment_id)
        if payment is None:
            raise error(404, "payment_not_found", f"No payment {payment_id}")
        return payment

    @router.get("/payments", tags=["payments"])
    async def list_payments(
        order_ref: str | None = None,
        status: str | None = None,
        created_after: datetime | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ):
        found = [
            p
            for p in payments.values()
            if (order_ref is None or p["order_ref"] == order_ref) and (status is None or p["status"] == status)
        ]
        return created_after_filter(found, created_after)[-limit:]

    return build_mock_app(
        state,
        title="Mock payment provider",
        description="Synthetic payment provider for the Integration Reliability Lab. Test methods only.",
        routers=[router],
    )
