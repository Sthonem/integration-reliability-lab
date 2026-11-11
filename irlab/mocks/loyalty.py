"""Mock loyalty platform: members, point accruals, and `points.*` webhooks.

Accruals do *not* deduplicate on order_ref: like many real loyalty APIs, the only
protection against double points is the `Idempotency-Key`. A caller that
generates a new key per retry will double-credit the member, which M2's outbox
dispatcher is tested against.

`PUT /_admin/members/{id}` creates or restores a member, which is how the
missing-customer-record scenario gets "fixed" before a replay.
"""

import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, FastAPI, Query, Request
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
from irlab.mocks.seed import loyalty_members

POINTS_PER_TRY = 1  # 1 point per full TRY spent


class AccrualCreate(BaseModel):
    member_id: str = Field(min_length=1, max_length=32)
    order_ref: str = Field(min_length=1, max_length=64)
    amount_minor: int = Field(gt=0, le=10_000_000)
    currency: str = Field("TRY", pattern=r"^TRY$")


class MemberUpsert(BaseModel):
    full_name: str = Field(min_length=1, max_length=100)
    phone: str = Field(min_length=8, max_length=20)
    email: str = Field(min_length=3, max_length=100)
    tier: str = "bronze"
    points_balance: int = Field(0, ge=0)


def create_loyalty_app(config: MockConfig | None = None) -> FastAPI:
    state = MockState(config or MockConfig(name="loyalty"))
    members = loyalty_members()
    accruals: dict[str, dict[str, Any]] = {}
    router = business_router()

    def _member(member_id: str) -> dict[str, Any]:
        member = members.get(member_id)
        if member is None:
            raise error(404, "member_not_found", f"No loyalty member {member_id}")
        return member

    @router.get("/members/{member_id}", tags=["loyalty"])
    async def get_member(member_id: str):
        return _member(member_id)

    @router.get("/members", tags=["loyalty"])
    async def find_members(phone: str | None = None):
        return [m for m in members.values() if phone is None or m["phone"] == phone]

    @router.post("/accruals", status_code=201, tags=["loyalty"], summary="Award points (Idempotency-Key required)")
    async def create_accrual(body: AccrualCreate, request: Request, idempotency_key: IdempotencyKey = None):
        async def handler() -> tuple[int, Any]:
            member = _member(body.member_id)
            points = body.amount_minor // 100 * POINTS_PER_TRY
            member["points_balance"] += points
            member["version"] += 1
            accrual = {
                "id": f"acc_{uuid.uuid4().hex[:16]}",
                "member_id": member["id"],
                "order_ref": body.order_ref,
                "amount_minor": body.amount_minor,
                "points": points,
                "balance_after": member["points_balance"],
                "created_at": now_iso(),
            }
            accruals[accrual["id"]] = accrual
            await state.emitter.emit("points.accrued", "member", member["id"], member["version"], dict(accrual))
            return 201, accrual

        return await state.idempotency.run(
            request, idempotency_key, body.model_dump(mode="json"), handler, required=True
        )

    @router.get("/accruals", tags=["loyalty"])
    async def list_accruals(
        order_ref: str | None = None,
        member_id: str | None = None,
        created_after: datetime | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ):
        found = [
            a
            for a in accruals.values()
            if (order_ref is None or a["order_ref"] == order_ref) and (member_id is None or a["member_id"] == member_id)
        ]
        return created_after_filter(found, created_after)[-limit:]

    admin_members = APIRouter(prefix="/_admin", tags=["mock control plane"])

    @admin_members.put("/members/{member_id}", summary="Create or restore a member (scenario fix-up)")
    async def upsert_member(member_id: str, body: MemberUpsert):
        existing = members.get(member_id)
        version = existing["version"] + 1 if existing else 1
        members[member_id] = {"id": member_id, **body.model_dump(), "version": version}
        return members[member_id]

    @admin_members.delete("/members/{member_id}", summary="Delete a member (missing-record scenario)")
    async def delete_member(member_id: str):
        _member(member_id)
        return members.pop(member_id)

    return build_mock_app(
        state,
        title="Mock loyalty platform",
        description="Synthetic loyalty platform for the Integration Reliability Lab. Not a real system.",
        routers=[router, admin_members],
    )
