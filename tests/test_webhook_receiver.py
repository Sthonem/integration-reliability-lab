"""Webhook receiver: authentication, validation, duplicate handling, failure behaviour.

Every test in this module runs twice: with the in-memory and with the Redis
(fakeredis) idempotency store.
"""

import asyncio
import json
import logging
import time

import pytest

from irlab.idempotency import InMemoryIdempotencyStore
from tests.conftest import Sender, hub_client, make_event, make_settings, make_store, running_hub


async def inbox(hub, **params):
    resp = await hub.get("/v1/inbox", params=params)
    assert resp.status_code == 200
    return resp.json()


async def receipts(hub, **params):
    resp = await hub.get("/v1/webhook-receipts", params=params)
    assert resp.status_code == 200
    return resp.json()


# ------------------------------------------------------------- happy path


async def test_signed_event_is_accepted_and_stored(hub, sender):
    event = make_event("order.created", ("order", "POS-000042"), sequence=1)
    resp = await sender.send("pos", event)

    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["event_id"] == event["id"]
    assert body["inbox_status"] == "received"

    [row] = await inbox(hub)
    assert row["source"] == "pos"
    assert row["event_type"] == "order.created"
    assert (row["resource_type"], row["resource_id"], row["sequence"]) == ("order", "POS-000042", 1)
    assert row["delivery_count"] == 1
    assert row["status"] == "received"

    detail = (await hub.get(f"/v1/inbox/{row['id']}")).json()
    assert detail["payload"] == event
    assert len(detail["payload_sha256"]) == 64


@pytest.mark.parametrize(
    "source, event_type, resource",
    [
        ("pos", "order.cancelled", ("order", "POS-000001")),
        ("payments", "payment.captured", ("payment", "pay_abc")),
        ("loyalty", "points.accrued", ("member", "LM-1001")),
    ],
)
async def test_each_source_has_its_own_endpoint(hub, sender, source, event_type, resource):
    resp = await sender.send(source, make_event(event_type, resource))
    assert resp.status_code == 202
    [row] = await inbox(hub, source=source)
    assert row["event_type"] == event_type


# ------------------------------------------------------ duplicate handling


async def test_duplicate_delivery_is_acknowledged_not_stored_twice(hub, sender):
    event = make_event()
    first = await sender.send("pos", event)
    # Provider retry: same event id, fresh timestamp/signature.
    second = await sender.send("pos", event, timestamp=int(time.time()) + 1)

    assert first.status_code == 202
    assert second.status_code == 200
    assert second.json() == {
        "status": "duplicate",
        "event_id": event["id"],
        "inbox_id": first.json()["inbox_id"],
        "payload_matches": True,
    }
    [row] = await inbox(hub)
    assert row["delivery_count"] == 2
    assert row["conflicting_deliveries"] == 0
    assert [r["outcome"] for r in await receipts(hub)] == ["duplicate", "accepted"]


async def test_many_redeliveries_count_but_never_duplicate_rows(hub, sender):
    event = make_event()
    statuses = [(await sender.send("pos", event)).status_code for _ in range(5)]
    assert statuses == [202, 200, 200, 200, 200]
    [row] = await inbox(hub)
    assert row["delivery_count"] == 5


async def test_same_id_on_different_sources_are_different_events(hub, sender):
    shared_id = "evt_shared_123"
    a = await sender.send("pos", make_event(event_id=shared_id))
    b = await sender.send("payments", make_event("payment.captured", ("payment", "pay_1"), event_id=shared_id))
    assert (a.status_code, b.status_code) == (202, 202)
    assert len(await inbox(hub)) == 2


async def test_reused_event_id_with_different_payload_is_flagged_and_first_kept(hub, sender):
    event = make_event(data={"total_minor": 19000})
    await sender.send("pos", event)
    tampered = {**event, "data": {"total_minor": 1}}
    resp = await sender.send("pos", tampered)

    assert resp.status_code == 200
    assert resp.json()["payload_matches"] is False
    assert resp.json()["reason"] == "payload_differs_from_first_delivery"
    [row] = await inbox(hub)
    assert row["conflicting_deliveries"] == 1
    detail = (await hub.get(f"/v1/inbox/{row['id']}")).json()
    assert detail["payload"]["data"] == {"total_minor": 19000}


async def test_reserialised_but_equal_payload_still_matches(hub, sender):
    event = make_event()
    await sender.send("pos", event)
    pretty = json.dumps(event, indent=2, sort_keys=True).encode()  # same JSON, different bytes
    resp = await sender.send("pos", event, body=pretty)
    assert resp.status_code == 200
    assert resp.json()["payload_matches"] is True


async def test_concurrent_duplicate_deliveries_store_exactly_one(hub, sender):
    event = make_event()
    results = await asyncio.gather(*(sender.send("pos", event) for _ in range(5)))
    statuses = sorted(r.status_code for r in results)

    assert statuses.count(202) == 1
    # The rest either saw it done (200) or were told to retry while it was in flight (409).
    assert set(statuses) <= {200, 202, 409}
    for r in results:
        if r.status_code == 409:
            assert r.headers["retry-after"] == "5"
    retry = await sender.send("pos", event)
    assert retry.status_code == 200
    [row] = await inbox(hub)
    assert row["id"] == next(r.json()["inbox_id"] for r in results if r.status_code == 202)


async def test_delivery_while_claim_is_held_gets_409_retry_after(hub_app, hub, sender):
    event = make_event()
    ingestor = hub_app.state.ingestor
    await ingestor.store.begin(ingestor.idempotency_key("pos", event["id"]), 30)

    resp = await sender.send("pos", event)
    assert resp.status_code == 409
    assert resp.headers["retry-after"] == "5"
    assert resp.json()["status"] == "in_progress"
    assert await inbox(hub) == []


async def test_storage_failure_releases_claim_so_retry_is_accepted(hub_app, hub, sender, monkeypatch):
    ingestor = hub_app.state.ingestor
    real_insert = ingestor._insert
    calls = {"n": 0}

    async def flaky_insert(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("database unavailable")
        return await real_insert(*args, **kwargs)

    monkeypatch.setattr(ingestor, "_insert", flaky_insert)
    event = make_event()

    failed = await sender.send("pos", event)
    assert failed.status_code == 503
    assert failed.json()["reason"] == "storage_unavailable"
    assert failed.headers["retry-after"] == "5"

    retried = await sender.send("pos", event)  # must not be told "in progress" or "duplicate"
    assert retried.status_code == 202
    assert len(await inbox(hub)) == 1
    assert [r["outcome"] for r in await receipts(hub)] == ["accepted", "error"]


# ---------------------------------------------------- authentication


async def test_tampered_body_is_rejected_and_not_stored(hub, sender):
    event = make_event(data={"total_minor": 19000})
    signed = json.dumps(event).encode()
    tampered = json.dumps({**event, "data": {"total_minor": 1}}).encode()

    resp = await sender.send("pos", event, body=tampered, signed_body=signed)
    assert resp.status_code == 401
    assert resp.json() == {"status": "rejected", "reason": "invalid_signature"}
    assert await inbox(hub) == []
    [receipt] = await receipts(hub)
    assert receipt["outcome"] == "rejected"
    assert receipt["event_id"] is None  # unauthenticated ids are not recorded
    assert receipt["body_bytes"] == len(tampered)


async def test_secret_of_another_source_is_rejected(hub, sender):
    resp = await sender.send("pos", make_event(), secret=sender.secret("payments"))
    assert resp.status_code == 401


@pytest.mark.parametrize("skew", [-600, 600])
async def test_replayed_or_future_timestamp_is_rejected(hub, sender, skew):
    resp = await sender.send("pos", make_event(), timestamp=int(time.time()) + skew)
    assert resp.status_code == 401
    assert resp.json()["reason"] == "timestamp_out_of_tolerance"


async def test_missing_signature_headers_is_400(hub):
    resp = await hub.post("/webhooks/pos", content=json.dumps(make_event()).encode())
    assert resp.status_code == 400
    assert resp.json()["reason"] == "missing_signature_headers"


async def test_unknown_source_is_404_and_not_recorded(hub, sender):
    resp = await sender.send("erp", make_event(), secret="whatever")
    assert resp.status_code == 404
    assert resp.json()["reason"] == "unknown_source"
    assert await receipts(hub) == []


# ------------------------------------------------------- validation


async def test_signed_but_invalid_json_is_400(hub, sender):
    resp = await sender.send("pos", make_event(event_id="evt_x"), body=b"{not json")
    assert resp.status_code == 400
    assert resp.json()["reason"] == "invalid_json"


async def test_signed_but_invalid_envelope_is_400(hub, sender):
    event = make_event()
    del event["resource"]
    resp = await sender.send("pos", event)
    assert resp.status_code == 400
    assert resp.json()["reason"] == "invalid_envelope"


async def test_naive_occurred_at_is_rejected(hub, sender):
    event = make_event()
    event["occurred_at"] = "2026-11-08T10:00:00"
    resp = await sender.send("pos", event)
    assert resp.status_code == 400


async def test_header_id_must_match_envelope_id(hub, sender):
    event = make_event()
    resp = await sender.send("pos", event, msg_id="evt_someone_else")
    assert resp.status_code == 400
    assert resp.json()["reason"] == "event_id_mismatch"
    assert await inbox(hub) == []


async def test_unknown_event_type_is_acknowledged_and_ignored(hub, sender):
    resp = await sender.send("pos", make_event("shift.closed", ("shift", "SH-1")))
    assert resp.status_code == 202
    assert resp.json()["inbox_status"] == "ignored"
    [row] = await inbox(hub, status="ignored")
    assert row["event_type"] == "shift.closed"


async def test_inbox_filters_and_404(hub, sender):
    await sender.send("pos", make_event("order.created", ("order", "POS-1")))
    await sender.send("pos", make_event("order.cancelled", ("order", "POS-1"), sequence=2))
    await sender.send("payments", make_event("payment.captured", ("payment", "pay_9")))
    assert len(await inbox(hub, source="pos")) == 2
    assert len(await inbox(hub, event_type="order.cancelled")) == 1
    assert len(await inbox(hub, resource_id="pay_9")) == 1
    assert len(await inbox(hub, limit=1)) == 1
    missing = await hub.get("/v1/inbox/99999")
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "inbox_event_not_found"


# --------------------------------------------- configuration edge cases
# These build their own hub, so they do not use the parametrised `hub` fixture.


async def test_unconfigured_source_fails_closed(tmp_path):
    settings = make_settings(tmp_path, loyalty_webhook_secrets="")
    async with running_hub(settings) as app, hub_client(app) as client:
        sender = Sender(client, settings)
        resp = await sender.send("loyalty", make_event("points.accrued"), secret="anything")
        assert resp.status_code == 503
        assert resp.json()["reason"] == "source_not_configured"
        health = (await client.get("/health")).json()
        assert health["webhook_sources_configured"] == {"pos": True, "payments": True, "loyalty": False}


async def test_secret_rotation_accepts_old_and_new(tmp_path):
    settings = make_settings(tmp_path, pos_webhook_secrets="new-secret-value, old-secret-value")
    async with running_hub(settings) as app, hub_client(app) as client:
        sender = Sender(client, settings)
        assert (await sender.send("pos", make_event(), secret="old-secret-value")).status_code == 202
        assert (await sender.send("pos", make_event(), secret="new-secret-value")).status_code == 202
        assert (await sender.send("pos", make_event(), secret="retired-secret")).status_code == 401


async def test_oversized_body_is_413(tmp_path):
    settings = make_settings(tmp_path, webhook_max_body_bytes=1024)
    async with running_hub(settings) as app, hub_client(app) as client:
        sender = Sender(client, settings)
        resp = await sender.send("pos", make_event(data={"blob": "x" * 2048}))
        assert resp.status_code == 413
        assert resp.json()["reason"] == "payload_too_large"


@pytest.mark.parametrize("kind", ["memory", "redis"])
async def test_db_constraint_dedupes_when_fast_path_forgets(tmp_path, kind):
    """Redis flushed / TTL expired: the unique constraint still catches the duplicate."""
    settings = make_settings(tmp_path)
    store = make_store(kind)
    async with running_hub(settings, store) as app, hub_client(app) as client:
        sender = Sender(client, settings)
        event = make_event()
        assert (await sender.send("pos", event)).status_code == 202
        if isinstance(store, InMemoryIdempotencyStore):
            store.clear()
        else:
            await store._r.flushall()
        resp = await sender.send("pos", event)
        assert resp.status_code == 200
        assert resp.json()["status"] == "duplicate"
        rows = (await client.get("/v1/inbox")).json()
        assert len(rows) == 1 and rows[0]["delivery_count"] == 2


class BrokenStore:
    backend = "broken"

    async def begin(self, key, lock_ttl_s):
        raise ConnectionError("redis down")

    async def complete(self, key, ttl_s):
        raise ConnectionError("redis down")

    async def release(self, key):
        raise ConnectionError("redis down")

    async def ping(self):
        return False

    async def aclose(self):
        pass


async def test_idempotency_store_outage_fails_open_to_db_constraint(tmp_path):
    settings = make_settings(tmp_path)
    async with running_hub(settings, BrokenStore()) as app, hub_client(app) as client:
        sender = Sender(client, settings)
        event = make_event()
        assert (await sender.send("pos", event)).status_code == 202
        assert (await sender.send("pos", event)).status_code == 200
        assert len((await client.get("/v1/inbox")).json()) == 1

        health = await client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "degraded"
        assert health.json()["idempotency_store"] == {"backend": "broken", "ok": False}


# ------------------------------------------------------------ hygiene


async def test_health_reports_components_without_leaking_secrets(hub, settings):
    resp = await hub.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["database"] is True
    assert body["webhook_sources_configured"] == {"pos": True, "payments": True, "loyalty": True}
    for source in ("pos", "payments", "loyalty"):
        assert settings.webhook_secrets(source)[0] not in resp.text


async def test_secrets_never_appear_in_logs(hub, sender, settings, caplog):
    caplog.set_level(logging.DEBUG, logger="irlab")
    event = make_event()
    await sender.send("pos", event)
    await sender.send("pos", event)
    await sender.send("pos", make_event(), secret="wrong")
    assert "webhook accepted" in caplog.text
    assert "webhook duplicate" in caplog.text
    assert "webhook rejected" in caplog.text
    for source in ("pos", "payments", "loyalty"):
        assert settings.webhook_secrets(source)[0] not in caplog.text
    assert repr(settings).count("**********") >= 3  # SecretStr masks them in reprs too


async def test_receipts_record_every_attempt_with_timing(hub, sender):
    event = make_event()
    await sender.send("pos", event)
    await sender.send("pos", event)
    await sender.send("pos", make_event(), timestamp=0)
    rows = await receipts(hub)
    assert [r["outcome"] for r in rows] == ["rejected", "duplicate", "accepted"]
    assert all(r["duration_ms"] >= 0 for r in rows)
    assert [r["http_status"] for r in await receipts(hub, outcome="rejected")] == [401]
