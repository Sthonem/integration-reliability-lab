"""Alert rules and sinks, the HTML dashboard, the summary feed, JSON logs and correlation ids."""

import io
import json
import logging
from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI, Request

from irlab.alerts import WebhookSink, alert_payload
from irlab.db import session_factory
from irlab.logs import JsonFormatter
from irlab.models import Alert, DeadLetter
from tests.conftest import make_event
from tests.labkit import Lab, open_lab


@pytest.fixture
async def lab(tmp_path) -> AsyncIterator[Lab]:
    async with open_lab(tmp_path, alert_signature_failures=3, alert_parked_age_s=60, park_timeout_s=600,
                        retry_max_attempts=2, retry_base_s=0.01) as lab:
        yield lab


class Capture:
    def __init__(self):
        self.events: list[tuple[str, str]] = []

    async def __call__(self, event: str, alert: Alert) -> None:
        self.events.append((event, alert.rule))


def firing(results: list[dict]) -> set[str]:
    return {r["rule"] for r in results if r["firing"]}


# ------------------------------------------------------------------ alerts


async def test_no_alerts_on_a_healthy_hub(lab):
    await lab.paid_order()
    await lab.tick()
    assert firing(await lab.runtime.alerts.evaluate()) == set()


async def test_dlq_alert_fires_once_and_resolves_after_replay(lab):
    capture = Capture()
    lab.runtime.alerts.sinks.append(capture)
    member = (await lab.client.delete("/mock/loyalty/_admin/members/LM-1002")).json()
    await lab.paid_order(member="LM-1002")
    await lab.tick()
    assert firing(await lab.runtime.alerts.evaluate()) == {"dlq_depth"}
    assert firing(await lab.runtime.alerts.evaluate()) == {"dlq_depth"}
    assert capture.events == [("firing", "dlq_depth")]  # not re-sent on every evaluation

    await lab.client.put("/mock/loyalty/_admin/members/LM-1002",
                         json={k: member[k] for k in ("full_name", "phone", "email")})
    [dl] = await lab.get("/v1/dead-letters", status="open")
    await lab.client.post(f"/v1/dead-letters/{dl['id']}/replay", json={"requested_by": "ops"}, headers=lab.ops())
    assert firing(await lab.runtime.alerts.evaluate()) == set()
    assert capture.events == [("firing", "dlq_depth"), ("resolved", "dlq_depth")]
    [alert] = await lab.get("/v1/alerts")
    assert alert["resolved_at"] is not None and alert["details"] is not None


async def test_retries_exhausted_is_critical(lab):
    await lab.faults("loyalty", fail_next=50)
    await lab.paid_order()
    await lab.tick()
    lab.clock.advance(1)
    await lab.tick()
    results = {r["rule"]: r for r in await lab.runtime.alerts.evaluate()}
    assert results["retries_exhausted"]["firing"] and results["retries_exhausted"]["severity"] == "critical"
    assert results["dlq_depth"]["firing"]


async def test_signature_failure_spike(lab):
    event = make_event()
    for _ in range(3):
        resp = await lab.client.post("/webhooks/pos", content=json.dumps(event),
                                     headers={"webhook-id": event["id"], "webhook-timestamp": "1",
                                              "webhook-signature": "v1,Zm9yZ2Vk"})
        assert resp.status_code == 401
    results = {r["rule"]: r for r in await lab.runtime.alerts.evaluate()}
    assert results["signature_failures"]["firing"] and results["signature_failures"]["value"] == 3
    [alert] = await lab.get("/v1/alerts", firing=True)
    assert alert["details"]["by_source"] == {"pos": 3}


async def test_open_mismatch_and_stale_parked_alerts(lab):
    await lab.faults("pos", webhook_copies=0)
    order = await lab.pos_order(member=None)
    await lab.client.post(f"/mock/pos/orders/{order['id']}/cancel")
    [created, cancelled] = sorted((await lab.get("/mock/pos/_admin/webhooks")),
                                  key=lambda e: e["event"]["sequence"])
    await lab.client.post(f"/mock/pos/_admin/webhooks/{cancelled['event']['id']}/deliver")  # v2 only
    await lab.tick()
    assert firing(await lab.runtime.alerts.evaluate()) == set()
    lab.clock.advance(61)
    assert firing(await lab.runtime.alerts.evaluate()) == {"stale_parked_events"}

    await lab.faults("payments", webhook_copies=0)
    other = await lab.pos_order(member=None)
    await lab.pay(other, capture_amount=10000)
    await lab.reconcile(auto_repair=False)
    assert "open_mismatches" in firing(await lab.runtime.alerts.evaluate())
    assert created


async def test_webhook_sink_posts_json_and_never_raises():
    received: list[dict] = []
    target = FastAPI()

    @target.post("/hook")
    async def hook(request: Request):
        received.append(await request.json())
        return {"ok": True}

    alert = Alert(rule="dlq_depth", severity="warning", summary="1 open dead letter(s)", value=1, threshold=1,
                  details={"by_reason": {"member_not_found": 1}})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=target), base_url="http://sink") as client:
        await WebhookSink("http://sink/hook", client=client)("firing", alert)
    assert received == [alert_payload("firing", alert)]
    await WebhookSink("http://127.0.0.1:9/unreachable", timeout_s=0.2)("firing", alert)  # logs, does not raise


async def test_alerts_api(lab):
    assert (await lab.client.post("/v1/alerts/evaluate")).status_code == 401
    resp = await lab.client.post("/v1/alerts/evaluate", headers=lab.ops())
    assert resp.status_code == 200 and {r["rule"] for r in resp.json()} == {
        "dlq_depth", "retries_exhausted", "signature_failures", "open_mismatches", "stale_parked_events"}


# ------------------------------------------------------- dashboard/summary


async def test_dashboard_renders_every_section(lab):
    await lab.client.delete("/mock/loyalty/_admin/members/LM-1001")
    await lab.paid_order(member="LM-1001")
    await lab.tick()
    await lab.runtime.alerts.evaluate()
    resp = await lab.client.get("/dashboard")
    assert resp.status_code == 200 and resp.headers["content-type"].startswith("text/html")
    page = resp.text
    for heading in ("Open mismatches", "Dead-letter queue", "Parked events", "Alerts", "Webhook receipts",
                    "Replay audit"):
        assert heading in page
    assert "member_not_found" in page and "dlq_depth" in page


async def test_dashboard_escapes_untrusted_text(lab):
    async with session_factory()() as s:
        s.add(DeadLetter(origin="outbox", origin_id=999, reason_code="http_400",
                         last_error='<script>alert("x")</script>', attempts=1, order_id="<b>POS</b>"))
        await s.commit()
    page = (await lab.client.get("/dashboard")).text
    assert "<script>alert" not in page and "&lt;script&gt;" in page
    assert "<b>POS</b>" not in page


async def test_summary_counts(lab):
    await lab.paid_order()
    order = await lab.pos_order(member=None)
    await lab.pay(order, capture_amount=100)
    await lab.tick()
    summary = await lab.get("/v1/summary")
    assert summary["orders_by_status"] == {"paid": 1, "partially_paid": 1}
    assert summary["outbox_by_status"] == {"sent": 1}
    assert summary["inbox_by_status"]["processed"] >= 6
    assert summary["receiver_latency_ms"]["samples"] >= 6 and summary["receiver_latency_ms"]["p95"] >= 0


# ------------------------------------------------------- logs/correlation


@pytest.fixture
def json_logs():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("irlab")
    old_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    yield lambda: [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]
    logger.removeHandler(handler)
    logger.setLevel(old_level)


async def test_json_logs_carry_one_correlation_id_from_webhook_to_outbox(lab, json_logs):
    order = await lab.paid_order()
    await lab.tick()
    records = json_logs()
    assert all({"ts", "level", "logger", "msg"} <= r.keys() for r in records)
    emitted = await lab.get("/mock/payments/_admin/webhooks")
    captured_event = next(e["event"]["id"] for e in emitted if e["event"]["type"] == "payment.captured")
    chain = [r for r in records if r.get("correlation_id") == captured_event]
    loggers = {r["logger"] for r in chain}
    assert {"irlab.processor", "irlab.outbox"} <= loggers
    outbox = [r for r in chain if r["logger"] == "irlab.outbox"]
    assert outbox[0]["order_id"] == order["id"] and outbox[0]["outbox_id"]


async def test_logs_never_contain_secrets(lab, json_logs):
    await lab.paid_order()
    await lab.tick()
    await lab.client.post("/v1/worker/tick", headers=lab.ops())
    text = json.dumps(json_logs())
    for secret in [lab.settings.ops_api_key.get_secret_value()] + [
        lab.settings.webhook_secrets(s)[0] for s in ("pos", "payments", "loyalty")
    ]:
        assert secret not in text


async def test_correlation_id_header(lab):
    resp = await lab.client.get("/health", headers={"X-Correlation-ID": "ticket-4711"})
    assert resp.headers["x-correlation-id"] == "ticket-4711"
    resp = await lab.client.get("/health", headers={"X-Correlation-ID": "bad id\nwith newline"})
    assert resp.headers["x-correlation-id"].startswith("req_")
    assert (await lab.client.get("/health")).headers["x-correlation-id"].startswith("req_")
