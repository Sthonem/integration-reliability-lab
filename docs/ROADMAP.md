# Roadmap

Scenario: **Demo Bistro Group**, a fictional café chain in Istanbul and Ankara.
Each store runs a **POS**; card payments go through a **payment provider**;
customers collect points in a **loyalty platform**. The company's **ordering
hub** (this repository's service) stitches the three together into one canonical
order. The three external systems are mocks with failure-injection controls.
Every store, member, order, payment and secret here is synthetic or generated.

> "POS, payment, ordering and loyalty": POS, payments and loyalty are the three
> mock external APIs; *ordering* is the hub we own.

## Design principles

1. **At-least-once in, exactly-once effect.** Providers retry webhooks and our
   own outbound calls get retried. Every boundary is idempotent: event ids on the
   way in, `Idempotency-Key` on the way out, unique constraints underneath.
2. **The database is the source of truth; Redis is a fast path.** Redis
   short-circuits duplicates and serialises concurrent deliveries, but a unique
   constraint in Postgres is what guarantees correctness if Redis is flushed or
   down.
3. **Never lose an event, never hide a failure.** Anything that cannot be
   applied is parked or dead-lettered with its reason, visible on a dashboard,
   alertable, and replayable after the root cause is fixed.
4. **Trust nothing unauthenticated.** Webhooks are verified against the raw
   bytes with HMAC + timestamp before parsing; unverified bodies are never stored.
5. **Measure, then claim.** README numbers come from the scenario runner, not
   from estimates. Sections without measurements say so.

## Milestones

| # | Scope | Status |
|---|-------|--------|
| 1 | Repo structure, architecture doc, data model, FastAPI skeleton. Three mock external APIs (POS, payments, loyalty) with in-memory state, provider-side `Idempotency-Key` semantics, signed webhook emission and basic fault injection (fail next N, latency, drop/duplicate webhooks). Webhook receiver: HMAC-SHA256 signature over raw body, timestamp tolerance, secret rotation, fail-closed when unconfigured, idempotent inbox (Redis/in-memory fast path + DB unique constraint), payload-conflict detection, receipt log for every delivery attempt. Tests. | **done** |
| 2 | Event processing and outbound reliability. Inbox worker projects events into a canonical order (POS + payment + loyalty). Per-resource `sequence` handling for **out-of-order events** (park until the gap closes, then resume; dead-letter after a timeout). **Transactional outbox** for outbound calls (loyalty accrual; the outbox is generic over kinds — payment capture stays with the terminal in this scenario, see below). Dispatcher with **retry + exponential backoff + full jitter**, per-call deadline enforced with `asyncio.timeout`, stable `Idempotency-Key` per business action. Retry budget exhaustion or a permanent 4xx → **dead-letter queue** with reason and last error. **Failed-event replay** API (single + bulk by reason, discard) with an audit trail. Operator writes behind `X-Ops-Key` (fail closed). | **done** |
| 3 | **Reconciliation job**: compares hub ↔ POS ↔ payments ↔ loyalty over a time window and records 11 typed mismatch kinds (missing in hub, status mismatch, payment missing/stale, partial payment, overpayment, orphan payment, duplicate accrual, points mismatch, points on unpaid order, points missing), with a grace period, in-flight skipping and **automatic backfill** for dropped webhooks. **Mismatch dashboard** (server-rendered HTML: open mismatches, DLQ, parked events, alerts, receipts, replay audit). **Structured JSON logs** with correlation ids (event id → order id → outbox message → provider `X-Correlation-ID`). **Alert rules** (DLQ depth, retry exhaustion, signature-failure spike, open mismatches, stale parked events) with log + webhook sinks. | **done** |
| 4 | **Failure-injection scenario runner** (`python -m irlab.scenarios`) for the six required scenarios — duplicate webhook, timeout, partial payment, out-of-order event, missing customer record, data mismatch between systems — each a scripted scenario with expected outcomes, run as tests, in-process or against Compose over HTTP. **Measured results** (duplicates suppressed, detection rate, false positives, time to detect, retries per success, replay success rate, p50/p95 receiver latency, stable-key vs naive-retry double credits). **Test report** and **runbook for failed integrations**. Optional **AI-assisted support triage**: summarises a DLQ entry or mismatch and points to the runbook step; mock provider by default, Claude (structured JSON output, validated) optional, read-only. | **done** |
| 5 | **Docker Compose** (hub API, worker, three mock services, PostgreSQL 16, Redis 7, one-shot Alembic migration) with health checks; test profile against real Postgres + Redis. **Demo script**, Docker instructions, deployment diagram, final README with measured numbers, and "how this maps to payments, APIs and customer deployments". | **done** |

## Deliberate shortcuts

- Local runs and tests create tables at startup (`DB_AUTO_CREATE=true`); Docker
  Compose runs the Alembic migration instead. After a model change during local
  development, delete `irlab.db` (or run `alembic upgrade head`).
- The outbox has one kind, `loyalty.accrue`. In this scenario the card terminal
  authorises and captures in one call, so the hub never captures; the dispatcher
  is generic over kinds, and a capture kind would reuse it unchanged.
- Mock providers keep state in memory and deliver webhooks inline (plus manual
  redelivery through `/_admin`). Provider-side retry schedules are modelled by
  the scenario runner (redelivery, duplicate copies), not by a timer in the mock.
- Reconciliation reads up to 1,000 records per provider per run (filtered by
  `created_after`); production would page with cursors and also read settlement
  files.
- Operator endpoints use one shared key (`X-Ops-Key`); reads are open. A real
  deployment puts SSO in front of both.
- One worker replica in Compose. Claims are compare-and-set with leases and, on
  PostgreSQL, per-order advisory locks, so more replicas are designed for but not
  load-tested here.
- No refunds, voids or points reversals: those mismatches are left for a human.
