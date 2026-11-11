# Test report

Date: 2026-11-08. Machine: MacBook, Darwin arm64. Python 3.13.5, FastAPI
0.162, SQLAlchemy 2.1, pytest 9.2, Docker 29.5, `postgres:16-alpine`,
`redis:7-alpine`. Everything below comes from real runs on that machine.

## Test suite

| Run | Database | Idempotency store | Result |
|---|---|---|---|
| `pytest -q` on the host | SQLite (a temp file per test) | in-memory + `fakeredis` | **303 passed, 0 failed, 0 skipped** (~22 s) |
| Host against the Compose services (`TEST_DATABASE_URL`, `TEST_REDIS_URL`) | PostgreSQL 16 (`irlab_test`) | Redis 7 (db 15) | **303 passed, 0 failed, 0 skipped** (~46 s) |
| `docker compose --profile test run --rm tests` | PostgreSQL 16 (Compose network) | Redis 7 (Compose network) | **303 passed, 0 failed, 0 skipped** (~35 s) |

In the PostgreSQL/Redis runs, every test that builds a hub through
`tests/conftest.py` uses PostgreSQL and real Redis. That covers the receiver,
processor, outbox, replay, reconciliation, alerts, dashboard, triage, the
end-to-end tests and the Alembic migration test. Each test starts from an empty
schema and an empty Redis db.

Some tests never use either service. The scenario-runner tests build their own
in-process hub on SQLite in every mode. The mock-provider, signing and
backoff tests don't use a database at all.

**One failure on the way, now fixed.** The first container run had 4 failures:

- Three `test_devtools.py` tests failed because `.env.example` was not copied
  into the test image. The Dockerfile now copies it.
- One `test_failure_scenario[duplicate_webhook]` failed. The in-process
  scenario hub used a 0.25 s outbound deadline in *every* scenario. That run
  happened while mutation tests were loading the CPU, so an ordinary loyalty
  call took longer than 0.25 s and was cancelled halfway through its inline
  webhook delivery. The short deadline now applies only to the timeout
  scenario (`IN_PROCESS_OVERRIDES`); the others use the 2 s default. A test
  asserts this override.

After the fix, all three runs above passed.

### Tests per file

| File | Tests | Covers |
|---|---|---|
| `test_webhook_receiver.py` | 60 | Signatures, timestamps, rotation, fail-closed sources, envelope validation, duplicates, concurrent deliveries, storage failure, Redis down |
| `test_outbox_and_replay.py` | 43 | Backoff math, retryable classification, retries, retry exhaustion, timeout without double credit, the naive-key counterexample, missing member, bulk replay, discard, leases, races, ops key |
| `test_scenarios_and_triage.py` | 37 | The six failure scenarios end to end, triage rules for every reason and kind, the Claude provider (request shape, fallbacks, the real SDK with a mock transport) |
| `test_processing.py` | 36 | Projection, payment before order, partial payment and overpayment, parking and resuming, superseded events, gap timeout to DLQ to replay, atomic rollback, retries, leases |
| `test_reconciliation.py` | 31 | All mismatch kinds, backfill and repair, grace period, partial runs, report-only mode, `since` window, API auth |
| `test_signing.py` | 16 | HMAC scheme, multiple signatures, tolerance |
| `test_idempotency_store.py` | 16 | Memory and Redis store contract, TTLs |
| `test_alerts_dashboard_logs.py` | 13 | Alert firing and resolving, sinks, dashboard escaping, summary, JSON logs and correlation ids, no secrets in logs |
| `test_end_to_end.py` | 12 | Mounted mocks delivering real signed webhooks |
| `test_mock_*.py` | 35 | POS, payments and loyalty APIs, idempotency semantics, fault injection |
| `test_devtools.py` | 3 | `init-env` generates fresh secrets, uses mode 600, never overwrites |
| `test_migrations.py` | 1 | `alembic upgrade head`, then `alembic check` (no drift from the models), downgrade, upgrade |

Hub tests that are parametrised over `memory` and `redis` count twice.

## Mutation checks

To confirm the tests catch real bugs, each bug below was injected on its own,
the suite was run, and the file was restored.

| Injected bug | Failing tests |
|---|---|
| Receiver accepts any signature | 11 |
| Inbox unique constraint dropped | 3 |
| Claim never released after a storage error | 2 |
| Timestamp window disabled | 4 |
| Outbox sends a different Idempotency-Key per attempt | 4 |
| Sequence parking disabled (apply out-of-order events immediately) | 10 |
| Permanent 4xx treated as retryable | 12 |
| Reconciliation backfill disabled | 10 |
| Reconciliation ignores "already in the inbox" | 2 |

The first four bugs are from Milestone 1. The others are from Milestones 2 and
3.

## Scenario runner

`python -m irlab.scenarios --orders 50` ran each of the six failure scenarios
with 50 orders. All six passed in both environments:

- [In-process](results/scenarios-in-process.md): SQLite, in-memory store,
  ASGI transport.
- [Docker Compose](results/scenarios-compose.md): PostgreSQL 16 and Redis 7,
  over HTTP between containers.

The raw JSON is next to each report. The README's Measured Results table is
taken from these two files.

## Other checks

- **Live stack.** On Compose, a POS order was sent through the payment provider
  to the hub on Postgres, then out through the outbox to loyalty, using curl.
  The order ended `paid` and `accrued`.
- **Local run.** The README's local curl flow (`uvicorn irlab.main:app`, with
  the worker in-process) gave the same result.
- **Demo.** `python -m irlab.demo` runs end to end. The README shows its output.
- **Secrets.** No value from `.env` (webhook secrets, `OPS_API_KEY`,
  `POSTGRES_PASSWORD`) appears in the local or Compose logs. This was checked by
  grepping for each value.
- **Diagrams.** All 8 Mermaid diagrams in the README and `docs/` parse with
  mermaid 11.4.1.
- **Lint.** `ruff check --select E,F,W,B,UP,SIM`: clean.
- **Migration.** On Compose, the `migrate` service ran `alembic upgrade head`
  against an empty PostgreSQL before `hub` and `worker` started.

## Not covered

- Load and throughput. All numbers come from one laptop at low volume.
- More than one worker replica. The design supports it, but no concurrent load
  test was run.
- The Claude triage path against the live API. No API key was available.
  Request and response handling were tested through the official `anthropic`
  SDK (2.6.0) with a mock HTTP transport.
- Real providers, TLS, and a production secret store.
