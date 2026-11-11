# Data model

The hub owns one SQL database: SQLite for local runs and tests, PostgreSQL 16
under Docker Compose, with the schema created by the Alembic migration
`migrations/versions/0001_*`. The mock providers keep their own in-memory state
and can be reached only over HTTP, like real external systems. Amounts are
always integer **minor units** (kuruş) plus a currency code. Timestamps are
timezone-aware UTC (`irlab.db.UTCDateTime`). JSON columns are `JSONB` on
PostgreSQL.

## Tables

| Table | Milestone | Purpose |
|---|---|---|
| `inbox_events` | M1 (+M2 columns) | One row per verified `(source, event_id)`. Holds the deduplicated webhook log, the processing status and the lease. |
| `webhook_receipts` | M1 | Every HTTP delivery attempt, including duplicates and rejections. Only the size and SHA-256 of the body are stored. |
| `orders` | M2 | Canonical order: the POS snapshot plus derived payment and loyalty state |
| `order_payments` | M2 | Payment snapshots, keyed by `order_ref`. A payment may exist before its order. |
| `resource_versions` | M2 | Highest provider `sequence` applied per resource (out-of-order handling) |
| `outbox_messages` | M2 | Outbound calls, written in the same transaction as the state change that needs them |
| `dead_letters` | M2 | Items that need a person. One row per item; reopened if a replay fails again. |
| `replay_requests` | M2 | Audit trail: who replayed or discarded which dead letter, why, and the result |
| `reconciliation_runs` | M3 | One row per reconciliation run, with counts and errors |
| `mismatches` | M3 | Typed disagreements between the hub and a provider. One open row per (kind, entity). |
| `alerts` | M3 | One row per firing of an alert rule; `resolved_at` is set when the rule clears |

## Entity-relationship diagram

```mermaid
erDiagram
    INBOX_EVENTS {
        int id PK
        string source "pos | payments | loyalty"
        string event_id "UNIQUE with source"
        string event_type "order.created, payment.captured, order.snapshot (backfill)"
        string resource_type "order | payment | member"
        string resource_id
        int sequence "provider per-resource version"
        json payload "verified envelope"
        string payload_sha256
        int delivery_count
        int conflicting_deliveries
        string status "received processing processed parked superseded ignored dead"
        string origin "webhook | reconciliation"
        int processing_attempts
        datetime claimed_until "worker lease"
        datetime parked_at
        bool skip_sequence_check "operator replay or backfill"
    }
    WEBHOOK_RECEIPTS {
        int id PK
        string source
        string event_id "null unless signature verified"
        string outcome "accepted duplicate in_progress rejected error"
        string reason
        int http_status
        float duration_ms
    }
    ORDERS {
        string id PK "POS order id"
        string pos_status "placed | cancelled"
        int pos_version
        string status "awaiting_payment paid partially_paid overpaid payment_failed cancelled"
        int total_minor
        int authorized_minor
        int captured_minor
        string loyalty_member_id
        string loyalty_status "not_applicable not_due pending accrued member_missing failed skipped"
        int points_awarded
        string loyalty_accrual_id
        string last_event_id "correlation"
    }
    ORDER_PAYMENTS {
        string payment_id PK
        string order_id "provider order_ref"
        int amount_minor
        int captured_minor
        string status
        int version
    }
    RESOURCE_VERSIONS {
        string source PK
        string resource_type PK
        string resource_id PK
        int last_sequence
    }
    OUTBOX_MESSAGES {
        int id PK
        string kind "loyalty.accrue"
        string idempotency_key "UNIQUE, reused on every retry and replay"
        json payload
        string order_id
        string correlation_id
        string status "pending in_flight sent dead skipped"
        int attempts
        datetime next_attempt_at "backoff with full jitter"
        datetime claimed_until "lease"
        string last_error
        json response
    }
    DEAD_LETTERS {
        int id PK
        string origin "inbox | outbox"
        int origin_id "UNIQUE with origin"
        string reason_code
        string status "open replaying resolved discarded"
        int attempts
        int times_dead_lettered
        int replay_count
        string resolution "replayed superseded skipped discarded"
        json details
    }
    REPLAY_REQUESTS {
        int id PK
        int dead_letter_id FK
        string action "replay | discard"
        string requested_by
        string note
        string result "succeeded failed_again retry_scheduled discarded"
    }
    RECONCILIATION_RUNS {
        int id PK
        datetime window_since
        string status "ok | partial"
        int orders_checked
        int mismatches_found
        int repairs_queued
        int skipped_in_grace
        int skipped_in_flight
    }
    MISMATCHES {
        int id PK
        string kind
        string entity_id
        string order_id
        string severity
        json expected
        json actual
        string status "open | resolved"
        string repair "backfill_queued backfill_available manual"
        string resolution "repaired cleared manual"
        int first_run_id FK
        int last_run_id FK
    }
    ALERTS {
        int id PK
        string rule
        string severity
        float value
        datetime fired_at
        datetime resolved_at
    }

    INBOX_EVENTS }o--o| ORDERS : "projects into"
    ORDERS ||--o{ ORDER_PAYMENTS : "order_ref"
    ORDERS ||--o{ OUTBOX_MESSAGES : emits
    INBOX_EVENTS ||--o| DEAD_LETTERS : "may become"
    OUTBOX_MESSAGES ||--o| DEAD_LETTERS : "may become"
    DEAD_LETTERS ||--o{ REPLAY_REQUESTS : "audited by"
    RECONCILIATION_RUNS ||--o{ MISMATCHES : finds
    MISMATCHES }o--o{ INBOX_EVENTS : "backfill snapshot"
```

`order_payments.order_id` and `outbox_messages.order_id` are deliberately not
foreign keys. A payment can arrive before its order, and the hub has to store
it anyway.

## Constraints and indexes that carry correctness

| Constraint / index | Why |
|---|---|
| `uq_inbox_source_event UNIQUE (source, event_id)` | The guarantee behind duplicate handling. Redis only short-circuits. |
| `uq_outbox_idempotency_key UNIQUE (idempotency_key)` | One outbound action per business key, even if the projection runs twice |
| `uq_dead_letter_origin UNIQUE (origin, origin_id)` | One dead letter per item. A failed replay reopens it rather than adding a duplicate. |
| `resource_versions` primary key `(source, resource_type, resource_id)` | One "last applied" sequence per resource |
| `ix_inbox_status`, `ix_outbox_due (status, next_attempt_at)` | Worker polling |
| `ix_inbox_resource (resource_type, resource_id, sequence)` | Waking parked events and superseding old ones |
| `ix_dead_letters_status_reason`, `ix_mismatches_status_kind`, `ix_alerts_rule_open` | Dashboard, bulk replay, alert evaluation |
| `ix_receipts_received_at` | Signature-failure alert window |

## Mock provider resources (in memory, not in the hub DB)

| Provider | Resource | Id format | Versioned field (→ webhook `sequence`) |
|---|---|---|---|
| POS | order | `POS-000001` | `version` (created = 1, each change + 1) |
| Payments | payment | `pay_<16 hex>` | `version` (authorised = 1, captured = 2) |
| Loyalty | member | `LM-1001` | `version` (each accrual + 1) |
| Loyalty | accrual | `acc_<16 hex>` | — (emitted on the member resource) |

Seed data (`irlab/mocks/seed.py`): 3 stores, 5 menu items and 5 loyalty
members. Phone numbers are in the synthetic `+90 555 010 xxxx` block and emails
use `example.com`. The hub stores only the member id.
