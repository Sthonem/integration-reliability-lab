# Runbook: failed integrations

For whoever is on call for the Demo Bistro ordering hub. Each section says how
to recognise a problem, what usually causes it, and what to do. The triage
endpoint (`GET /v1/dead-letters/{id}/triage`, `GET /v1/mismatches/{id}/triage`)
links straight to these sections.

All data in this lab is synthetic. Commands assume the Docker Compose stack
(`HUB=http://127.0.0.1:8100`); for a local `uvicorn` run use `http://127.0.0.1:8000`.

```bash
export HUB=http://127.0.0.1:8100
export OPS_KEY=$(grep '^OPS_API_KEY=' .env | cut -d= -f2-)
```

## First five minutes

1. **Look at the dashboard**: `$HUB/dashboard`. It shows open dead letters,
   open mismatches, firing alerts, parked events and the outbox backlog.
2. **Is the hub healthy?** `curl -s $HUB/health`. `degraded` means Redis is down:
   duplicates are still caught by the database, only slower. `down` means the
   database is down; providers will retry, since the hub answers 503.
3. **Pick one item and triage it.** For a dead letter:
   ```bash
   curl -s $HUB/v1/dead-letters?status=open | jq '.[0]'
   curl -s $HUB/v1/dead-letters/<id>/triage | jq
   ```
   Triage only suggests what to do. It never changes anything.
4. **Follow one order end to end** with its correlation id, which is the event
   id that started the chain:
   ```bash
   curl -s $HUB/v1/orders/<order_id> | jq '{status, loyalty_status, events, outbox, dead_letters}'
   docker compose logs hub worker | grep '<correlation_id>'
   ```
5. **Fix the cause before you replay.** A replay sends the same request with the
   same idempotency key. If nothing has changed, it fails again.

## Operator actions (all need `X-Ops-Key`)

| Action | Command |
|---|---|
| Replay one dead letter | `curl -s -X POST $HUB/v1/dead-letters/<id>/replay -H "X-Ops-Key: $OPS_KEY" -H 'content-type: application/json' -d '{"requested_by":"<you>","note":"<why>"}'` |
| Bulk replay by reason | `curl -s -X POST $HUB/v1/dead-letters/replay -H "X-Ops-Key: $OPS_KEY" -H 'content-type: application/json' -d '{"requested_by":"<you>","reason_code":"member_not_found"}'` |
| Discard (close without replay) | `curl -s -X POST $HUB/v1/dead-letters/<id>/discard -H "X-Ops-Key: $OPS_KEY" -H 'content-type: application/json' -d '{"requested_by":"<you>","note":"<why>"}'` |
| Run reconciliation now | `curl -s -X POST $HUB/v1/reconciliation/runs -H "X-Ops-Key: $OPS_KEY" -H 'content-type: application/json' -d '{"requested_by":"<you>"}'` |
| Mark a mismatch handled | `curl -s -X POST $HUB/v1/mismatches/<id>/resolve -H "X-Ops-Key: $OPS_KEY" -H 'content-type: application/json' -d '{"requested_by":"<you>","note":"refund ref ..."}'` |
| Re-evaluate alerts | `curl -s -X POST $HUB/v1/alerts/evaluate -H "X-Ops-Key: $OPS_KEY"` |
| Audit trail | `curl -s $HUB/v1/replays` |

A replay result is `succeeded`, `failed_again` (the dead letter is open again
and `times_dead_lettered` goes up) or `retry_scheduled` (the error was transient
and the dispatcher keeps retrying).

## Alerts

| Alert | What it means | Go to |
|---|---|---|
| `dlq_depth` | Items need a human. Critical at 10 or more. | the reason code's section below |
| `retries_exhausted` | A provider failed for longer than the retry budget | [retries-exhausted](#retries-exhausted) |
| `signature_failures` | 5 or more webhooks failed signature or timestamp checks in 5 minutes | [signature-failures](#signature-failures) |
| `open_mismatches` | Reconciliation found disagreements | the mismatch kind's section below |
| `stale_parked_events` | An event has waited more than 60 s for a missing earlier version | [sequence-gap-timeout](#sequence-gap-timeout) |

---

## Dead-letter reasons

<a id="member-not-found"></a>
### `member_not_found`

**Symptom:** the order is paid, but `loyalty_status` is `member_missing`. The
DLQ entry is from the outbox, after 1 attempt. A permanent 404 is not retried.

**Cause:** the POS order names a loyalty member that the loyalty platform does
not have. The member may have been deleted or merged, or the id was mistyped at
the till.

**Fix:**
1. Check the member at the provider: `GET /members/<id>`, or search by phone.
2. Restore or correct the record at the provider. Never invent a member.
3. Replay the dead letter, or bulk-replay with `reason_code=member_not_found`.
   The provider does not cache 404 responses, so a replay with the same
   idempotency key goes through. Points are credited once.
4. If the customer cannot be identified, discard with a note. The order stays
   paid and earns no points.

<a id="retries-exhausted"></a>
### `retries_exhausted`

**Symptom:** the `retries_exhausted` alert fires (critical). `attempts` equals
`RETRY_MAX_ATTEMPTS`. `last_error` shows 5xx, timeouts or connection errors.

**Cause:** the provider was down, or answered too slowly, for longer than the
backoff schedule.

**Fix:** confirm the provider has recovered (`/health`, or its status page).
Then bulk-replay with `reason_code=retries_exhausted`. Replays reuse the same
idempotency key. Calls that timed out but did run at the provider are answered
from the provider's idempotency cache, so they are not applied twice. If the
replay fails again, escalate to the provider with the idempotency key and
timestamps from `details`.

<a id="provider-rejected-request"></a>
### `http_4xx` / `idempotency_key_reused` / other provider codes

**Cause:** the provider rejected the request permanently. For example,
`idempotency_key_reused` means the same key was sent with a different body.
That signals a bug: the hub never changes the payload of an outbox message.

**Fix:** read `details.request` and `last_error`. Fix the data at its source,
then replay. If the request itself is wrong, discard it and open a bug.

<a id="sequence-gap-timeout"></a>
### `sequence_gap_timeout`

**Symptom:** an inbox event waited longer than `PARK_TIMEOUT_S` for an earlier
version of the same order or payment.

**Cause:** the provider dropped a webhook, for example `order.created`.

**Fix:** usually nothing. The next reconciliation run loads the provider's
current snapshot, applies it, and resolves this entry as `superseded`. To fix it
sooner, run reconciliation now. If you have checked the provider and the event
is the latest state, replay the entry. A replay of this reason applies the event
as a snapshot and skips the sequence check.

<a id="invalid-payload"></a>
### `invalid_payload`

**Cause:** a signed event does not match the schema the hub expects. Possible
causes are a provider API change, a provider bug, or a field the hub does not
support yet.

**Fix:** look at `origin_record.payload`. Do not replay until something has
changed, because it will fail again. Report the event to the provider. Once
reconciliation has repaired the order, discard the entry with a note.

<a id="processing-failed"></a>
### `processing_failed`

**Cause:** an unexpected exception in the hub's own projection code, retried
`PROCESSING_MAX_ATTEMPTS` times.

**Fix:** find the stack trace by correlation id. Fix and deploy. Then replay.

---

## Mismatch kinds (reconciliation)

Reconciliation runs every `RECONCILE_INTERVAL_S` seconds, and on demand. It
skips anything younger than `RECONCILE_GRACE_S`, and anything whose event is
already in the hub's inbox but not applied yet. For the kinds below marked
*auto*, the hub queues a backfill from the provider's REST API. The mismatch
then resolves as `repaired` on the next run. The other kinds need a person.

<a id="missing-in-hub"></a>
### `missing_in_hub` (auto)

The POS has an order whose webhook never reached the hub. The backfill applies
the POS snapshot. If this keeps happening, check the provider's delivery log and
the hub's [signature-failures](#signature-failures) receipts:

```bash
curl -s "$HUB/v1/webhook-receipts?outcome=rejected"
```

<a id="status-mismatch"></a>
### `status_mismatch` (auto)

The POS has a newer version of the order than the hub. The most common case is
a dropped `order.cancelled`. After the backfill, check that no loyalty points
were credited for the cancelled order. If any were, see
[points-on-unpaid-order](#points-on-unpaid-order).

<a id="payment-missing-in-hub"></a>
### `payment_missing_in_hub` / `payment_status_mismatch` (auto)

The payment provider has a payment, or a newer version of one, that the hub
never received. The backfill moves the order to `paid`, and the loyalty accrual
follows.

<a id="partial-payment"></a>
### `partial_payment`

The amount captured is less than the order total. The order is
`partially_paid`, and no points were credited. Check at the payment provider.
Then collect the difference, or refund and cancel, according to store policy.
Resolve the mismatch with a note that says who handled it and how.

<a id="overpayment"></a>
### `overpayment`

The amount captured is more than the total. Usually the customer was charged
twice by a terminal retry with a new idempotency key. `actual.payments` lists
the payments. Refund the duplicate, then resolve the mismatch with the refund
reference.

<a id="orphan-payment"></a>
### `orphan_payment`

A payment has an `order_ref` that matches no POS order and no hub order. Find
the terminal and the store in the provider's record. Then attach the payment to
the right order, or refund it.

<a id="duplicate-accrual"></a>
### `duplicate_accrual`

There is more than one accrual for one order, so the customer got double
points. The hub's own retries cannot cause this, because they always reuse the
same key. Look for another client. `actual.accrual_ids` lists the accruals.
Reverse the extras at the provider, then resolve the mismatch.

<a id="points-mismatch"></a>
### `points_mismatch`

The points credited differ from the policy of 1 point per full TRY. Compare the
accrual request with the order total, and adjust at the provider.

<a id="points-on-unpaid-order"></a>
### `points_on_unpaid_order`

Points exist for an order that was cancelled or is not fully paid. Reverse the
accrual and resolve the mismatch.

<a id="points-missing"></a>
### `points_missing`

The hub recorded a successful accrual, but the provider has none. Look for the
accrual id from the outbox `response` at the provider. Escalate with the
idempotency key.

---

<a id="signature-failures"></a>
## Signature failures

Look at the rejected receipts by reason and source:
`curl -s "$HUB/v1/webhook-receipts?outcome=rejected"`.

| Reason | Likely cause | Fix |
|---|---|---|
| `invalid_signature` on one source | The secret was rotated on one side only | Add the new secret first (`POS_WEBHOOK_SECRETS=new,old`). The provider switches next. Remove the old secret last. |
| `timestamp_out_of_tolerance` | Clock skew, or replayed requests | Check NTP on both sides. If the timestamps are old and the bodies are identical, someone may be replaying captured traffic. |
| Many failures from unknown traffic | Forged requests | Nothing was stored. Rate-limit at the edge. |
| `source_not_configured` (503) | The secret is missing from the hub config | Set `<SOURCE>_WEBHOOK_SECRETS` and restart the hub |

<a id="general"></a>
## General

- **Nothing moves** (the outbox has unsent messages, and the inbox has events
  in `received`): is the worker running? Check `docker compose ps worker` and
  its logs. Leases free up claims from crashed workers after `CLAIM_LEASE_S`.
- **Redis down:** the hub keeps working. `/health` shows `degraded`, and
  duplicates are caught by the database's unique constraint. Restart Redis.
- **Database down:** webhooks get 503 with `Retry-After`, and providers retry.
  Nothing is lost while the providers keep retrying.
- **Before you discard anything,** write in the note who decided and why. The
  note is the audit trail (`/v1/replays`).
