# Demo script (about 4 minutes)

Two ways to give the demo:

- **No Docker:** `python -m irlab.demo` runs the whole story in-process in a few
  seconds. Read its output top to bottom alongside the talking points below.
- **Live, on the Compose stack:** use the steps below. The dashboard sits in
  one window and a terminal in the other.

Preparation (once):

```bash
python -m irlab.devtools init-env
```

```bash
docker compose up -d --build
```

```bash
export HUB=http://127.0.0.1:8100 POS=http://127.0.0.1:8101 PSP=http://127.0.0.1:8102 LOY=http://127.0.0.1:8103
export OPS_KEY=$(grep '^OPS_API_KEY=' .env | cut -d= -f2-)
```

Open `$HUB/dashboard`. It refreshes every 10 seconds.

---

## 0:00 — The problem (20 s)

> "A café chain runs three systems it doesn't own: POS, payments and loyalty.
> The hub joins them into one order. Every webhook can arrive twice, late, out
> of order or forged, and every outbound call can time out. The rest of this demo
> injects those failures on purpose and shows what the hub does about each."

## 0:20 — Happy path (40 s)

```bash
curl -s -X POST $POS/orders -H 'content-type: application/json' -d '{"store_id":"ST-IST-01","items":[{"sku":"SKU-LATTE","qty":2}],"loyalty_member_id":"LM-1001"}'
```

```bash
curl -s -X POST $PSP/payments -H 'content-type: application/json' -H 'Idempotency-Key: pay-POS-000001' -d '{"order_ref":"POS-000001","amount_minor":19000}'
```

```bash
curl -s $HUB/v1/orders/POS-000001 | jq '{status, loyalty_status, points_awarded, outbox: [.outbox[] | {idempotency_key, status, attempts}]}'
```

> "Three signed webhooks went into the inbox. The order is paid, and the loyalty
> accrual went out through the outbox with a key derived from the order, so the
> key is the same on every retry."

## 1:00 — Duplicates and forgeries (30 s)

```bash
curl -s -X POST $POS/_admin/webhooks/$(curl -s $POS/_admin/webhooks | jq -r '.[0].event.id')/deliver | jq '{status, response}'
```

> "The provider retries the webhook and gets a 200 `duplicate`: there is one inbox
> row, with `delivery_count` 2. A request with a forged signature gets a 401, and
> its body is never stored."

## 1:30 — Timeout without double points (40 s)

Run the timeout scenario and the missing-customer scenario together, then open
the dashboard:

```bash
python -m irlab.scenarios --target http --orders 10 --only timeout --only missing_customer
```

> "Loyalty does the work but answers after our 1-second deadline. The hub
> retries with the **same** Idempotency-Key and gets the stored answer back:
> 10 orders, 0 double credits. The naive client, which uses a new key on each
> retry, credits every customer twice."

## 2:10 — Missing customer → DLQ → triage → replay (50 s)

Show the replay audit on the dashboard, or delete a member yourself:

```bash
curl -s -X DELETE $LOY/_admin/members/LM-1004 > /tmp/lm-1004.json
```

```bash
curl -s -X POST $POS/orders -H 'content-type: application/json' -d '{"store_id":"ST-IST-02","items":[{"sku":"SKU-TEA","qty":3}],"loyalty_member_id":"LM-1004"}'
```

Pay for the new order (the POS returned its id; replace `POS-0000NN` below):

```bash
curl -s -X POST $PSP/payments -H 'content-type: application/json' -H 'Idempotency-Key: pay-POS-0000NN' -d '{"order_ref":"POS-0000NN","amount_minor":7500}'
```

```bash
curl -s "$HUB/v1/dead-letters?status=open" | jq '.[0] | {id, reason_code, attempts, order_id}'
```

```bash
curl -s $HUB/v1/dead-letters/<id>/triage | jq '{summary, runbook_url, next_steps}'
```

Restore the member, then replay:

```bash
curl -s -X PUT $LOY/_admin/members/LM-1004 -H 'content-type: application/json' -d "$(jq '{full_name, phone, email, tier, points_balance}' /tmp/lm-1004.json)"
```

```bash
curl -s -X POST $HUB/v1/dead-letters/<id>/replay -H "X-Ops-Key: $OPS_KEY" -H 'content-type: application/json' -d '{"requested_by":"demo","note":"member restored"}'
```

> "A 404 is permanent, so the hub dead-letters the call after one attempt
> instead of retrying it. Triage only suggests the runbook step and cannot
> change anything. After the data is fixed, the replay uses the same key, and
> who replayed what is in the audit trail."

## 3:00 — Reconciliation and the dashboard (50 s)

```bash
python -m irlab.scenarios --target http --orders 12 --only data_mismatch --only partial_payment
```

> "Webhooks are notifications, not a ledger. Reconciliation reads each
> provider's own records and stores typed mismatches. Dropped webhooks are
> backfilled from the provider's REST API and resolve as `repaired`. Double
> charges, double points and partial payments stay open for a person. The
> `open_mismatches` alert fires, and each mismatch links to its runbook
> section."

## 3:50 — Close (10 s)

> "All six failure scenarios run as tests, and the scenario runner measures them
> against PostgreSQL and Redis. The numbers in the README come from those runs."

---

Reset between demos:

```bash
docker compose down -v
```

```bash
docker compose up -d
```
