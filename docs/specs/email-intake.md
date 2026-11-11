# SPEC: Email intake (Gmail → R2 lake → CRM / tasks / finance)

Status: APPROVED by the owner 2026-11-11 ("รับตามที่เสนอ"): every proposal in Decisions accepted.
Standard: ADR-0004 (spec, then failing test, then code). Nothing is built.
Platform rules: ADR-0013 (one R2 lake, Parquet + DuckDB + dbt, no warehouse, no always-on server).
Reuses: the generic Bronze envelope and manifests (`domain=communications/dataset=emails`), dbt models
`stg_emails.sql` and `email_activity_daily.sql` that are still in this repo, and the denylist / gateway
patterns of `x_lakehouse` (`docs/specs/meta-intake.md`).
Planned code: `infra/scripts/data_lake/mail_intake.py` (pull + Bronze), `mail_route.py` (Silver → routes),
Railway cron service `mail-intake` in project `data-schedules`.
Planned tests: `infra/tests/data_lake/mail_intake_test.py`, `mail_route_test.py`,
`infra/dbt/tests/stg_emails_quality.sql` (exists, extended).

## Why

Email is the one owner channel that never reaches the lake. Leads, client replies, invoices and
receipts, job-application answers and platform alerts all arrive there, and today they are read by hand.
The first pipeline (Gmail API → Bronze → Iceberg → SQLite `emails` → CRM / tasks) ran locally and was
retired on 2026-10-11 together with the local SQLite store; only its contract
(`platform-integrations-api/docs/OVERVIEW.md`) and the dbt models above are left.

This spec brings it back hosted, smaller, and with privacy rules first: email holds far more personal
data than the X or Meta feeds, much of it about third parties who never agreed to be in a lake.

## Scope

| phase | what |
|---|---|
| v1 | One Gmail account, read-only. Metadata of every message in scope → Bronze. Body text only for messages that pass the body rule. Daily mart `email_activity_daily`. Routes to CRM activity, tasks and finance as **suggestions** in a review queue (no auto-write). |
| v2 | Auto-write of routes the owner trusts after 14 days of v1 (per route, owner switch). |
| v3 (decision only) | Second account, attachments (PDF invoices → finance), Gmail push via Pub/Sub instead of polling. |

Out of scope always: sending, replying, labelling or deleting mail in Gmail; company mail (NexaTech /
CuteERP lives on the company server, never here); Oak and Happy-Teeth mail beyond metadata unless the owner
names them; logged-in scraping of any webmail.

## Flow

```
Gmail API (gmail.readonly, OAuth refresh token of the owner)
  -> Railway cron mail-intake (every 2 h, :50, restart NEVER, no public port)
       history.list since the stored historyId
         (historyId rejected / 404 -> full reconciliation, see Guards; never a "since last day" shortcut)
       per message: messages.get format=metadata  (+ format=full only when the body rule passes)
       scope filter: account, labels, exclude rules (see Guards)
       -> Bronze envelope, one Parquet part per batch, manifest last
          metadata: s3://mail-lake/bronze/domain=communications/dataset=emails/schema_version=2/ingest_date=D/source=gmail/part-*.parquet
          bodies:   s3://mail-lake/bronze/domain=communications/dataset=email_bodies/schema_version=1/ingest_date=D/source=gmail/part-*.parquet
          s3://mail-lake/bronze/_manifests/source=gmail/batch_id=<id>.json
       state: s3://mail-lake/state/gmail/<account_hash8>.json  (historyId, last run, counts)
  -> body re-check: every stored body against the current body rule (see Guards)
  -> retention step mail_retention (see Guards), BEFORE anything reads Bronze
  -> dbt (same container, DuckDB)  stg_emails -> int_email_routes -> email_activity_daily
       (stg_emails also drops any body whose ingest_date is older than 90 days, as a second guard)
  -> route step (text only, after the denylist screen) via ai-gateway subject `mail-intake`
       -> review queue Gold: s3://mail-lake/gold/mail_routes/date=D.parquet
       -> Telegram summary to the owner (counts + up to 5 items, no bodies)
```

A separate bucket `mail-lake` with its own bucket-scoped R2 token, so no other pipeline token can read mail
and mail can be wiped in one move. No new database: the review queue is a Gold Parquet file, and the
accepted items go to the cells that already own them (CRM, tasks, finance) through their APIs.

## Data

### Bronze payload (`communications.emails`, schema_version 2)

Envelope columns stay as in v1 (`event_id`, `source`, `domain`, `dataset`, `schema_version`,
`received_at`, `event_time`, `privacy_class`, `retention_class`, `ingest_date`, `payload_json`).
`event_id` = sha256 of `account_hash8 + gmail message id`. `payload_json`:

| field | rule |
|---|---|
| `id`, `thread_id` | Gmail ids |
| `account` | `account_hash8` only, never the address |
| `from_address`, `from_domain` | lowercased; `from_name` dropped |
| `to_count`, `cc_count` | counts only, never the recipient list |
| `subject` | max 300 chars |
| `labels` | Gmail system + user labels |
| `is_read`, `is_important`, `is_starred`, `has_attachment` | booleans |
| `list_unsubscribe` | boolean (newsletter signal) |
| `body_rule` | which rule allowed the body (`crm_contact` / `crm_org_domain` / `owner_label` / `finance_sender` / `none`) |

`privacy_class`: `pii-metadata`. `retention_class`: `mail-meta-365d`.

### Bronze `communications.email_bodies` (schema_version 1)

Content never sits in the metadata dataset, so expiring it never means rewriting metadata parts.
One row per message whose body rule passed: `event_id` (same as the metadata row), `snippet`, `body_text`
(plain text, quoted replies and signatures cut, max 8,000 chars). `privacy_class`: `pii-content`,
`retention_class`: `mail-body-90d`. `stg_emails` left-joins it by `event_id`; when the part is gone the
columns are simply null. Silver and Gold are rebuilt from Bronze every run and are not kept as a second
copy of the body (see Retention).

### Body rule

The body (and snippet) is kept only when one of these is true; otherwise the message is metadata only:

1. the sender's **exact address** is a CRM contact or lead; or the sender's domain is an organisation
   domain the CRM marks as verified for a client/lead company (`crm_org_domain`). Public mail providers
   (`gmail.com`, `googlemail.com`, `hotmail.com`, `outlook.com`, `live.com`, `yahoo.com`, `icloud.com`,
   `me.com`, `proton.me`, `protonmail.com`, plus `MAIL_PUBLIC_DOMAINS`) never match by domain, only by exact
   address. The lookup list is exported from the CRM each run, held in memory, never written to the lake;
2. the owner put the Gmail label `lake` on the thread;
3. the sender is on the finance sender list (`MAIL_FINANCE_SENDERS`, e.g. banks, payment providers, Stripe).

Newsletters, social notifications and everything else stay metadata only.

### Gold `mail_routes` (review queue)

| column | rule |
|---|---|
| `event_id`, `thread_id`, `received_at` | from Silver |
| `route` | `crm_activity` / `task` / `finance` / `job_reply` / `ignore` |
| `label` | `client` / `lead` / `invoice_receipt` / `job` / `alert` / `newsletter` / `personal` / `other` |
| `summary_th` | one Thai line from the gateway, max 200 chars |
| `target_hint` | CRM contact id, task project, or finance cell, when known |
| `status` | `suggested` / `accepted` / `rejected` / `applied`: taken from the decision log, `suggested` when none |
| `triage_version` | `mail-v1` |

Gold is rebuilt every run, so the owner's decisions are **not** stored in it. They live in an append-only
decision log `s3://mail-lake/state/route_decisions/date=D/<decision_id>.parquet` (`event_id`, `status`,
`route`, `label`, `target_id`, `triage_version`, `decided_at`, `decided_by`; no subject, address or body).
Whatever records a decision (Telegram reply to the summary, or the CLI) appends one file with the route and
target the owner saw; applying a route appends `applied` with the same route and target. The Gold build
joins the latest decision per `event_id`, and **for a decided message the route, label and target come from
the decision, not from a new gateway call**: the route step skips messages that have a decision, so a later
body or label change never turns an approval into approval of a different route. Only the owner can reopen
one (a `reopen` decision returns it to `suggested`). A tombstoned message also removes its decision files.

## Guards

- **Read-only scope.** The OAuth client asks only for `https://www.googleapis.com/auth/gmail.readonly`.
  The code has no send/modify call; a test fails if any other Gmail method is referenced.
- **Account and exclusions.** One account in v1 (`MAIL_ACCOUNT`, see Q1). `MAIL_EXCLUDE_DOMAINS` and
  `MAIL_EXCLUDE_LABELS` (default: `CHAT`, `SPAM`, `TRASH`, and the domains of hands-off clients) are dropped
  before Bronze: not even metadata is stored.
- **Denylist before AI.** `subject` + `body_text` are screened with `CLIENT_DENYLIST` before any gateway
  call. A match is stored but gets `route = ignore` and never reaches the gateway or Telegram. Without the
  denylist the route step is skipped (fails closed); intake still runs.
- **Gateway gets text only**, never attachments, headers beyond subject, or addresses (the sender is
  replaced by `from_domain` + `known_contact: true/false`).
- **Logs** carry counts, ids hashed, and exception types only; never addresses, subjects or bodies (test
  greps captured log output, like Meta M8).
- **Idempotent.** A rerun over the same historyId window writes nothing new (`event_id` dedupe in
  `stg_emails`, manifest-committed parts only). A crash before the manifest leaves an uncommitted part that
  dbt ignores; the next run rewrites it.
- **Deletes follow Gmail.** `history.list` `messagesDeleted` events, and messages that move to Trash, are
  removed from Bronze at the next run (parts are small per batch; a batch with a deleted message is
  rewritten without it and its manifest replaced), and the id goes to `state/deleted.parquet` so a replay
  is refused.
- **Body re-check every run.** The body rule depends on the CRM export, `MAIL_FINANCE_SENDERS`,
  `MAIL_PUBLIC_DOMAINS` and Gmail labels, which change without a Gmail history event. So each run, after
  intake and before retention and dbt, checks every stored body against the current rule: a body whose
  message no longer passes (contact removed from the CRM, sender off the finance list, `lake` label gone)
  is removed from `email_bodies` (part rewritten, manifest replaced, original `ingest_date` kept). Messages
  that newly pass, within the body retention window, get their body fetched (max 200 per run, the rest
  next run). If the CRM export fails, the run keeps no body that needs `crm_contact` / `crm_org_domain`
  (fails closed: those bodies are not fetched and new ones are not written until the export works).
- **History gaps.** When Gmail rejects the stored `historyId` (expired, 404) or the state file is missing
  after a first run, the run does a full reconciliation before it moves the checkpoint: read the new
  `historyId` from `users.getProfile` first, list every message id in scope for the whole retained window
  (365 days) with `messages.list`, add the ones Bronze lacks, tombstone every Bronze id that is no longer
  listed (deleted, in Trash, or now excluded), and **refresh every id that is in both**: fetch its metadata
  again, write a new metadata version when `labels`, `is_read`, `is_starred` or `is_important` changed
  (`stg_emails` keeps the newest per `event_id`), re-check the body rule, fetch the body when it is newly
  allowed and remove the body when it no longer is. Only then store the new `historyId`.
  The same refresh runs in normal incremental runs for `labelsAdded` / `labelsRemoved` history events. If the
  reconciliation does not finish, the old state stays and the next run starts it again.
- **Retention** (own step `mail_retention`, never fails the run, logs counts only):
  - `email_bodies` Bronze parts: deleted when `ingest_date` is more than 90 days old. Two layers: the step
    deletes by `ingest_date` (primary, since a part rewritten after a delete gets a new upload time), and
    the R2 lifecycle rule `mail-bodies-90d` on the full key prefix
    `bronze/domain=communications/dataset=email_bodies/` (object age 90 days) catches parts if the cron stops.
    A rewritten part keeps its original `ingest_date`, so the step still removes it on time. Manifests drop
    the part keys in the same step. M8 also checks that the rule exists with that exact prefix;
  - Silver / Gold: rebuilt from Bronze each run, so an expired body disappears from them at the next run;
    the DuckDB file in the container is temporary and not uploaded; no other replayable copy is kept
    (the Kafka / landing paths are not used for mail);
  - metadata rows: deleted after 365 days;
  - Gold `mail_routes`: 90 days;
  - `state/`: kept (ids only).
- **Token.** The refresh token lives only in Infisical `/mail-intake` and the Railway service variables;
  if Google revokes it, the run fails with one Telegram ops alert and writes nothing (no partial run).
- **Heartbeat** `mail-intake` every 5 h via monitor.bookchaowalit.com; Sentry not used (no bodies in
  error reports).
- **PDPA note.** Purpose: the owner's own business follow-up. Lawful basis: legitimate interest for
  business correspondence; third-party bodies are kept only under the body rule and only 90 days. Written
  into the data contract before build.

## Owner actions (v1)

1. Google Cloud project + Gmail API: https://console.cloud.google.com/apis/library/gmail.googleapis.com
   → Enable.
2. OAuth consent screen: https://console.cloud.google.com/auth/branding → External, add the chosen account
   as a test user, scope `gmail.readonly`, then **Publish app** (in "Testing" status refresh tokens expire
   after 7 days; recheck this rule before build). The "unverified app" warning is fine for own use.
3. OAuth client: https://console.cloud.google.com/auth/clients → Desktop app → store the JSON in Infisical
   (https://app.infisical.com) path `/mail-intake`, name `GMAIL_OAUTH_CLIENT_JSON`.
4. Run the one-time consent script Claude provides (`python -m data_lake.mail_intake --authorize`), sign in
   with the chosen account; the script stores `GMAIL_REFRESH_TOKEN` in `/mail-intake` itself.
5. Approve: R2 bucket `mail-lake` + bucket-only token
   (https://dash.cloudflare.com/?to=/:account/r2/api-tokens), and the Railway cron service in
   `data-schedules`.

Claude does the rest: bucket and lifecycle rule, data contract, code, tests, dbt changes, Railway service,
heartbeat, Telegram summary.

## Acceptance

- M1 a first run on an empty state pulls the last 30 days of the account into Bronze, writes a manifest,
  and stores the historyId; `stg_emails` returns those rows once each.
- M2 a second run with no new mail writes nothing; a run after one new mail writes exactly one row.
- M3 a message from a CRM contact keeps `body_text`; a newsletter from an unknown sender has
  `body_text = null` and `snippet = null`.
- M4 a message from an excluded domain or label is not in Bronze at all (not even metadata).
- M5 a message whose subject or body matches the denylist never reaches the gateway (test: the gateway
  client receives no call for it) and is not in the Telegram summary.
- M6 no Gmail method other than `users.history.list`, `users.messages.list`, `users.messages.get`,
  `users.getProfile` is used (static test over the module).
- M7 deleting a message in Gmail removes its row from Bronze, Silver and Gold at the next run; replaying
  the old batch file does not bring it back.
- M8 with a clock 91 days after ingest the `email_bodies` part is gone from R2 and its manifest, a full
  dbt rebuild shows the body as null, and the metadata stays; at 366 days the metadata row is gone too.
- M9 logs and the Telegram ops alert contain no address, subject or body (test greps captured output).
- M10 a revoked refresh token makes the run fail with one ops alert and zero writes.
- M11 `email_activity_daily` builds from schema_version 2 and its existing quality test passes.
- M12 a message from a stranger at `gmail.com`, while a CRM contact also uses `gmail.com`, is metadata only;
  a message from an unknown sender at a verified client domain keeps its body.
- M16 with no Gmail history event, removing a contact from the CRM export removes that sender's stored
  bodies at the next run, before dbt or the gateway read them; adding a contact fetches bodies of its
  messages still inside the 90-day window.
- M17 a body whose `ingest_date` is 91 days old but whose object is fresh (rewritten) is deleted by the
  retention step before dbt runs, and the gateway client receives no call with it.
- M18 after an `accepted` decision for `crm_activity`, a changed body or label in a later run does not change
  the item's route or target, and no gateway call is made for it; a `reopen` decision makes it `suggested`.
- M14 an `accepted` decision survives two more runs and a full Gold rebuild; the item is not suggested again.
- M15 a message read and labelled `lake` during a `historyId` gap has `is_read = true`, the new label and its
  body after the reconciliation run.
- M13 with an expired `historyId`, a message deleted during the gap is tombstoned and removed by the
  reconciliation run, and the checkpoint moves only after the reconciliation finishes (a crash halfway
  leaves the old state).

## Decisions (owner, 2026-11-11: "รับตามที่เสนอ")

1. **Account:** `bookchaowalit.work@gmail.com` only (`MAIL_ACCOUNT`). The personal account is v3.
2. **Bodies:** yes, only under the body rule (exact address for public mail domains, verified org domains,
   `lake` label, finance senders), re-checked every run.
3. **Writes:** v1 suggests only (review queue + Telegram summary); auto-write per route in v2 after 14 days.
4. **Retention:** bodies 90 days, metadata 365 days, Gold `mail_routes` 90 days.
5. **First backfill:** 30 days.
