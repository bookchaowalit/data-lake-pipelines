# SPEC: Meta intake (Facebook feed and groups, Instagram, Threads)

Status: APPROVED by the owner 2026-11-11 ("อนุมัติ + merge"); open questions decided the same day (see Decisions). Nothing is built yet.
Code (planned): `infra/scripts/x_lakehouse/meta.py`, Worker `workers/meta-capture/`.
Tests (planned): `infra/tests/x_lakehouse/x_lakehouse_meta_test.py`, `workers/meta-capture/test/`.

## Why

X arrives through the Chief Data bot (Grok), which can read X itself. Meta has nothing like that:

| surface | official API (as of mid-2026, recheck before build) |
|---|---|
| Facebook News Feed | none. `user_posts` returns only the user's own posts |
| Facebook Groups | none. The Groups API was removed in Graph API v19 (April 2024), admins included |
| CrowdTangle | shut down August 2024 |
| Meta Content Library | approved researchers and non-profits only |
| Pages / Instagram Business the owner runs | yes, own content only (IG hashtag search: 30 tags per 7 days) |
| Threads | yes, keyword search on public posts (Threads API) |

So feed and group content can only enter the lake when the owner hands it in. Logged-in scraping of
the owner's account (Playwright, browser extension) is **out**: it breaks Meta's terms, risks the
owner's main account, and copies private-group members' data without a basis (PDPA).

## Scope

| phase | what | automatic |
|---|---|---|
| v1 | **share-to-inbox**: the owner sends Facebook / Instagram / Threads posts (link, pasted text, screenshots) to a private Telegram bot; they land in the lake and the X digest | no, owner-curated |
| v2 | **Threads collector**: keyword and account watchlist via the Threads API, same lake path | yes |
| v3 (decision only) | public Pages / public groups through a paid data vendor (Bright Data, Apify) | yes, costs money |

v3 is not built until the owner asks; it needs a cost cap and a PDPA note first.

## v1: share-to-inbox

### Flow

```
owner (phone) --share/forward--> Telegram bot @<meta-capture>
   -> Cloudflare Worker meta-capture (webhook)
        checks secret header + owner chat id, normalises, writes JSON + images
   -> R2 x-intel-lake  <prefix>/landing/meta_capture/date=YYYY-MM-DD/<capture_id>.json
                       <prefix>/landing/meta_capture_media/<capture_id>/<n>.jpg
   -> x-lakehouse cron (existing, 3x/day)
        landing -> raw parquet (dataset meta_capture) -> curated x_intel.meta_posts
        local OCR (Tesseract tha+eng, in the cron container) for captures without text
        denylist screen on pasted + OCR text
        label + route (gateway, text only, same LABELS / ROUTES as weekly.py)
        retention step meta_retention (see Guards)
   -> digest: section "จาก Meta" in the next morning/midday/daily send
```

No new database role, no new always-on server: the Worker only writes to R2 (bucket binding),
and the cron that already runs does the rest (ADR-0013: one lake).

### What the owner sends

One Telegram message, or one album of photos, is one capture.

| in the message | becomes |
|---|---|
| a link on `facebook.com` (incl. `m.`, `web.`, `www.`), `fb.com`, `fb.watch`, `instagram.com`, `instagr.am`, `threads.com` or `threads.net` (the old Threads domain, still shared) | `post_url` (tracking params such as `mibextid`, `rdid`, `__cft__`, `fbclid`, `igsh`, `xmt` stripped; `l.facebook.com/l.php?u=` redirects unwrapped; `threads.net` rewritten to `threads.com` so both forms dedupe), `platform` |
| text other than the link and tags | `text`, `text_source = pasted` |
| photos (screenshots) | `image_keys`; OCR later if `text` is empty, `text_source = ocr` |
| `#g <ชื่อกลุ่ม>` | `surface = group`, `group_label` = the name the owner typed |
| `#feed`, `#page`, `#ig`, `#threads` | `surface` (default: from the link, else `unknown`) |
| other `#tags` | `tags` |
| `#note <ข้อความ>` | `owner_note` |

The bot replies with one line: `เก็บแล้ว · <platform>/<surface> · <capture_id>` or the reason it refused.
A message with no link, no text and no photo is refused.

Forwarding a whole post from the Facebook app usually gives only a link. A link to a closed group opens only
for members, so for groups the owner should send the screenshot (or paste the text) as well.

### Dataset `meta_capture` (raw) and `x_intel.meta_posts` (curated)

| column | type | rule |
|---|---|---|
| `capture_id` | text, key | `tg-<chat_hash8>-<message_id>` (album: id of the first message) |
| `captured_at` | timestamptz | Telegram message date, stored ICT like the X data |
| `platform` | text | `facebook` / `instagram` / `threads` / `unknown` |
| `surface` | text | `feed` / `group` / `page` / `profile` / `unknown` |
| `group_label` | text | owner-typed; never looked up |
| `post_url` | text | normalised; null when none |
| `url_key` | text | sha256 of `post_url`, for duplicates |
| `author_name` | text | only from `#author <name>` in the message, and always null for `surface = group` (`#author` ignored there); never parsed from text or OCR |
| `text` | text | pasted or OCR, max 8,000 chars |
| `text_source` | text | `pasted` / `ocr` / `none` |
| `image_keys` | text[] | R2 keys |
| `tags`, `owner_note` | text | as sent |
| `content_label`, `route` | text | from the gateway (`fact_report` … / `alert_digest` …), `lh-v1` definitions |
| `domain`, `topic` | text | from the gateway, same domain list as `x_signals` |
| `triage_version` | text | `meta-v1` |

Duplicates: a second capture with the same `url_key` is kept in raw but merged in curated (newest text wins,
tags and notes are joined). Re-running the cron on the same landing files writes nothing (idempotent, like
`ingest()`).

### Guards

- The Worker accepts only requests with Telegram's `X-Telegram-Bot-Api-Secret-Token` and messages from the
  allowlisted owner chat id (`META_CAPTURE_CHAT_IDS`). Anything else gets `200` with no reply and no write.
- Worker and cron logs carry counts, ids and exception types only, never text, URLs or images.
- **Images never leave R2 and the cron container.** OCR runs locally (Tesseract with `tha+eng` in the
  x-lakehouse image), so the denylist can be checked before anything is sent out. The gateway only ever gets
  text, and only after the screen.
- Pasted text and local OCR text are screened with `CLIENT_DENYLIST` before any gateway call; a matching capture
  is stored but marked `route = store` and never sent to the gateway or the digest. Without the denylist,
  labelling is skipped (fails closed), the capture is still stored. Local OCR itself runs without the denylist,
  since nothing leaves the container.
- Retention is its own cron step, `meta_retention`, after the curated upsert (the existing `inbox.prune()` only
  clears the drained inbox and `ingest_deliveries`, so it does not cover Meta data):
  - `landing/meta_capture/` JSON and `landing/meta_capture_media/` images: deleted 30 days after `captured_at`
    (R2 lifecycle rule on both prefixes, plus the step deletes any it finds older, in case the rule is missing);
  - `x_intel.meta_posts`: rows with `captured_at` older than 365 days deleted;
  - raw `meta_capture` parquet: partitions `date=` older than 365 days deleted.
  It logs counts only and never fails the lake run.
- `#del` (a reply to the bot's confirmation) deletes that capture everywhere at the next cron run: the curated
  row, its landing JSON and images, and the row in its raw partition (the partition file is rewritten without
  it). The capture id goes to `x_intel.meta_deleted` (id and time only) so a later landing or replay of the same
  capture is refused.

### Digest

`mart_meta_priority` (view): curated captures of the window with `route = alert_digest` or `verify`, newest
first. The digest adds a section `จาก Meta` (max 5 items) under the X sections. Group items show
`group_label`, not the author. The section is omitted when empty. The existing digest checks (denylist,
host check, byte budget) apply unchanged.

## v2: Threads collector (outline, own spec before build)

- Owner creates a Meta developer app with the Threads use case and a long-lived Threads user token
  (60 days, refreshable). Keyword search reads public posts only.
- A cron step `threads_pull` reads the `x_query_plan` rows tagged `platform=threads` (new column, default `x`),
  calls keyword search, and writes `threads_feed` rows with the same columns as `x_feed` (`post_id`, `post_url`,
  `author_handle`, `posted_at`, `text`, `likes`, `reposts`, `replies`, `views`, `query_id`).
- Token refresh joins the token-rotate job; heartbeat `threads-pull`.
- Rate limits and the exact endpoints are checked against the Threads API docs when that spec is written.

## Owner actions (v1)

1. Create the bot: https://t.me/BotFather → `/newbot` → store the token in Infisical
   (https://app.infisical.com), path `/meta-capture`, name `META_CAPTURE_BOT_TOKEN`.
2. Send `/start` to the bot once so the Worker can learn the chat id; Claude stores it as `META_CAPTURE_CHAT_IDS`.
3. Approve the Cloudflare Worker deploy (`meta-capture.bookchaowalit.com` or the workers.dev URL) and the R2
   lifecycle rule on `x-intel-lake`.

Claude does the rest: webhook secret (`META_CAPTURE_WEBHOOK_SECRET`, generated, in Infisical), `setWebhook`,
Worker code, cron step, migration, digest section.

## Acceptance

- M1 a message with a Facebook group link, a screenshot and `#g ชื่อกลุ่ม` lands as one capture with
  `surface = group`, `author_name` null, the image in R2, and the bot replies `เก็บแล้ว`.
- M2 a message from a chat id not on the allowlist, or without the secret header, writes nothing and gets no reply.
- M3 tracking params are stripped; two captures of the same post merge into one curated row, including the
  same Threads post sent once as `threads.net` and once as `threads.com`.
- M4 a capture without text gets local OCR text at the next cron run (no network call for OCR); an OCR error
  leaves `text_source = none` and the next run retries it.
- M5 a capture whose pasted or OCR text matches the client denylist never reaches the gateway or the digest;
  no image is ever sent to the gateway (test: the gateway client receives no image payload).
- M6 the cron is idempotent: a second run over the same landing files writes nothing.
- M7 the digest shows `จาก Meta` only when there are items, max 5, with `group_label` not the author.
- M8 logs contain no post text, URL or image data (test greps the captured log output).
- M9 landing JSON and screenshots older than 30 days are gone (lifecycle rule exists, checked by `validate`;
  the step deletes older ones it finds).
- M10 with a clock 366 days after `captured_at`, `meta_retention` removes the curated row and the raw partition;
  a 364-day-old capture is kept.
- M11 `#del` removes the curated row, landing files and the raw row; replaying the same landing JSON is refused.

## Decisions (2026-11-11, owner delegated: "คุณคิดเลย")

1. **Bot: a new bot just for capture.** The digest bot stays outbound-only, the capture token can be revoked
   alone, and a wrong reply never lands in the digest chat.
2. **Author names: never for `surface = group`.** `#author` is ignored on group captures (the bot says so in
   its reply). On feed, page and profile captures `#author` is kept. Group members never agreed to be in the
   lake, and the post text is what matters for intel.
3. **Digest: a section inside the X digest.** No extra messages, and the existing screens (denylist, host
   check, byte budget) apply unchanged. Revisit if Meta items pass 5 a day for a week.
4. **v2 (Threads) waits for two weeks of v1 use.** Review on or after the 14th day from the first capture: if
   the owner sends fewer than 3 captures a week, the effort goes to Threads automation; otherwise v2 still
   follows but v1 stays the main Meta path.
