---
title: "Source coverage — batch 1 (2026-11-12)"
status: current
created: 2026-11-12
registry: ../../repository-catalog/registries/source-coverage.yaml
---

# Source coverage รอบที่ 1 (20 แหล่ง, 2026-11-12)

ลำดับวิธีดึงข้อมูล: official API ก่อน ถ้าไม่มีค่อยใช้ MCP และ scraping เป็นทางเลือกสุดท้าย
รายละเอียดของแต่ละแถว (docs URL, auth, rate limit, ToS, หลักฐาน) อยู่ใน `source-coverage.yaml`
ตรวจไฟล์ด้วย `python3 infra/scripts/utils/source_coverage_check.py --summary`

## ภาพรวม

- **ทั้ง 20 แหล่งมี official API** จึงไม่มีแหล่งไหนต้อง scrape
  - LinkedIn: API แบบ self-serve ใช้ได้เฉพาะสมาชิกใน EEA/สวิตเซอร์แลนด์ ส่วน User Agreement ห้าม scrape จึงเหลือทางเดียวคือขอ data archive ด้วยมือ
  - Meta และ YouTube ห้าม scrape เช่นกัน
- **MCP ไม่ได้ให้ข้อมูลมากกว่า API** แทบทุกตัวเป็นแค่ wrapper ของ API เดิม ใช้ quota เดียวกัน
  จึงเหมาะให้ agent ใช้ถาม ไม่เหมาะกับการ bulk ingest เข้า lake
  - ข้อยกเว้นเล็กน้อย: PostHog (HogQL) และ Sentry (search)
  - Slack MCP ห้ามเก็บผลลัพธ์แบบถาวร

| สถานะวันนี้ | จำนวน | แหล่ง |
|---|---|---|
| covered (ได้ครบ) | 0 | — |
| partial | 5 | X 60 % (x-lakehouse; ยังขาดบัญชีของ owner), Meta 25 % (meta intake ผ่าน Telegram), Stripe 35 % (payment events → sales marts), GitHub 40 % (**ingest-now รอบนี้**), Google Workspace 5 % (email-intake spec อนุมัติแล้ว) |
| missing | 15 | ที่เหลือ |

| Verdict | จำนวน | แหล่ง |
|---|---|---|
| ingest-now | 1 | GitHub |
| covered (มี pipeline แล้ว ส่วนที่ขาดเป็นงานเล็ก) | 2 | X, Meta |
| later | 10 | Stripe, Shopify, Google Workspace, Slack, YouTube, Reddit, GA4 + Search Console, PostHog, Sentry, LINE OA |
| skip | 6 | Salesforce, HubSpot (ไม่มีบัญชีใช้งาน), Microsoft 365 (tenant ของบริษัท NexaTech), Notion, Linear, Jira (เป็นปลายทางที่ระบบเราเขียนลงไป ไม่ใช่แหล่งข้อมูล) |
| blocked-legal | 1 | LinkedIn (ToS ห้าม scrape และไม่มี API สำหรับสมาชิกในไทย) |

## ingest-now: GitHub activity

- pipeline คือ workflow `github-activity` (spec: `docs/specs/github-activity.md`) ทำงานวันละครั้งจาก jobs queue
  ดึง repo public ของ owner และ event public ลง Bronze `domain=engineering`
  (`github_repos`, `github_events`)
- ใช้ `GITHUB_TOKEN` ของ workflow เอง ไม่ต้องมี secret ใหม่
- ตัด commit message, อีเมล และ body ออกก่อนลง landing
- heartbeat `github-activity` ถ้าไม่มีรอบไหนเข้ามาภายใน 26 ชม. จะแจ้งเตือนทาง Telegram
- รันจริงบน R2 วันที่ 2026-11-12 ผ่านทั้ง capture และ heartbeat ([run 43067986523](https://github.com/bookchaowalit/data-lake-pipelines/actions/runs/43067986523)):
  ได้ 300 event และ 119 repo ลง Bronze
  manifest อยู่ที่ `control/manifests/source=github/batch_id=dd85a497…` (events) และ `…d92ae73b…` (repos)
- รันซ้ำแล้ว events ได้ `new: false` แปลว่าอ่าน batch เดิมกลับจาก R2 ได้ ไม่เขียนซ้ำ
  ([run 43068222264](https://github.com/bookchaowalit/data-lake-pipelines/actions/runs/43068222264))
- deploy แล้ว: uptime Worker target `github-activity` (26 ชม.) และ jobs queue job `github-activity` (`10 3 * * *` UTC)
- ยังไม่มี Silver/Gold ของ GitHub เพราะรอบนี้ทำถึง Bronze ตาม spec
  Silver (dedupe event ตาม `source_record_id`) จะทำเมื่อมีคนใช้ข้อมูลนี้ (delivery metrics)

## Top 5 ที่ควร ingest ต่อ

1. **Stripe snapshot** (customers, subscriptions, invoices) value 5, effort 2
   ต้องมี restricted read-only key ก่อน และ**ต้องย้าย Stripe MCP ไปใช้ agent API key ก่อน 2026-11-30**
2. **Google Search Console** snapshot รายวันต่อเว็บ ข้อมูลเป็นของเว็บเราเอง PII ต่ำ
   ใช้ตัดสินใจเรื่อง pSEO hub ต้องมี service account ก่อน
3. **PostHog batch export → R2** แทบไม่ต้องเขียนโค้ด เอกสารรองรับ R2 โดยตรง
   คิดเงินตามจำนวนแถว (ราว 1M แถว/เดือนฟรี ตัวเลขนี้มาจาก search snippet) owner ต้องยืนยันราคาก่อน
4. **Sentry** จำนวน issue ต่อ project ต่อวัน ไม่เก็บ event body ต้องมี org token
5. **Gmail** ตาม email-intake spec ที่อนุมัติแล้ว (บัญชีงาน, body เก็บ 90 วัน) ต้องมี OAuth client

## สิ่งที่ owner ต้องทำ (เรียงตามลำดับความสำคัญ)

| แหล่ง | สิ่งที่ต้องทำ | หน้า | ที่เก็บ key |
|---|---|---|---|
| Stripe | สร้าง restricted key แบบอ่านอย่างเดียว และย้าย MCP ไป agent key ก่อน 11-30 | https://dashboard.stripe.com/apikeys, https://docs.stripe.com/mcp | Infisical `/stripe` `STRIPE_READONLY_KEY` |
| Search Console | สร้าง service account แล้วเพิ่มเป็น user ของแต่ละ property | https://console.cloud.google.com/iam-admin/serviceaccounts, https://search.google.com/search-console/users | Infisical `/google-search-console` |
| PostHog | ยืนยันราคา batch export และสร้าง R2 token เฉพาะ bucket lake | https://posthog.com/data-pipelines/pricing, https://dash.cloudflare.com/?to=/:account/r2/api-tokens | Infisical `/posthog` `R2_EXPORT_*` |
| Sentry | สร้าง org token (org:read, project:read) | https://sentry.io/settings/auth-tokens/ | Infisical `/sentry` `SENTRY_STATS_TOKEN` |
| Gmail | สร้าง OAuth client ของบัญชีงาน (owner item ของ email-intake) | https://console.cloud.google.com/apis/credentials | Infisical `/google-work` |
| GitHub (ไม่บังคับ) | สร้าง fine-grained PAT ถ้าต้องการข้อมูล repo private | https://github.com/settings/personal-access-tokens/new | Infisical `/github` `GITHUB_ACTIVITY_PAT` |
| LINE OA | บอกว่าใช้ OA ตัวไหน แล้วเพิ่ม Messaging API channel | https://developers.line.biz/console/ | Infisical `/line` |
| Shopify | ยืนยันว่าร้านไหนเปิดขายจริง | https://admin.shopify.com/settings/apps/development | Infisical `/shopify` |
| Slack | บอกว่า workspace ไหนรับข้อความจากคนนอก และยืนยันว่าเข้าใจข้อ bulk export ใน API Terms ถูกต้อง | https://slack.com/terms-of-service/api | — |
| Reddit | อ่าน Data API Terms แล้วขออนุมัติ app | https://redditinc.com/policies/data-api-terms, https://www.reddit.com/prefs/apps | Infisical `/reddit` |

## คำถามด้านกฎหมายที่ต้องให้ owner ตัดสิน (ผมไม่ได้เดาแทน)

- **Slack**: การ archive workspace ของเราเองด้วย internal app นับเป็น "bulk export" ที่ API Terms ห้ามหรือไม่
- **YouTube**: ข้อมูล public เก็บได้ไม่เกิน 30 วัน มีแค่ Analytics ของช่องตัวเองที่เก็บได้นาน
  lake ต้องตั้ง retention 30 วันให้ข้อมูลที่ไม่ใช่ของช่องเรา
- **X**: ต้อง mirror การลบโพสต์ภายใน 24 ชม. เมื่อได้รับคำขอ ข้อนี้ x-lakehouse ยังไม่มี
  ควรเปิดเป็นงานแยก
- **Reddit**: เงื่อนไข non-commercial และข้อห้ามใช้ train ML อ่านได้จากแหล่งรองเท่านั้น
  เพราะหน้าทางการบล็อกการ fetch

## ToS ที่ตรวจเพิ่ม และจุดที่ยังไม่ได้ยืนยัน

ยืนยันจากหน้าทางการเพิ่มเติม 2026-11-12:
- **Microsoft**: APIs Terms §3(b)(4) ห้ามสร้าง database หรือสำเนาข้อมูลเกินกว่าที่ use case ของแอปต้องใช้
  ใช้ยืนยันเหตุผลที่ skip M365 เพิ่มอีกข้อ นอกจากเหตุที่เป็น tenant บริษัท
- **LINE**: User Data Policy §3.2.3 ให้เก็บข้อมูล Friend/Group ได้ไม่เกิน 24 ชม.
  ถ้าจะเก็บข้อมูลผู้ใช้นานกว่านั้นต้องแจ้งผู้ใช้ก่อน และต้องลบเมื่อผู้ใช้ขอ
  pipeline LINE จึงต้องมีทั้งการแจ้งผู้ใช้และทางลบข้อมูล
- **Jira** (Developer Terms §9.2) และ **Linear** (AI Addendum §13 ไม่ train บน customer data) ไม่มีข้อห้ามเรื่อง cache

ยังไม่ได้ยืนยัน เพราะหน้าทางการบล็อกการ fetch:
- Salesforce MSA และ Notion Developer Terms ทั้งสองแถวเป็น skip จึงไม่กระทบรอบนี้
- **PostHog terms** owner ต้องอ่านพร้อมราคา batch export ก่อนเปิด export
- **Reddit Data API Terms** owner ต้องอ่านก่อน capture ใดๆ

ตัวเลข rate limit ของ Salesforce, ราคา LINE OA ไทย และราคา batch export ของ PostHog
มาจาก search snippet หรือแหล่งรอง และเขียนว่า `unverified` ไว้ในแถวของแหล่งนั้นแล้ว

## Batch 2 (เรียงไว้แล้ว)

แหล่งที่มีบัญชีใช้งานอยู่และ data-sources ระบุว่ายัง not-captured มาก่อน:
Cloudflare, Vercel, Railway, Neon, Supabase (ค่า hosting และ usage → `paas-ops`),
Google Play Console, App Store Connect, Ahrefs, TikTok, Shopee, Lazada, PayPal, Wise,
Binance TH, Strava, Telegram, Discord, Upwork, Beehiiv/Substack, Cal.com/Calendly
