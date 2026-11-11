---
title: "Source coverage — batch 2 (2026-11-12)"
status: current
created: 2026-11-12
registry: ../../repository-catalog/registries/source-coverage.yaml
---

# Source coverage รอบที่ 2 (20 แหล่ง, 2026-11-12)

รอบนี้เลือกแหล่งที่เรามีบัญชีใช้งานอยู่ หรือที่ `data-sources.yaml` ระบุว่ายัง not-captured
ได้แก่ hosting/cost, app store, SEO, marketplace ไทย, การเงิน, health และ communication
ตรวจไฟล์ด้วย `python3 infra/scripts/utils/source_coverage_check.py --summary`

## ภาพรวม

- **ทั้ง 20 แหล่งมี official API** แต่มี 1 แหล่งที่ API ใช้กับบัญชีในไทยไม่ได้
  - Wise: personal token อ่าน statement ไม่ได้นอก US/CA/AU/NZ/SG/MY จึงต้อง export เอง (`manual-export`)
- **ไม่มีแหล่งไหนต้อง scrape** แหล่งที่มีความเสี่ยงชัดเจน ผมเลือกไม่ใช้เส้นทางนั้น:
  - Substack: scraper ของชุมชนต้องใช้ session token ของบัญชี
  - TikTok: เสี่ยงขัด ToS
  - Discord: ห้ามใช้ self-bot
- **ไม่มี ingest-now ในรอบนี้** ทุกแหล่งเป็นข้อมูลบัญชีของเราเอง จึงต้องมี credential ใหม่
  ซึ่งผมจะไม่สร้าง key หรือตั้ง secret ให้ repo public เอง (AGENTS.md)
  - ตัวที่พร้อมที่สุดคือ Cloudflare, Railway และ Vercel ทั้งสามตัวรอแค่ token แบบอ่านอย่างเดียว

| สถานะวันนี้ | จำนวน | แหล่ง |
|---|---|---|
| covered | 0 | — |
| partial | 2 | Cloudflare 10 % (posture monitor อ่าน R2 trigger ของ ADR-0013 §7 ทุกเดือน), Telegram 30 % (channel ลง lake ในเครื่องด้วยมือ) |
| missing | 18 | ที่เหลือ |

| Verdict | จำนวน | แหล่ง |
|---|---|---|
| later | 17 | Cloudflare, Vercel, Railway, Neon, Supabase, Google Play, App Store, Ahrefs, TikTok, Shopee, Lazada, PayPal, Wise, Binance TH, Strava, Telegram, Upwork |
| skip | 3 | Discord (ไม่มี server ของเรา), Beehiiv/Substack (ยังไม่มี newsletter), Cal.com/Calendly (ไม่ได้ใช้ booking link) |

## Top 5 ที่ควร ingest ต่อ (รวมเป็น pipeline เดียว: ค่าใช้จ่าย hosting รายวัน)

1. **Railway** `estimatedUsage` รายวัน ถ้ามีข้อมูลนี้ตั้งแต่ก่อน 2026-11-07 จะเห็นล่วงหน้าว่าใกล้ชนเพดาน $40
   ก่อนที่ทุก service จะถูก pause
   ชื่อ field มาจาก forum ต้องยืนยันใน GraphiQL ก่อน
2. **Cloudflare** billable usage (แถว FOCUS) และ R2/Workers usage จาก GraphQL
   endpoint ยังเป็น beta และต้องยืนยันว่าบัญชี $0 ได้แถวกลับมาหรือไม่
3. **Vercel** FOCUS charges รายวันต่อ project ต้องลองเรียกหนึ่งครั้งว่า Hobby ใช้ได้หรือไม่
4. **Strava** → health cell ต้องตัดจุดเริ่มและจุดจบของเส้นทาง (บ้าน/ที่ทำงาน) ออกก่อนลง Bronze
5. **Binance TH** → investments ใช้ key แบบอ่านอย่างเดียว ห้ามเปิดสิทธิ์ trade/withdraw และล็อก IP

ทั้งสามตัวแรกใช้รูปแบบ FOCUS (Cloudflare, Vercel) หรือแปลงให้เป็น FOCUS ได้ (Railway)
จึงรวมเป็น dataset `domain=finance/dataset=cloud_costs` ตัวเดียวได้ ซึ่งเป็น flow `paas-ops` ใน data-sources

## สิ่งที่ owner ต้องทำ

| แหล่ง | สิ่งที่ต้องทำ | หน้า | ที่เก็บ key |
|---|---|---|---|
| Railway | สร้าง workspace token แบบอ่านอย่างเดียว | https://railway.com/account/tokens | Infisical `/railway` `RAILWAY_USAGE_READ_TOKEN` |
| Cloudflare | สร้าง token สิทธิ์ Account Analytics Read และ Billing Read | https://dash.cloudflare.com/profile/api-tokens | Infisical `/cloudflare` `CF_USAGE_READ_TOKEN` |
| Vercel | สร้าง token แบบอ่านอย่างเดียว | https://vercel.com/account/settings/tokens | Infisical `/vercel` `VERCEL_USAGE_READ_TOKEN` |
| Strava | authorize app ที่มีอยู่หนึ่งครั้งเพื่อให้ได้ refresh token | https://www.strava.com/settings/api | Infisical `/strava` `STRAVA_REFRESH_TOKEN` |
| Binance TH | สร้าง key แบบอ่านอย่างเดียว ไม่เปิด trade และ withdraw ล็อก IP | https://www.binance.th/en/my/settings/api-management | Infisical `/binance-th` |
| Neon | ตัดสินใจว่าคุ้มจะขึ้นแผน Launch ไหม (Free ได้ 403 ใน consumption API) | https://neon.com/pricing | — |
| Shopee / Lazada / PayPal | ยืนยันว่ามีร้านหรือบัญชีที่ใช้งานจริง | https://open.shopee.com/, https://open.lazada.com/apps | Infisical `/shopee`, `/lazada`, `/paypal` |
| Telegram | เลือก allowlist ของ channel ที่จะย้ายขึ้น R2 | — | — |

หลังได้ token แล้ว ต้องเพิ่มเป็น secret ของ `data-lake-pipelines` ด้วยชื่อเดียวกัน (repo นี้เป็น public
log จึงแสดงได้แค่จำนวนแถวกับ object key)

## ToS และ PDPA ที่ต้องรู้

- **Telegram** API Terms §1.5 ห้ามใช้ข้อมูล Telegram train, fine-tune หรือ deploy โมเดล AI/ML
  dataset นี้ต้องติด tag ห้ามใช้กับ AI และห้ามส่งเข้า RAG
- **Strava** API Agreement §4.4 ต้องลบข้อมูล Strava ทั้งหมดเมื่อเลิกใช้ และแสดงข้อมูลได้เฉพาะเจ้าของ
  ส่วน API Policy ที่แยกออกมายังไม่ได้อ่าน
- **Microsoft** (จาก batch 1) และ **Wise** (เป็นข้อจำกัดของ API) ไม่ใช่เรื่อง legal
- ข้อมูลแชต (Telegram) และคำสั่งซื้อ (Shopee/Lazada) มีข้อมูลส่วนบุคคลของคนอื่น
  ต้องมี allowlist, retention และทางลบข้อมูลก่อนเข้า lake

## จุดที่ยังไม่ได้ยืนยัน

- หน้า docs หรือ terms ทางการเข้าไม่ได้ ข้อมูลจึงมาจากแหล่งรอง: Shopee, Lazada, Discord, Upwork, PayPal user agreement, Ahrefs plan table, Substack
- ชื่อ field usage ของ Railway และ path ของ billable-usage ของ Cloudflare ต้องลองเรียกจริงก่อนเขียนโค้ด

ทุกจุดเขียนว่า `unverified` ไว้ในแถวของแหล่งนั้นแล้ว

## Batch 3 (เรียงไว้แล้ว)

ค่าใช้ AI และการตลาด: OpenAI usage, Anthropic usage, Google Ads, Meta Ads

ส่ง mail และ media: Resend, Cloudinary

ธุรกิจในไทย: Grab Merchant, LINE MAN Wongnai, Fastwork

ความรู้และชีวิต: Readwise, Spotify, Zoom

ข้อมูลเปิดและข้อมูลตลาด (ส่วนใหญ่เป็น public data จึงน่าจะได้ ingest-now โดยไม่ต้องมี credential):
Bank of Thailand API, SET market data, data.go.th, CoinGecko, JobsDB, JobThai, Hugging Face, Kaggle
