---
title: "Source coverage — batch 3 (2026-11-12)"
status: current
created: 2026-11-12
registry: ../../repository-catalog/registries/source-coverage.yaml
rows: ../../repository-catalog/registries/source-coverage/batch-03.rows.yaml
---

# Source coverage รอบที่ 3 (20 แหล่ง, 2026-11-12)

รอบนี้แบ่งเป็น 4 กลุ่ม:
- ค่าใช้ AI และการตลาด: OpenAI, Anthropic, Google Ads, Meta Ads
- ส่ง mail และ media: Resend, Cloudinary
- ธุรกิจในไทย: Grab, LINE MAN Wongnai, Fastwork
- ความรู้และชีวิต: Readwise, Spotify, Zoom
- ข้อมูลเปิดและข้อมูลตลาด: BOT, SET, data.go.th, CoinGecko, JobsDB, JobThai, Hugging Face, Kaggle

แถวทั้งหมดอยู่ใน `source-coverage.yaml` ส่วนต้นฉบับของรอบนี้อยู่ใน `source-coverage/batch-03.rows.yaml`

## ภาพรวม

| Verdict | จำนวน | แหล่ง |
|---|---|---|
| ingest-now | 1 | Hugging Face trending |
| later | 11 | OpenAI usage, Anthropic usage, Resend, Grab, Readwise, Zoom, Bank of Thailand, SET, data.go.th, JobsDB, Kaggle |
| skip | 5 | Google Ads, Meta Ads (ยังไม่มีงบโฆษณา), Cloudinary, LINE MAN Wongnai, Spotify |
| blocked-legal | 3 | Fastwork (robots ห้าม `/me/*` และ `/inbox`), JobThai (ToS ห้ามคัดลอกถ้าไม่ได้รับอนุญาตเป็นลายลักษณ์อักษร), CoinGecko (Terms §6 ไม่สนับสนุนการเก็บประวัติ) |

สถานะวันนี้: partial 4 แหล่ง (OpenAI และ Anthropic ผ่าน ai-usage/gateway, CoinGecko ผ่าน market-capture ที่รันด้วยมือ,
Hugging Face รอบนี้) ส่วนอีก 16 แหล่ง missing

**ครั้งแรกที่มีแถว `method: scrape`** คือ JobsDB:
- เหตุผลที่ API ไม่พอ: SEEK API เป็น partner-only
- เหตุผลที่ MCP ไม่พอ: ไม่มี MCP ทางการ
- robots.txt อนุญาตเฉพาะหน้า search `?keywords`
- ToS ยังอ่านไม่ได้ verdict จึงเป็น `later` จนกว่า owner จะอ่านและตัดสินใจ
- scraper ที่มีอยู่ไม่ได้อยู่ใน hosted capture ที่ตั้งเวลาไว้

## ingest-now: Hugging Face trending

- spec `docs/specs/hf-trending.md`, workflow `hf-trending` รันทุกวัน 03:40 UTC และมี heartbeat `hf-trending`
- ไม่ต้องมี credential: robots.txt `Allow: /`, anonymous ใช้ได้ 500 ครั้งต่อ 5 นาที แต่ละรอบเรียกแค่ 3 ครั้ง
- แต่ละวันเก็บ model, dataset และ space ที่ติด trending ประเภทละ 100 อันดับ ลง Bronze `domain=market-intelligence`
  ไม่เก็บ description และ card data
- ทดสอบกับ API จริงในเครื่องแล้ว ได้ครบ 100/100/100
- รันจริงบน R2 วันที่ 2026-11-12 ([run 43074787900](https://github.com/bookchaowalit/data-lake-pipelines/actions/runs/43074787900)) ผ่านทั้ง capture และ heartbeat:
  ได้ 100 model, 100 dataset และ 100 space ลง Bronze
  manifest อยู่ที่ `control/manifests/source=huggingface/batch_id=1352be5a…`, `f2803fae…` และ `aedb4229…`
- deploy แล้ว: uptime Worker target `hf-trending` (26 ชม., version `c09a5867`) และ jobs queue job `hf-trending` (`40 3 * * *` UTC, version `4a126fa5`)

## Top 5 ที่ควร ingest ต่อ

1. **OpenAI + Anthropic costs** → dataset `ai_costs` ตัวเดียว ตอนนี้ใช้ OpenAI key ร่วมกันทุกระบบ
   รู้จำนวนครั้งที่เรียกต่อ subject แต่ไม่รู้ยอดเงิน ต้องมี admin key ทั้งสองเจ้า
2. **Bank of Thailand** อัตราแลกเปลี่ยนและดอกเบี้ยทางการของไทย ใช้แทน Frankfurter (ECB) ใน finance cells ได้ ต้องมี key ฟรีหนึ่งตัว
3. **data.go.th** snapshot ของ catalogue รายวัน (metadata และ licence) ไม่ต้องมี credential
   เป็นตัวเลือก ingest-now ของ batch 4 ถ้าไม่มีแหล่งที่ดีกว่า
4. **Resend** delivery events เข้า cell platform-communication ก่อน และต้อง hash อีเมลก่อนนำออกไป lake
5. **Readwise** export highlights ถ้า owner ยืนยันว่ามี subscription

## สิ่งที่ owner ต้องทำ

| แหล่ง | สิ่งที่ต้องทำ | หน้า | ที่เก็บ key |
|---|---|---|---|
| OpenAI | สร้าง admin key | https://platform.openai.com/settings/organization/admin-keys | Infisical `/openai` `OPENAI_ADMIN_KEY` |
| Anthropic | สร้าง organization และ admin key | https://platform.claude.com/settings/admin-keys | Infisical `/anthropic` `ANTHROPIC_ADMIN_KEY` |
| Bank of Thailand | สมัครบัญชีแล้วสร้าง app key | https://portal.api.bot.or.th/ | Infisical `/bank-of-thailand` `BOT_API_KEY` |
| Resend | สร้าง webhook ไปที่ platform-communication | https://resend.com/webhooks | Infisical `/resend` `RESEND_WEBHOOK_SECRET` |
| Readwise | ยืนยันว่ามี subscription แล้วสร้าง token | https://readwise.io/access_token | Infisical `/readwise` |
| Grab | ดูว่า merchant portal มี CSV export ยอดขายหรือไม่ | https://merchant.grab.com/ | — |
| SET | ตัดสินใจว่าจะซื้อแผน SETSMART หรือไม่ | infoproducts@set.or.th | — |

## คำถามด้านกฎหมายที่ต้องให้ owner ตัดสิน

- **CoinGecko**: market-capture เก็บราคาย้อนหลังลง Bronze อยู่แล้ว (รันด้วยมือ) แต่ Terms §6.1/6.2 ไม่สนับสนุนการเก็บ
  และให้รีเฟรชภายใน 24 ชม. ต้องตัดสินว่าจะเก็บประวัติเดิมไว้ หรือลบ และเปลี่ยนไปใช้แหล่งอื่น
- **JobsDB**: robots.txt อนุญาตหน้า search แต่ยังไม่ได้อ่าน ToS ห้ามตั้งเวลา scraper จนกว่าจะอ่าน
  และห้ามดึงหน้า `*/job/` ทุกกรณี
- **JobThai**: ToS ห้ามคัดลอกข้อมูลถ้าไม่ได้รับอนุญาตเป็นลายลักษณ์อักษร ต้องขอ Thinknet ก่อน
- **Zoom**: recording มีเสียงของคนอื่นอยู่ด้วย จึงต้องขอ consent ตาม PDPA ก่อนเก็บ

## Batch 4 (เรียงไว้แล้ว)

เน้นข้อมูลเปิดที่ทำ ingest-now ได้ทันที:
- ความรู้และเทคโนโลยี: Hacker News, arXiv, OpenAlex, Crossref, Wikimedia pageviews, npm registry,
  PyPI stats, Product Hunt, Google Trends, Stack Exchange
- เศรษฐกิจโลก: World Bank, IMF, OECD
- ข้อมูลไทย: Air4Thai (PM2.5), TMD weather, e-GP procurement, DBD DataWarehouse
- อื่นๆ: Open-Meteo, GDELT, Common Crawl
