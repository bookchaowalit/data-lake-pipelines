---
title: "Source coverage — batch 4 (2026-11-12)"
status: current
created: 2026-11-12
registry: ../../repository-catalog/registries/source-coverage.yaml
rows: ../../repository-catalog/registries/source-coverage/batch-04.rows.yaml
---

# Source coverage รอบที่ 4 (20 แหล่ง, ข้อมูลเปิด, 2026-11-12)

รอบนี้เป็นข้อมูลเปิดทั้งหมด ได้แก่ ความรู้และเทคโนโลยี, เศรษฐกิจโลก, ข้อมูลภาครัฐไทย และข่าว/เว็บ

## ภาพรวม

ทั้ง 20 แหล่งมี official API มีแค่ 2 แหล่งที่ต้องมี credential คือ Product Hunt และ Google Trends (alpha ที่ต้องได้รับเชิญ)

| Verdict | จำนวน | แหล่ง |
|---|---|---|
| ingest-now | 1 | Wikimedia pageviews (top articles th + en) |
| later | 14 | Hacker News, arXiv, OpenAlex, npm, PyPI, Google Trends, Stack Exchange, World Bank, Air4Thai, TMD, e-GP, DBD, Open-Meteo, GDELT |
| skip | 4 | Crossref (ซ้ำกับ OpenAlex), IMF, OECD (ไทยไม่ใช่สมาชิก), Common Crawl (ขนาดระดับ TB และมี PII) |
| blocked-legal | 1 | Product Hunt (ตาม terms ตั้งต้น ห้ามใช้เชิงพาณิชย์) |

**ข้อควรระวังด้าน licence** ที่ verdict ไม่ได้บอกไว้ตรงๆ:
- arXiv: เก็บได้เฉพาะ metadata (CC0) ห้ามเก็บ PDF
- Open-Meteo: free tier ใช้ได้เฉพาะงานที่ไม่ใช่เชิงพาณิชย์ และ lake ของเราป้อนข้อมูลให้สินค้าที่ขายได้
- Stack Exchange: dump ปี 2024 ห้ามใช้เชิงพาณิชย์และห้าม train LLM
- GDELT: ต้องอ้างอิงที่มาทุกครั้ง

## ingest-now: Wikipedia top articles

- spec `docs/specs/wiki-pageviews.md`, workflow `wiki-pageviews` รันทุกวัน 03:50 UTC และมี heartbeat `wiki-pageviews`
- ข้อมูลเป็น CC0 ไม่ต้องมี credential ส่ง User-Agent ที่ระบุตัวตนชัดเจนตาม access policy และยิงทีละ request
- แต่ละวันเก็บ top 200 บทความของ th.wikipedia และ en.wikipedia ลง Bronze `domain=market-intelligence/dataset=wiki_top_articles`
  ข้อมูลของวันหนึ่งเผยแพร่วันถัดไป จึงดึงย้อนหลัง 2 วัน
- ทดสอบกับ API จริงในเครื่องแล้ว ได้ 400 แถว (วันที่ 2026-11-09)
- ผลการรันบน R2 อยู่ในส่วนบันทึกการรันด้านล่าง

## Top 5 ที่ควร ingest ต่อ (ทุกตัวไม่ต้องมี credential)

1. **arXiv** เก็บ metadata ของ cs.AI/cs.CL ทุกวันผ่าน OAI-PMH เป็นข้อมูลให้แผนเรียน AI governance (เคารพเงื่อนไข 1 request ต่อ 3 วินาที)
2. **GDELT** กรองเฉพาะ event ที่เกี่ยวกับไทยในแต่ละวันก่อนลง R2
3. **Air4Thai** ค่า PM2.5 รายวัน ต้องตรวจก่อนว่า TLS chain ยืนยันได้จาก runner (ห้ามปิดการตรวจ TLS)
4. **World Bank** ชุด indicator ของไทย ดึงเดือนละครั้ง
5. **Hacker News** เก็บเฉพาะ id, score และ title ถ้า owner โอเค

## สิ่งที่ owner ต้องตัดสินหรือทำ

| แหล่ง | สิ่งที่ต้องทำ | หน้า |
|---|---|---|
| Hacker News | ตกลงว่าเก็บเฉพาะ title (ไม่เก็บ comment) ได้ไหม | — |
| Product Hunt | ถ้าต้องการใช้ ต้องขออนุญาตใช้เพื่อ market research | hello@producthunt.com |
| Google Trends | สมัคร alpha ของ API ทางการ (ไม่ scrape) | https://developers.google.com/search/apis/trends |
| TMD | สมัครใช้ NWP API | https://data.tmd.go.th/nwpapi/register |
| e-GP | สร้าง developer token และบอกว่าใช้ประกาศจัดซื้อจัดจ้างภาครัฐเป็นแหล่ง lead ไหม | https://data.go.th/ |
| DBD | ตัดสินว่าข้อมูลบริษัทจาก BDEX คุ้มกับการสมัครและค่าธรรมเนียมไหม | https://bdex.dbd.go.th/opendata |

## Batch 5 (เรียงไว้แล้ว)

เน้นอสังหาฯ, ท่องเที่ยว และข่าว ซึ่งเกี่ยวกับธุรกิจ real-estate และ pSEO:
- อสังหาฯ: DDproperty, Livinginsider, Hipflat, Facebook Marketplace
- แผนที่: Google Places, OpenStreetMap (Overpass)
- ท่องเที่ยว: Agoda, Booking.com, Airbnb, Tripadvisor
- ข่าวไทย: Thairath, Thai PBS, Bangkok Post, Matichon, Blognone
- ข่าวเทคต่างประเทศ: TechCrunch, The Verge
- กระแส: YouTube trending, TikTok Creative Center, Google News

batch นี้คาดว่าจะมี blocked-legal หลายแหล่ง เพราะเว็บประกาศอสังหาฯ และเว็บจองที่พักมักห้าม scrape
