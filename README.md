# data-lake

Pipeline และ collector ของ lake-first data products ของ Solo Empire: landing → Bronze Parquet → Silver, scraping coverage/health, scheduled capture (news/market/SEO/jobs), discovery portfolio, Telegram/Brave collectors, diary intake, และ dbt projection

ที่มา: ย้ายจาก `solo-empire` เมื่อ 2026-10-11 พร้อมประวัติ git ของ `infra/scripts/data_lake`, `infra/scripts/discovery`, `infra/scripts/opportunities`, `infra/tests/data_lake`, `infra/dbt`.
repo นี้คงโครงสร้าง `infra/...` เดิมไว้ เพื่อให้ `ROOT = parents[3]` และ path ใน test ทำงานเหมือนเดิม.

- `infra/scripts/data_lake/` — 47 modules (ingest/storage/query/silver/product_adapter ฯลฯ + run_* capture + cron setup)
- `infra/scripts/{discovery,opportunities}/`, `infra/scripts/sync/{sync_brave_bookmarks,brave_bookmark_workflow,organize_brave_bookmarks,telegram_channels,telegram_personal}.py`, `infra/scripts/cli/diary-intake-cli.py`
- `infra/scripts/utils/` — สำเนา vendored ของ utils ที่โค้ดข้างบนต้องใช้ (`_env`, `cell_store`, `task_store`, `secret_resolver`, `job_client`, `data_product_contract_guard`, …)
- `infra/dbt/` — dbt projection (`dbt_lake_test.py`)
- `infra/tests/` — test ทั้งหมดของโค้ดข้างต้น
- ผลิตภัณฑ์ข้อมูล `job.v1` (ผู้บริโภคคือ `book-job-data` และ `crm` job-radar) — `book-job-data` ยังเป็น repo ของตัวเอง; ที่นี่มีเฉพาะ adapter/readiness/e2e

## ความสัมพันธ์กับ repo อื่น

- `data-lake-core` (public) เป็น runtime แพ็กเกจพกพา; README ของมันระบุว่า "parent `infra/scripts/data_lake`" เป็น canonical สำหรับ 9 modules ที่ใช้ร่วม — canonical นั้นย้ายมาที่ `infra/scripts/data_lake/` ใน repo นี้แล้ว (ต้องแก้ `scripts/check_parity.py` ของ public repo ให้ชี้มาที่นี่ — ยังไม่ได้ทำ)
- `event-hub` มีสำเนา vendored ของ ingest/storage/parquet/iceberg

## Self-contained

repo นี้ไม่มี symlink ไปยัง solo-empire. สิ่งที่ต้องใช้จริงถูกคัดลอกมาไว้ใน repo:

- `systems/architecture/` และ `systems/ai-data/` — data-product / discovery / scraping contracts และ scraping coverage registry (สำเนา; ต้นฉบับอยู่ใน `architecture-docs` `systems/`)
- `templates/discovery-profile.example.json`, `repository-catalog/registries/projects.yaml`
- `infra/database/` — schema และ migrations (ใช้โดย SQLite cell doubles ใน test)
- `infra/scripts/cli/data-intake-cli.py` — สำเนาจาก `event-hub` (`diary-intake-cli.py` เรียกใช้)

**Product repositories** (`book-news-scraping`, `book-job-data`, `book-job-scraping`, `book-*-data`) เป็น repo แยก. runner/ingest อ่านจาก
`$SOLO_BOOK_DEV_ROOT` (โฟลเดอร์ Book Dev ที่มี `github/` อยู่ข้างใน; ไม่มีค่าเริ่มต้น, ดู `infra/scripts/data_lake/workspace.py`). ถ้าไม่พบ runner จะรายงานว่า source หายไป
และ test ที่ต้องใช้ product repo จะ **skip** พร้อมเหตุผล (ไม่ fail). `BOOK_JOB_DATA_ROOT` override path ของ book-job-data ได้.

## ติดตั้งและทดสอบ

```bash
uv venv && uv pip install -r requirements.txt     # pyarrow, duckdb, pyiceberg, httpx, dbt-duckdb, ...
for t in $(find infra/tests -name '*_test.py' | sort); do .venv/bin/python "$t" || echo "FAIL $t"; done
```

ใช้ `.venv` ที่ root ของ repo เพราะ `setup_*_cron.sh` เลือก `.venv/bin/python` ก่อน. ไม่มีตัวแปร env ที่ต้องตั้งเพื่อรัน suite
(ถ้าต้องการ ทดสอบแบบไม่มี product repo: ไม่ตั้ง `SOLO_BOOK_DEV_ROOT`).

ข้อจำกัดที่ทราบ: `news_scheduler_contract_test` สอง test ที่รัน `book-news-scraping/setup_cron.sh` จะ skip เพราะสคริปต์นั้นหา
`run_news_capture.py` โดยไล่ขึ้นไปตาม ancestor ของตัวเอง (ตอนนี้ runner อยู่ใน repo นี้ ต้องแก้ฝั่ง book-news-scraping ให้รับ root จาก env).

## Platform integrations

Public plugin catalogs และ approved remote MCP references เก็บ exact raw → Bronze → immutable manifests ก่อน atomic publication ให้ owning integrations API ไม่มีการอ่านข้อมูลบัญชีหรือเขียน application database ดูคำสั่ง replay/fetch, source governance และ failure/quarantine policy ใน [platform-integrations.v1](contracts/platform-integrations.v1.md) และ source allowlist ใน `systems/ai-data/platform-integration-sources.json`.
