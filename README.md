# data-lake

Pipeline และ collector ของ lake-first data products ของ Solo Empire: landing → Bronze Parquet → Silver, scraping coverage/health, scheduled capture (news/market/SEO/jobs), discovery portfolio, Telegram/Brave collectors, diary intake, และ dbt projection

ที่มา: ย้ายจาก `solo-empire` เมื่อ 2026-10-11 พร้อมประวัติ git ของ `infra/scripts/data_lake`, `infra/scripts/discovery`, `infra/scripts/opportunities`, `infra/tests/data_lake`, `infra/dbt`.
repo นี้คงโครงสร้าง `infra/...` เดิมไว้ เพื่อให้ `ROOT = parents[3]` และ path ใน test ทำงานเหมือนเดิม.

- `infra/scripts/data_lake/` — 47 modules (ingest/storage/query/silver/product_adapter ฯลฯ + run_* capture + cron setup)
- `infra/scripts/{discovery,opportunities}/`, `infra/scripts/sync/{sync_brave_bookmarks,brave_bookmark_workflow,organize_brave_bookmarks,telegram_channels,telegram_personal}.py`, `infra/scripts/cli/diary-intake-cli.py`
- `infra/scripts/sync/sheet_summaries.py` — สรุปภาษาไทยรายวันของชีต "Chief Findings Warehouse" ผ่าน AI gateway (Railway cron `sheet-summaries`, 19:00 เวลาไทย) ดู [`docs/sheet-summaries.md`](docs/sheet-summaries.md)
- `infra/scripts/utils/` — สำเนา vendored ของ utils ที่โค้ดข้างบนต้องใช้ (`_env`, `cell_store`, `task_store`, `secret_resolver`, `job_client`, `data_product_contract_guard`, …)
- `infra/dbt/` — dbt projection (`dbt_lake_test.py`) รวม metadata models ใน `models/metadata/` (`meta_source_coverage`, `meta_source_lineage`, `meta_lake_datasets`)
- `infra/scripts/data_lake/registry_lake_export.py` — ส่ง registry ของ solo-empire (data-sources, online-systems) และ metadata catalog ของ architecture-docs (catalog, lineage) เข้า lake เป็น Bronze `domain=metadata` ดู [Metadata ใน lake](#metadata-ใน-lake)
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
uv sync --all-extras    # จาก uv.lock (pyproject.toml ตรงกับ infra/requirements-data-lake.txt); หรือ uv venv && uv pip install -r requirements.txt
for t in $(find infra/tests -name '*_test.py' | sort); do .venv/bin/python "$t" || echo "FAIL $t"; done
```

ใช้ `.venv` ที่ root ของ repo เพราะ `setup_*_cron.sh` เลือก `.venv/bin/python` ก่อน. ไม่มีตัวแปร env ที่ต้องตั้งเพื่อรัน suite
(ถ้าต้องการ ทดสอบแบบไม่มี product repo: ไม่ตั้ง `SOLO_BOOK_DEV_ROOT`).

ข้อจำกัดที่ทราบ: `news_scheduler_contract_test` สอง test ที่รัน `book-news-scraping/setup_cron.sh` จะ skip เพราะสคริปต์นั้นหา
`run_news_capture.py` โดยไล่ขึ้นไปตาม ancestor ของตัวเอง (ตอนนี้ runner อยู่ใน repo นี้ ต้องแก้ฝั่ง book-news-scraping ให้รับ root จาก env).

## Metadata ใน lake

ต้นฉบับยังอยู่ใน Git: registry ใน `solo-empire/repository-catalog/registries` และ catalog/lineage ใน
`architecture-docs/systems/ai-data/metadata-catalog`. Exporter อ่านสอง checkout นี้แล้วเขียน Bronze 6 dataset
(`data_sources`, `systems`, `catalog_assets`, `lineage_edges`, `capability_domains`, `system_capabilities`) ผ่าน ingest ปกติ. ส่งเฉพาะโครงสร้าง ไม่ส่ง `notes`.
batch อิงเนื้อหา รันซ้ำบน registry เดิมจะไม่เขียนอะไรใหม่.

```bash
.venv/bin/python infra/scripts/data_lake/registry_lake_export.py \
  --registries ~/solo-empire/repository-catalog/registries \
  --catalog <architecture-docs>/systems/ai-data/metadata-catalog
cd infra/dbt && ../../.venv/bin/dbt build --profiles-dir . --select path:models/metadata
../../.venv/bin/dbt docs generate --profiles-dir .   # lineage ของทุก model ใน target/
```

- `meta_source_coverage` — หนึ่งแถวต่อ flow: automation, system และ host, จำนวน catalog asset และ consumer ที่ไปถึง
- `meta_source_lineage` — asset ทุกตัวที่ flow ไปถึง (จาก `catalog_assets` ของ flow แล้วตาม lineage edges)
- `meta_lake_datasets` — dataset ที่ lake มีจริงตาม manifest
- `meta_system_landscape` — หนึ่งแถวต่อ system: host, EOS capability domain (primary ก่อน), inbound flow, asset และ consumer ที่ไปถึง
- `meta_capability_coverage` — หนึ่งแถวต่อ EOS domain (15 ตัว จาก `business-capabilities.yaml`): จำนวน system, ที่รันอยู่,
  automated flow, consumer และ `platform_coverage` (missing = ไม่มี system รัน, partial = รันแต่ไม่มี automated flow, served = มีทั้งสอง)

หน้า Markdown ของ landscape เดียวกันนี้ render จากไฟล์ต้นทางชุดเดียวกัน (ไม่ต้องมี lake) แล้ว commit ไว้ที่
`architecture-docs/operations/platform/SYSTEM-LANDSCAPE-GENERATED.md`. ผลลัพธ์ไม่มี timestamp จึง diff ได้ และ `--check` บอกว่าไฟล์เก่าหรือไม่:

```bash
.venv/bin/python infra/scripts/data_lake/system_landscape.py \
  --registries ~/solo-empire/repository-catalog/registries \
  --catalog <architecture-docs>/systems/ai-data/metadata-catalog \
  --output <architecture-docs>/operations/platform/SYSTEM-LANDSCAPE-GENERATED.md   # เพิ่ม --check เพื่อตรวจอย่างเดียว
```

Gate: solo-empire `registry_check.py` (guard `business-capabilities`) fail ถ้ามี system ใน online-systems.yaml ที่ไม่ได้ map domain,
และ dbt test `meta_system_landscape.capability_mapped` fail ถ้าข้อมูลใน lake มี system ที่ไม่มี domain.

metadata models อ่าน batch ล่าสุดเท่านั้น จึงต้อง export อย่างน้อยหนึ่งครั้งก่อน `dbt build`.

### Export อัตโนมัติ (Railway cron `registry-export`)

Railway project `data-schedules`, service `registry-export`, cron `40 23 * * *` UTC (06:40 เวลาไทย), restart NEVER.
แต่ละรอบรัน `infra/scripts/data_lake/scheduled_registry_export.py`: ดึงไฟล์ต้นทาง 5 ไฟล์จาก `main` ของ solo-empire และ
architecture-docs ผ่าน GitHub API แล้ว export เข้า R2 lake เดียวกับ news-capture (เขียนแค่ `bronze/domain=metadata`,
`landing/source=solo-empire-registry`, `control/*/source=solo-empire-registry`) พิมพ์แค่จำนวนแถว และส่ง heartbeat `registry-export`
ไปที่ uptime Worker (แจ้งเตือนถ้าเกิน 26 ชม. หรือรอบที่ล้มเหลว)

| Variable | Value |
|---|---|
| `GITHUB_TOKEN` | fine-grained token, Contents: read บน solo-empire และ architecture-docs |
| `SOLO_EMPIRE_DATA_LAKE_URI` | `s3://solo-empire-portfolio/portfolio-demo/free-cloud-e2e-001` |
| `DATA_LAKE_CLOUD_WRITE_ENABLED` | `true` |
| `DATA_LAKE_S3_ENDPOINT`, `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION=auto` | R2 key ของ bucket `solo-empire-portfolio` จาก Infisical `/job-data` |
| `HEARTBEAT_TOKEN` | Infisical `/monitoring` |

Deploy: CLI upload (`railway up` ของ `git archive`, `RAILWAY_DOCKERFILE_PATH=deploy/registry-export/Dockerfile`) แบบเดียวกับ
sheet-summaries; push ขึ้น main ไม่ redeploy เอง. image build รัน dry run บน fixture ถ้าพังจะ build ไม่ผ่าน.
ปิด: ลบ cron ของ service ใน Railway. ล้างข้อมูล: ลบสาม prefix ข้างบนใน bucket (ไม่มี dataset อื่นใช้ร่วม).
