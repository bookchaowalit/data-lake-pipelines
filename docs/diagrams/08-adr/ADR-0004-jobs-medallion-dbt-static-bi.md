# ADR-0004 — Jobs medallion: one Silver snapshot part → dbt-duckdb → Gold manifest, BI as a static site

- Status: accepted (retro-documented)
- Date: retro-documented 2026-11-04 (plan 2026-11-02; marts `6115fdd` 2026-11-02; BI `3a0d5f8` 2026-11-03)

## Context

jobs เป็น domain แรกที่ต้องครบ Bronze → Silver → Gold → BI ต้นทุน $0 และรันซ้ำได้ Silver ทุกรอบเป็น full snapshot การ glob ทุก part จะนับซ้ำ

## Decision

- `silver.py` job.silver.v2: quarantine ≤ 2 % ไป `job_postings_rejects`, ตัด `offline_fixture`, เกินเกณฑ์ = fail-closed ก่อนเขียน
- `jobs_marts.py` สร้างหรือหา Silver snapshot ของ Bronze ปัจจุบัน แล้วส่ง part เดียวให้ dbt (`--vars jobs_silver_part`) ใน scratch DuckDB ที่ทิ้งหลังรัน; marts export เป็น Parquet deterministic และเขียน Gold ครั้งเดียวพร้อม Gold manifest; run id อิงเนื้อหา
- BI: `jobs_bi_snapshot.py` ตรวจ sha256 ตาม manifest → `jobs_bi_site.py` render HTML ด้วย stdlib (SVG ฝั่ง server, CSP เข้ม, ADR-0007 tokens) → Cloudflare Pages หลัง Cloudflare Access; deploy เมื่อ `<gold_run_id>@<commit>` เปลี่ยน
- แทน Evidence.dev ที่ owner เลือกตอนแรก (D2) เพราะ framework open-source หยุดพัฒนา, lockfile มี 144 OSV advisories และ DuckDB-WASM เกินขีด 25 MiB ของ Pages
- D1: v1 marts เป็น postings เท่านั้น ไม่เอา `job_matches` (คะแนนส่วนตัว) เข้าสิ่งที่แชร์

## Consequences

- ผู้อ่าน Gold ต้องอ่าน manifest ล่าสุด ไม่ใช่ glob
- site ไม่มี query แบบ interactive นอกจาก filter script ที่มากับหน้า
- D3: `remote_job_feeds` (domain `income`) เก็บไว้แยกเพราะ sales-crm job-radar ใช้อยู่

## Evidence

`docs/plans/2026-11-02-jobs-silver-mart-bi.md` (D1–D3, Task 1–6), `infra/scripts/data_lake/{silver,jobs_marts,jobs_bi_snapshot,jobs_bi_site}.py`, `infra/dbt/models/marts/jobs/`, `.github/workflows/jobs-marts.yml`, `README.md` (§Jobs), commits `ba98f09`, `6115fdd`, `ee184a1`, `346b150`, `3a0d5f8`, `a54908e`
