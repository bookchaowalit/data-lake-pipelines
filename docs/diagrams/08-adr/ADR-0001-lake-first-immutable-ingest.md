# ADR-0001 — Lake-first ingest: immutable landing, Bronze and manifest before any projection

- Status: accepted (retro-documented)
- Date: retro-documented 2026-11-04 (cloud lake MVP doc 2026-08-07; imported with history `125663c`, 2026-10-11)

## Context

data product ของ owner มาจาก scraping, API และ collector หลายตัว ต้อง replay และ audit ได้ และ consumer (SQLite/Neon/app) ไม่ควรเป็น source of truth

## Decision

- `ingest.py` เขียน artifact immutable สามชิ้นต่อ batch: `landing/` (bytes ต้นทางตรงตัว), `bronze/` (Parquet envelope หนึ่งแถวต่อ record), `control/manifests/` (audit manifest เป็น commit marker)
- ไม่มี SQLite หรือ Ops table ถูกเขียนในขั้นนี้; Silver/Gold/ตาราง operational project จาก Bronze หลัง ingest สำเร็จเท่านั้น
- batch id อิงเนื้อหา รันซ้ำบนข้อมูลเดิมไม่เขียนอะไรใหม่
- pattern เดียวกันใช้กับ stream (Avro landing → Bronze) และ consumer ภายนอก (sales-crm job-radar)

## Consequences

- replay = อ่าน Bronze/landing ใหม่ ไม่ต้องดึงต้นทางซ้ำ
- จำนวน manifest โตตามเวลา: การอ่าน manifest ทั้งหมดทำให้ news-capture เกิน timeout 900 s (2026-10-28) ต้องแก้เป็นอ่านทีละ prefix (`5fb21c9`) และจำกัดช่วง partition ใน health check (`3c9cc2a`)
- ยังไม่มีการลบหรือ compaction: `maintenance.py` เป็นเครื่องมืออ่านอย่างเดียวที่รายงาน retention/orphan/small-file และ restore หนึ่ง manifest; expiry/compaction ตั้งใจยังไม่ทำจนกว่า operator ตรวจรายงาน

## Evidence

`infra/scripts/data_lake/ingest.py` (docstring), `systems/ai-data/cloud-data-lake-mvp.md`, `infra/scripts/data_lake/registry_lake_export.py`, `infra/scripts/data_lake/product_adapter.py` (comment 900 s), `infra/scripts/data_lake/maintenance.py`, commits `125663c`, `ab41749`, `5fb21c9`, `3c9cc2a`
