# ADR-0002 — One object-store abstraction; cloud writes off unless explicitly enabled

- Status: accepted (retro-documented)
- Date: retro-documented 2026-11-04 (cloud lake MVP 2026-08-07)

## Context

lake contract ต้องรันได้ทั้ง local (dev, fixture, test, owner crontab) และ hosted (R2) โดย test หรือ workflow ใน repo public ต้องไม่เขียน bucket จริงโดยไม่ตั้งใจ

## Decision

- `storage.ObjectStore` รับ `file://` หรือ `s3://`; S3-compatible (R2) ใช้ AWS CLI (`aws`) ผ่าน subprocess ด้วย `DATA_LAKE_S3_ENDPOINT`
- put แบบ immutable: ตรวจ `head` ก่อนเขียน และปฏิเสธการเขียนทับที่เนื้อหาต่างกัน
- การเขียน cloud ต้องตั้ง `DATA_LAKE_CLOUD_WRITE_ENABLED=true` เอง มิฉะนั้น put ถูกปฏิเสธ
- local target ใช้ SQLite Iceberg catalog; hosted ใช้ R2 (และ R2 Data Catalog ตาม MVP doc)
- workflow แบบ contract (`data-lake-cloud-contract`, `free-cloud-e2e`) ไม่เขียน cloud

## Consequences

- image ที่เขียน R2 ต้องติดตั้ง awscli (`deploy/registry-export/Dockerfile`)
- hosted lake ไม่ใช่ dual-write replica ของ local (MVP doc)
- secret R2 แยกตาม bucket/งาน (`JOB_DATA_*` ใน GitHub, Infisical `/job-data`)

## Evidence

`infra/scripts/data_lake/storage.py` (`ObjectStore`, `_run_aws`, `put_bytes`), `systems/ai-data/cloud-data-lake-mvp.md`, `infra/tests/data_lake/cloud_lake_contract_test.py`, `.github/workflows/data-lake-cloud-contract.yml`, `PRODUCT.md` (What works)
