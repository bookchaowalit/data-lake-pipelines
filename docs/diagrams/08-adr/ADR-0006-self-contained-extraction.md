# ADR-0006 — Self-contained extraction from solo-empire: keep the `infra/...` layout, vendor copies

- Status: accepted (retro-documented)
- Date: retro-documented 2026-11-04 (imports `125663c`, `2bea861`, `355af14`, 2026-10-11)

## Context

pipeline เดิมอยู่ใน monorepo solo-empire และอ้าง path จาก root (`ROOT = parents[3]`) รวมถึง contract และ utils ของ monorepo การแยก repo ต้องไม่ทำให้ test และ cron พัง

## Decision

- import `infra/scripts/data_lake`, `discovery`, `opportunities`, `infra/tests/data_lake`, `infra/dbt` พร้อมประวัติ git และคง path `infra/...` เดิม
- ไม่มี symlink ไป solo-empire: คัดลอก `systems/architecture/`, `systems/ai-data/`, `templates/`, `repository-catalog/registries/projects.yaml`, `infra/database/`, `infra/scripts/utils/` (vendored) และ `data-intake-cli.py` เข้ามา
- product repos อ่านจาก `$SOLO_BOOK_DEV_ROOT` (ไม่มีค่าเริ่มต้น); ไม่พบ = runner รายงาน source หาย และ test skip พร้อมเหตุผล (CI ตั้ง `SOLO_EMPIRE_CI=1` ให้ skip ที่ขึ้นกับ runtime กลายเป็น fail)

## Consequences

- สำเนา contract อาจ drift จากต้นฉบับใน architecture-docs
- canonical ของ 9 modules ที่ `data-lake-core` ใช้ร่วมย้ายมาที่นี่ แต่ `check_parity.py` ของ repo นั้นยังไม่ชี้มา; `event-hub` มีสำเนา vendored ของ ingest/storage/parquet/iceberg
- vendored utils ยังไม่ผ่าน mypy (15 errors)

## Evidence

`README.md` (บรรทัดแรก, §ความสัมพันธ์กับ repo อื่น, §Self-contained), `PRODUCT.md` (Limits), `infra/scripts/data_lake/workspace.py`, `pyproject.toml` (`[tool.mypy]` comment), commits `125663c`, `2bea861`, `355af14`
