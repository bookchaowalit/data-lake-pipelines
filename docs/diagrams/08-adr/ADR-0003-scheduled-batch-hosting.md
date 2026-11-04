# ADR-0003 — Scheduled work runs as short batch jobs: Railway crons by CLI upload, GitHub Actions dispatched by the jobs queue, each with a heartbeat

- Status: accepted (retro-documented)
- Date: retro-documented 2026-11-04 (sheet-summaries `d7ddc9d` 2026-10-26, registry-export `3296a5d` 2026-10-27, jobs queue `aace807` 2026-11-03)

## Context

host เดิมของ repo คือเครื่อง owner (crontab) ซึ่งไม่ได้เปิดตลอด งานต้องรันเองโดยไม่มีค่าใช้จ่ายคงที่ และ Railway ปฏิเสธ `serviceConnect` repo นี้สำหรับ CLI user

## Decision

- งานที่ต้องการ secret ไม่เปิดเผยและ runtime สั้น: Railway project `data-schedules`, service ละงาน, cron + `restartPolicyType NEVER` (owner decision D6)
- deploy ด้วย `railway up` ของ `git archive <commit>` สะอาด; push เข้า main ไม่ redeploy; Dockerfile รัน dry run บน fixture ระหว่าง build เป็น smoke test
- งาน marts และ event sink อยู่บน GitHub Actions (GitHub-hosted, repo public) และไม่ใช้ `schedule:` แต่ให้ platform-jobs-worker (ADR-0008) สั่ง `workflow_dispatch` เพราะ GitHub cron เริ่มช้าและไม่มี retry; queue retry และพักไว้ใน `jobs-dlq`
- log บน runner public พิมพ์แค่จำนวน; stderr เก็บใน temp file
- ทุกงานส่ง heartbeat ไปที่ uptime Worker (`monitor.bookchaowalit.com`) และ Sentry แบบ opt-in

## Consequences

- live commit ของ Railway services ต้องบันทึกเอง (เช่น `docs/sheet-summaries.md` "First deploy 2026-10-26: d7ddc9d")
- heartbeat ใหม่ต้องเพิ่ม entry ใน uptime Worker ด้วยถึงจะ alert
- งาน capture บางส่วนยังอยู่บน crontab ของ owner

## Evidence

`docs/sheet-summaries.md` (ตาราง Where it runs), `README.md` (§Export อัตโนมัติ), `deploy/*/Dockerfile`, `.github/workflows/{jobs-marts,event-sink,data-lake-cloud-contract,free-cloud-e2e}.yml` (comment "Scheduled by the jobs queue"), `infra/scripts/data_lake/setup_*_cron.sh`, commits `d7ddc9d`, `3296a5d`, `aace807`, `141d142`, `8fc9098`
