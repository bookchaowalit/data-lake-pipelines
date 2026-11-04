# 06 — C4 Container

```mermaid
C4Container
  title data-lake-pipelines - containers (all batch, no long-running service)
  System_Ext(jobsq, "platform-jobs-worker")
  System_Ext(kafka, "Aiven Kafka")
  System_Ext(github, "GitHub API")
  System_Ext(sheets, "Google Sheets")
  System_Ext(gateway, "AI Gateway")
  System_Ext(pages, "Cloudflare Pages")
  System_Ext(uptime, "uptime Worker")
  Person(owner, "Owner")

  Container_Boundary(rw, "Railway project data-schedules") {
    Container(regexp, "registry-export", "Python 3.12 cron, restart NEVER", "40 23 * * * UTC; scheduled_registry_export.py; deploy/registry-export/Dockerfile")
    Container(sheet, "sheet-summaries", "Python 3.12 cron, restart NEVER", "0 12 * * * UTC; sheet_summaries.py; deploy/sheet-summaries/Dockerfile")
    Container(news, "news-capture", "Railway cron", "run_news_capture.py, every 2 h per ops record")
  }
  Container_Boundary(gha, "GitHub Actions, GitHub-hosted, public logs") {
    Container(marts, "jobs-marts.yml", "build + bi jobs", "jobs_marts.py -> dbt-duckdb -> Gold; jobs_bi_snapshot + jobs_bi_site -> wrangler")
    Container(sink, "event-sink.yml", "hourly", "event_heartbeat.py, stream_sink.py sink --until-empty, convert")
    Container(checks, "ci / contract workflows", "ci.yml, data-lake-cloud-contract, free-cloud-e2e, silver-parity, preflight", "never write cloud")
  }
  Container_Boundary(local, "Owner machine") {
    Container(crons, "Capture crons", "crontab via setup_*_cron.sh", "run_jobs/market/seo_capture.py, capture_lock")
    Container(cli, "Collectors and CLIs", "Python", "x_feed_lake_export, telegram, brave, diary intake, registry_lake_export, system_landscape")
  }
  ContainerDb(r2, "R2 lake", "Parquet, Avro, JSON manifests", "landing / bronze / silver / gold / control")
  ContainerDb(llake, "Local lake", "file:// + SQLite Iceberg catalog", "dev and owner captures")

  Rel(jobsq, marts, "workflow_dispatch 47 */2 * * *")
  Rel(jobsq, sink, "workflow_dispatch 5 * * * *")
  Rel(jobsq, checks, "weekly dispatch")
  Rel(owner, regexp, "railway up of git archive")
  Rel(owner, sheet, "railway up of git archive")
  Rel(regexp, github, "Read 5 source files, commit landscape page")
  Rel(regexp, r2, "bronze/domain=metadata")
  Rel(sheet, sheets, "Read tabs, append rows")
  Rel(sheet, gateway, "Summaries")
  Rel(news, r2, "News Bronze, health check")
  Rel(marts, r2, "Read Bronze/Silver, write Silver and Gold")
  Rel(marts, pages, "Deploy when gold_run_id@commit changes")
  Rel(sink, kafka, "Consume and commit after lake write")
  Rel(sink, r2, "landing/stream Avro, bronze Parquet")
  Rel(crons, llake, "Ingest")
  Rel(cli, r2, "Ingest when cloud write enabled")
  Rel(regexp, uptime, "heartbeat registry-export")
  Rel(marts, uptime, "heartbeat jobs-marts")
  Rel(sink, uptime, "heartbeat event-sink")
  Rel(sheet, uptime, "heartbeat sheet-summaries")
```

Sources: `deploy/registry-export/Dockerfile`, `deploy/sheet-summaries/Dockerfile`, `docs/sheet-summaries.md`, `README.md` (§Jobs, §Event plane, §Export อัตโนมัติ), `.github/workflows/*.yml`, `infra/scripts/data_lake/{setup_jobs_cron.sh,capture_lock.py,storage.py,run_news_capture.py}`, `infra/scripts/sync/x_feed_lake_export.py` (ที่ `3c9cc2a`)

## Open questions

- `news-capture`: repo นี้มี runner (`run_news_capture.py`) แต่ไม่มี Dockerfile/Railway config ของ service; image สร้างจากไหนและ commit ไหน live (ops record: CLI deploy lake#16, 2026-10-28; `3c9cc2a` แก้ health check 2026-11-04 ยังไม่ทราบว่า deploy แล้วหรือไม่)
- crontab ของ jobs/market/seo บนเครื่อง owner ยังเปิดอยู่หรือไม่ หลัง jobs ย้ายไป Track B บน GitHub Actions
- heartbeat `sheet-summaries` ต้องมี entry ใน uptime Worker ก่อนจะ alert (`docs/sheet-summaries.md`): มีแล้วหรือยัง
