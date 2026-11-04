# 05 — C4 Context

```mermaid
C4Context
  title solo-data-lake (data-lake-pipelines) - system context
  Person(owner, "Owner", "Runs local captures, deploys Railway crons, reads BI")
  System(dlp, "data-lake-pipelines", "Lake-first batch pipelines: ingest, Silver, dbt Gold, event sink, metadata export, BI build")
  SystemDb_Ext(r2, "Cloudflare R2", "Bucket solo-empire-portfolio: landing, bronze, silver, gold, control")
  System_Ext(products, "Product repos", "book-news-scraping, book-job-data, career-job-scraper (Track B)")
  System_Ext(jobsq, "platform-jobs-worker", "Jobs queue, dispatches workflows, jobs-dlq")
  System_Ext(kafka, "Aiven Kafka", "commerce, platform, data.ingest, ops events v1")
  System_Ext(intake, "intake-api", "x-feed pulls")
  System_Ext(github, "GitHub", "solo-empire registries, architecture-docs catalog and landscape page")
  System_Ext(sheets, "Google Sheets", "Chief Findings Warehouse")
  System_Ext(gateway, "AI Gateway", "api.bookchaowalit.com /api/ai/generate")
  System_Ext(pages, "Cloudflare Pages + Access", "jobs-insights.pages.dev")
  System_Ext(uptime, "platform-uptime-worker", "monitor.bookchaowalit.com heartbeats")
  System_Ext(crm, "sales-crm job-radar", "Reads and writes income/remote_job_feeds")
  System_Ext(sentry, "Sentry", "Opt-in, scrubbed")

  Rel(owner, dlp, "Runs, deploys, reviews")
  Rel(owner, pages, "Reads jobs BI", "Cloudflare Access")
  Rel(jobsq, dlp, "workflow_dispatch on schedule")
  Rel(products, dlp, "Capture files and Bronze job_postings")
  Rel(dlp, kafka, "Consume group lake-sink, produce heartbeat", "SASL_SSL")
  Rel(dlp, intake, "List landed pulls, mark exported", "HTTPS bearer")
  Rel(dlp, github, "Read registries, commit landscape", "GitHub API")
  Rel(dlp, sheets, "Read tabs, append AI Summaries")
  Rel(dlp, gateway, "Summaries", "HTTPS")
  Rel(dlp, r2, "Immutable writes and reads", "S3 API via aws CLI")
  Rel(dlp, pages, "wrangler deploy of static site")
  Rel(dlp, uptime, "Heartbeats")
  Rel(dlp, sentry, "Static failure events")
  Rel(crm, r2, "Same lake contract")
```

Sources: `README.md`, `PRODUCT.md`, `docs/sheet-summaries.md`, `docs/plans/2026-11-02-jobs-silver-mart-bi.md`, `infra/scripts/data_lake/{storage,stream_sink,scheduled_registry_export,jobs_marts}.py`, `infra/scripts/sync/x_feed_lake_export.py`, `.github/workflows/{jobs-marts,event-sink}.yml` (ที่ `3c9cc2a`)

ภาพรวมระดับ platform อยู่ใน architecture-docs (`docs/architecture/C4-DIAGRAMS.md`, `operations/platform/SYSTEM-LANDSCAPE-GENERATED.md` ที่ repo นี้ generate)

## Open questions

- `x_feed_lake_export.py` (merge `15a5e6f` 2026-11-04) ยังไม่มี schedule ใน repo: จะรันจาก Railway, jobs queue หรือเครื่อง owner
- Telegram channel / Brave bookmark collectors และ diary intake รันที่ไหนและเขียน lake ใด: repo ไม่มี scheduler ของมัน
- consumer อื่นของ Gold jobs นอกจาก BI site มีหรือไม่ (`book-job-data` API อ่าน Silver/Gold?)
