# Public platform integration collection

Collector: `infra/scripts/data_lake/collect_platform_integrations.py`. ข้อมูลเป็น public catalog capability เท่านั้น ไม่แสดงสถานะบัญชี การติดตั้ง authentication หรือ health ของเจ้าของ

Administrator เป็นผู้ดูแล `systems/ai-data/platform-integration-sources.json`: source ID, owner, fixed public HTTPS URL, parser, platforms, explicit app aliases, `public_catalog_read`, `public`, `archival`, freshness 24 ชั่วโมง โดยไม่อ่าน registry ของ repository อื่นขณะ runtime การเพิ่ม source ต้องตรวจข้อมูลสาธารณะและ mapping ก่อนเพิ่มลง config

รองรับ official Codex/Claude marketplace JSON และ approved remote MCP configuration โดยไม่เรียก endpoint MCP และไม่รัน command/code ที่ดาวน์โหลด ชื่อที่ไม่ตรง alias ไม่สร้าง capability; catalog ที่ไม่ครบไม่พิสูจน์ว่า unavailable Backend เติม unknown สำหรับ platform/app/channel ที่ไม่มีหลักฐาน รวมถึง Claude Web/Desktop native connectors การจับคู่ ecosystem `atlassian`/`atlassian-rovo` กับ Jira/Confluence ไม่ได้แสดงว่ารองรับทุก feature

HTTP จำกัด fixed host `raw.githubusercontent.com`, HTTPS, timeout รวม 15 วินาที, 2 MiB, ไม่ตาม redirect และไม่ส่ง credential; URL ที่มี userinfo/query/fragment/IP ถูกปฏิเสธ ไม่มีการโหลด URL ที่พบภายใน payload การ replay ต้องให้ timestamp UTC เดิม (`YYYY-MM-DDTHH:MM:SSZ`) เพื่อไม่เปลี่ยนอายุหลักฐาน

Download ทำงานใน daemon worker ที่ไม่มีสิทธิ์เขียน lake/publication; caller หยุดเมื่อครบ deadline แม้ HTTP headers ส่งช้ามาก การใช้งานผ่าน CLI จบ process และ daemon เมื่อผิดพลาด หากเรียกเป็น library ใน long-lived process worker ที่ timeout อาจอ่าน public source ต่อจน socket ล้ม/process จบ แต่ไม่มีการเผยแพร่ข้อมูลจาก run ที่ timeout

```bash
.venv/bin/python infra/scripts/data_lake/collect_platform_integrations.py \
  --capture-dir /path/to/public-captures --captured-at 2026-10-13T00:00:00Z \
  --lake-uri /path/to/lake --output /path/to/platform_integrations.snapshot.json

.venv/bin/python infra/scripts/data_lake/collect_platform_integrations.py \
  --fetch --lake-uri /path/to/lake --output /path/to/platform_integrations.snapshot.json
```

Capture filenames คือ `<source-id>.json` เช่น `codex-plugins.json`, `claude-plugins.json`, `github-mcp.json`, `linear-mcp.json`, `notion-mcp.json` ไม่ควรใส่ private MCP configuration ลง capture directory ตัว parser ยอมรับ `headers`/`env` เฉพาะ public variable placeholders เช่น `${VARIABLE}` หรือ `Bearer ${VARIABLE}` โดยไม่ resolve หรือ project; literal values ถูกปฏิเสธก่อนเขียน lake และไม่ project command/args

ลำดับ write: ตรวจทุก source และ preliminary serialized public contract → exact raw bytes ใน landing → normalized source records ใน Bronze Parquet → immutable manifests → normalized `platform_integrations` dataset → ตรวจ raw/Bronze checksum, semantic row agreement และ referenced manifests ทุกตัว → ตรวจ final serialized public contract → atomic replace publication file เก็บ capture timestamp ใน event_time/metadata; `received_at` คือเวลา ingest จริง ไม่ใช่เวลา capture

Source batch identity ผูกกับ source, raw checksum และ transformation fingerprint ที่รวม original capture timestamp, exact source config, platform metadata, normalization revision และ normalized-record digest การเปลี่ยน alias/parser/platform/revision/ผล normalization จึงไม่ใช้ Bronze batch เดิม ส่วน same config+timestamp+raw replay ใช้ identity เดิมได้ `normalization` ใน normalized dataset raw เก็บ revision, public config และ source transform/record checksums ให้ reconstruct ได้ โดยไม่เพิ่ม fields ลง consumer snapshot; source manifests เก็บ `normalization_revision`, `transform_sha256`, `records_sha256` และ normalized manifest เก็บ `config_sha256`

Portable validator `infra/scripts/data_lake/platform_snapshot_contract.py` ตรวจ exact fields/enums, duplicate grains, UTC timestamps, URL/userinfo/control characters, identifiers สูงสุด 80 ตัว, entry IDs สูงสุด 128 ตัว, platform names 1–80 ตัว, manifest keys/hash และขนาด serialized snapshot สูงสุด 4 MiB/50 platforms/20,000 capabilities/100 sources ก่อนแทนไฟล์เดิม URL ที่มี literal `?`/`#` แม้ query/fragment ว่าง, backslash, DEL และ invalid Unicode ถูกปฏิเสธ Validator อยู่ใน repository นี้และไม่มี runtime import จาก consumer repository หรือ hub

Publication ใช้ `schema_version=solo-empire.platform-integrations.v1`, `captured_at`, `platforms[{id,name,mcp_transports,docs_url}]`, `capabilities[{platform_id,app_id,channel,availability,entry_id,endpoint_url,evidence_url,captured_at}]`, `lineage{dataset,manifest_key,manifest_sha256,raw_sha256,bronze_sha256,source_manifests}` โดย transports คือ `stdio`, `sse`, `streamable_http`; manual MCP availability เป็น available เฉพาะเมื่อ source transport อยู่ใน declared host transports มิฉะนั้นเป็น unknown; lineage dataset คือ `platform_integrations`; source manifests เพิ่ม `source_id` Hash เป็น SHA256 lowercase; manifest keys อยู่ใต้ `control/manifests/`

Malformed JSON, duplicate keys/entries/grains, surface restriction ที่ไม่รู้จัก, missing capture, ingest failure และ artifact corruption ทำให้ run ล้มและคง publication เดิมไว้ Raw ที่ถูกปฏิเสธให้เก็บใน caller-owned local capture directory เพื่อ quarantine/review ไม่เขียนลง public lake; fetch ที่ถูกปฏิเสธถูกทิ้ง ไม่มี payload/path/error จาก source ใน CLI output Partial immutable batches ที่เขียนสำเร็จแล้วคงอยู่และ retry ได้

Publication file เป็น public handoff artifact mode `0644` ให้ owning integrations service อ่านได้หลัง root-owned Docker COPY ไม่มี DB write, schedule activation หรือ deployment ใน collector ผู้ดูแลต้องจัด daily refresh และจัดเก็บ capture ต้นฉบับพร้อม timestamp การ retry ต้องใช้ timestamp เดิม; service เป็นผู้ตัดสิน freshness ของข้อมูลและ overlay private owner observations

ทดสอบ offline ด้วย temporary real ObjectStore/Parquet:

```bash
.venv/bin/python infra/tests/data_lake/platform_integration_collection_test.py
```
