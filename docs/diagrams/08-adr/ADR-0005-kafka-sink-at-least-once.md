# ADR-0005 — Event plane sink: confluent-kafka, at-least-once, offsets committed after the lake write

- Status: accepted (retro-documented)
- Date: retro-documented 2026-11-04 (`54a5cb2`, ADR-0008 M4, 2026-11-04)

## Context

ADR-0008 (architecture-docs) วาง Aiven Kafka เป็น event plane และกำหนด milestone M4 ให้ event ลง lake Bronze; Kafka เป็น replay buffer ส่วน object storage คือ source of truth

## Decision

- `stream_sink.py` ใช้ confluent-kafka (librdkafka) ผ่าน adapter เล็ก เพราะ kafka-python-ng ยืนยันตัวตนกับ Kafka 4.x ไม่ได้; SASL_SSL เมื่อมี `STREAM_KAFKA_USERNAME`, PLAINTEXT สำหรับ Redpanda ในเครื่อง
- ทุก micro-batch: เขียน Avro, manifest, checkpoint แบบ immutable ก่อน แล้วค่อย commit offset (at-least-once); batch id มาจาก topic/partition/offset จึง retry ได้
- `convert` แปลง Avro เป็น Bronze Parquet (`occurred_at` → `event_time`)
- e2e: `event_heartbeat.py` ส่ง `ops.heartbeat.emitted` แล้วตรวจว่า event นั้น (`--expect-id`) กลับมาถึง lake ในรอบเดียวกัน
- group `lake-sink` เริ่มที่ปลาย topic ณ 2026-11-04 (ข้าม smoke data ของ M3)
- fail เมื่อ group join timeout หรือ commit ต่อ partition ล้ม (`a175820`)

## Consequences

- consumer ของ Bronze stream ต้อง dedupe ตาม envelope id
- SASL user แยก `lake-sink` (consume) กับ `ops-heartbeat` (produce)
- รันชั่วโมงละครั้งผ่าน jobs queue ไม่ใช่ consumer ตลอดเวลา: latency สูงสุดราว 1 ชม.

## Evidence

`infra/scripts/data_lake/stream_sink.py` (docstring), `infra/scripts/data_lake/event_heartbeat.py`, `infra/scripts/data_lake/avro.py`, `.github/workflows/event-sink.yml`, `README.md` (§Event plane), commits `54a5cb2`, `a175820`, `eaa7077`
