# ouster-kafka2db — contributor notes

Editor-facing rules for this service. End-user documentation lives in
`README.md`; this file is for whoever modifies the code.

## What this repo is

The lidar detections sink: `my-topic` (object_list frames from ouster2kafka)
-> `laddms.object_detections` / `laddms.broadway_object_detections`. The whole
service is `ouster2db.py`. It is a Kafka CONSUMER writing to Postgres, so the
`anything2kafka` template (a poll-and-produce service) applies with the
deviations below. `bench_insert.py` is a manual benchmark, not part of the
service.

## Hard rules — these exist because of the 2026-10-04 data loss

1. **A database error is never a bad message.** Only `rows_from_message()`
   failures and `psycopg.DataError` / `psycopg.IntegrityError` may skip a
   message. Every other `psycopg.Error` (OperationalError, ProgrammingError,
   InternalError, ...) must leave the batch in hand, uncommitted, and be
   retried until it lands or the process exits. Do not add a broad
   `except Exception: log; continue` anywhere on the write path: that is
   exactly what discarded 17.5 hours of data.
2. **Offsets are committed only after the Postgres COMMIT**, synchronously,
   in `process_batch()`. Never call `store_offsets()`, never use
   `KafkaConsumer.run()` / `messages()` / `batches()` (run() poisons and
   commits past a batch after two retries; the others store offsets on their
   own schedule), and never turn on `enable.auto.offset.store`.
3. **Do not poll while holding an unwritten batch** except to build it.
   Rebalance callbacks fire inside `poll()`; `_on_revoke()` drops pending
   messages of revoked partitions so the next owner re-reads them.
4. **The two tables, their 41 columns (`COLUMNS`) and the values are a
   contract** with every reader (lv_db_api, lidar_zones webtool, analytics).
   In particular `frame_timestamp_dt` is the KAFKA message timestamp, not the
   frame's, and `intersection_id` is the Kafka key string as-is. Broadway
   routing (`BROADWAY_INTERSECTION_IDS`) is duplicated in lv_db_api and
   lidar_db_replay; change all of them together.
5. **Throughput is a requirement.** The old writer struggled to keep up. Any
   change to the insert path needs an argument (or a measurement with
   `bench_insert.py`) that it is not slower, and a new insert strategy goes
   behind `DB_INSERT_METHOD`, defaulting to the current one.

## Template invariants that still hold

1. Telemetry through `lv_telemetry_connector` (`configure_telemetry` per
   worker process, `tel.get_logger`, `tel.counter`...). No `logging.getLogger`
   for service loggers, no `prometheus_client`.
2. Kafka through `lv_kafka_connector`. No `confluent_kafka` import.
3. Env-var-only config, constants at the top of the file.
4. Graceful shutdown: `_shutdown` + `_sleep_responsively()`; a worker finishes
   its batch on SIGTERM.
5. One file. The `connectors` startup line stays.

## Where this service deviates — and why

- **Raw psycopg on the insert path, not a `Connector` subclass.** lv_db_connector
  1.1 has no executemany or COPY inside a transaction (`TransactionScope` is
  execute-only) and `Connector.insert()` commits per call per table. All SQL
  still lives in one class, `DetectionWriter`; credentials still come from
  `DbEnvCredentials`. `docs/connector_proposals.md` #1-#3 would let this move
  onto the connector.
- **`DetectionsConsumer` overrides `_on_assign` / `_on_revoke`**, underscore
  methods of `KafkaConsumer`, because the connector has no public rebalance
  hook. `_on_assign` receives the raw librdkafka client and must call
  `consumer.assign(partitions)` itself for modified start offsets to apply.
  Proposal #6.
- **Several processes per container** (`WORKERS`, default 3 for the Docker
  host). Each is one consumer-group member with its own connection and its
  own metrics port (base + index), started with `spawn`. On k8s use
  WORKERS=1 and replicas. If any worker dies the parent exits 1.
- **`DB_OUTAGE_EXIT_S` must stay under max.poll.interval.ms (300 s)** minus a
  statement timeout, because a worker does not poll during an outage (no
  public pause/resume; proposal #5). `_check_config()` enforces it.

## Tests

`pytest tests/` with fakes only: a fake psycopg connection with real
transaction/savepoint semantics (`FakeDb`), a fake offset commit sharing its
event log, and a fake raw consumer for the rebalance callbacks. They run the
real `DetectionWriter` and `DetectionsConsumer`. When you fix a failure mode,
add a test that fails without the fix (mutation-check it).
