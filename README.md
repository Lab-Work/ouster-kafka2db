# ouster-kafka2db

Ouster lidar object detections, Kafka -> Postgres. Reads object_list frames
from `my-topic` (produced by `ouster2kafka`) and writes one row per detected
object into **`laddms.object_detections`**, or
**`laddms.broadway_object_detections`** for the Broadway installs (Kafka keys
11-15). Those two tables, their columns and the values written are unchanged
from the pre-rewrite writer.

The whole service is `ouster2db.py`. Contributor rules are in `CLAUDE.md`.

## Why it was rewritten

On 2026-10-04 at 14:48 CDT Postgres was crash-restarted. The old writer held
one connection with no reconnect, logged every failed insert as
`Malformed insert.`, kept consuming, and Kafka auto-commit advanced the
offsets. About 17.5 hours of detections were consumed and discarded; the
topic keeps only ~1.3 h, so they could not be replayed. The rewrite makes a
database failure stop the writer (loudly) instead of eating data.

## What happens when...

| Situation | Behaviour |
|---|---|
| Normal batch | Poll up to `BATCH_MAX_MESSAGES` (300) or `BATCH_TIMEOUT_S` (1 s); `BEGIN`; one INSERT stream per table; `COMMIT`; then a synchronous Kafka offset commit for the last message of each partition. |
| A message cannot be parsed | That message only is skipped: `ouster_kafka2db_messages_malformed_total{reason}` + one `malformed_messages_skipped` warning per batch listing topic/partition/offset. The rest of the batch commits. |
| Postgres rejects a row as data (`DataError`/`IntegrityError`) | The batch is re-run message by message, each in a SAVEPOINT, in one transaction; only the rejected messages are skipped (`reason="db_rejected"`). |
| Connection lost, server restarting, statement timeout, permission denied, read-only server... | Never skipped. The transaction rolls back, offsets are NOT committed, the worker stops polling, reconnects with backoff (1, 2, 4 ... 30 s) and retries the **same** batch. `db_write_failures_total{error}`, `db_connected`=0, `db_reconnects_total`. |
| Database still unwritable after `DB_OUTAGE_EXIT_S` (180 s) | `db_unwritable_too_long_exiting` (CRITICAL), exit code 1. The container restarts (needs a restart policy, see Deployment) and re-reads the uncommitted batch from Kafka. |
| Database down at startup | The worker retries the connection before it joins the consumer group, and exits 1 after `DB_OUTAGE_EXIT_S`. |
| Message with no `object_list` (e.g. the old producer's zone configs) | Skipped, `messages_skipped_total{reason="no_object_list"}`. Not an error. |
| SIGTERM / SIGINT | Each worker finishes the batch in hand (write + commit), leaves the group, exits 0. During a database outage it exits without committing. |
| A worker process dies (WORKERS > 1) | The parent stops the others and exits 1, so the container restarts as a whole. |
| Restart after a long gap | See "Restart behaviour". |

### Delivery semantics: at-least-once, duplicates possible

Offsets are committed only after the batch's Postgres transaction commits.
The detection tables have **no unique key**, so if the process dies (or the
offset commit fails) between the Postgres `COMMIT` and the Kafka offset
commit, the next owner of that partition reads the batch again and **inserts
it twice**. The window is one batch per partition (at most 300 messages,
about 1 s of data at today's rates) because the offset commit is synchronous
after every batch. A failed offset commit is counted in
`offset_commit_failures_total`; each one means one duplicated batch. A lost
connection during `COMMIT` itself is ambiguous (the server may have
committed); the batch is retried, so that case can also duplicate one batch.

## Restart behaviour (the lag guard)

The old writer started every partition at the newest offset
(`force_latest=True`), so every restart silently skipped whatever arrived
while it was down. Now each partition resumes from the consumer group's
committed offset, with a guard so a restart after a long outage does not
spend hours catching up. On every partition assignment:

| Committed offset | Start at | Logged |
|---|---|---|
| none (new group) | `KAFKA_AUTO_OFFSET_RESET` (latest) | info |
| within `LAG_SKIP_AFTER_S` (3600 s) | committed offset | info, with lag in messages |
| older than `LAG_SKIP_AFTER_S` | newest offset | **ERROR `lag_guard_skipped_to_latest`** with messages skipped and how many were older than the limit; `lag_skipped_messages_total{partition}` |
| expired from Kafka retention | newest (guard on) or oldest still held (guard off) | **ERROR `committed_offset_expired_data_lost`** with the count |

`LAG_SKIP_AFTER_S=0` disables the guard. "Older than the limit" is decided
with `offsets_for_times(now - LAG_SKIP_AFTER_S)`: if the committed offset is
before the first message inside the window, the partition skips. If any of
these lookups fails, the partition resumes from its committed offset.

## Configuration

Env vars only (see `example.env` for a commented copy). Credentials come
from the lv_* connectors' standard names.

| Var | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP`, `KAFKA_USER`, `KAFKA_PASSWORD` | required | Broker and SASL credentials (lv_kafka_connector). |
| `KAFKA_CA_LOCATION` / `KAFKA_CA_CERT` | `strimzi-ca.crt` / none | CA file, or inline PEM (wins). |
| `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM` | `SASL_SSL`, `SCRAM-SHA-512` | |
| `KAFKA_TOPIC` | `my-topic` | Input topic. The real production name. |
| `KAFKA_GROUP_ID` | `ouster2db_objects` | Consumer group; the old writer's group, so the new one resumes where it stopped. |
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | Only for partitions with no committed offset. |
| `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_DBNAME` | required (`DB_DBNAME=NDOT`) | Postgres (lv_db_connector `DbEnvCredentials`). |
| `DB_INSERT_METHOD` | `executemany` | `executemany` or `copy` (opt-in; see below). |
| `DB_STATEMENT_TIMEOUT_MS` | `60000` | Server-side limit per statement. |
| `DB_CONNECT_TIMEOUT_S` | `10` | |
| `DB_OUTAGE_EXIT_S` | `180` | Exit non-zero after the DB has been unwritable this long. With the statement timeout it must stay under 300 s (Kafka `max.poll.interval.ms`); startup refuses otherwise. |
| `WORKERS` | `3` | Worker processes in this container (each one group member). k8s: 1. |
| `BATCH_MAX_MESSAGES`, `BATCH_TIMEOUT_S` | `300`, `1.0` | One batch = one transaction = one offset commit. |
| `LAG_SKIP_AFTER_S` | `3600` | Restart guard; 0 disables. |
| `BROADWAY_INTERSECTION_IDS` | `11,12,13,14,15` | Keys routed to `broadway_object_detections`, compared as strings. |
| `DEVICE_ID` | `kafka_objects` | Written to `device_id`. |
| `SERVICE_NAME` | `ouster_kafka2db` | Log service field and metric prefix (underscores only). |
| `TELEMETRY_METRICS_PORT` | `9100` | Base port; worker i serves base + i. |
| `TELEMETRY_*` | | Log level, profiling, tracing: see `example.env`. |

Renamed from the old service: `SQL_HOSTNAME` -> `DB_HOST`, `SQL_PORT` ->
`DB_PORT`, `SQL_USERNAME` -> `DB_USER`, `SQL_PASSWORD` -> `DB_PASSWORD`; the
database name was hardcoded `NDOT` and is now `DB_DBNAME`. Removed:
`LOG_PATH`, `DATA_LOG_FILE_DIR`, `DATA_LOG_FILE`, `DATA_LOG_BACKUPS` (the
file-bypass logger is gone; logs go to stdout) and
`KAFKA_MAX_START_DELAY_SECONDS` (read but never acted on). The old code
defaulted `KAFKA_TOPIC` to `test`; it now defaults to `my-topic`.

## Metrics and alerts

All prefixed `ouster_kafka2db_` (from `SERVICE_NAME`):

| Metric | Type | Use |
|---|---|---|
| `last_insert_timestamp_seconds` | gauge | **Freshness alert:** `time() - max(ouster_kafka2db_last_insert_timestamp_seconds) > 600`. This is the alert that would have caught 2026-10-04 within minutes. |
| `rows_inserted_total{table}` | counter | Write rate per table. |
| `batch_insert_seconds{method}` | histogram | BEGIN..COMMIT time per batch. Saturation shows as this approaching `BATCH_TIMEOUT_S`. |
| `batch_messages`, `batch_rows` | histogram | Batch size. Batches pinned at 300 messages = catching up. |
| `consumer_lag_seconds{partition}` | gauge | Age of the newest committed message. |
| `messages_malformed_total{reason}` | counter | Alert on a sustained rate (an upstream format change would show here). |
| `messages_skipped_total{reason}` | counter | Expected, not an error. |
| `db_connected`, `db_reconnects_total`, `db_write_failures_total{error}` | | Database health as this writer sees it. |
| `offset_commit_failures_total` | counter | Each = one duplicated batch. |
| `lag_skipped_messages_total{partition}` | counter | Data deliberately skipped by the restart guard (lost). |

`lv_kafka_connector` adds `lv_kafka_consumer_lag{partition}` (messages behind
the high watermark, from librdkafka stats) and `lv_kafka_consume_total`.
Logs: one `progress` line per worker per minute (rows per table, batches,
malformed, max lag); the old per-100-message INFO lines are gone.

With `WORKERS=3` in one container the three workers serve :9100, :9101 and
:9102. A single k8s scrape annotation covers only one port, which is why the
k8s manifest runs `WORKERS=1` per pod.

## Throughput

### What the old hot path actually did

`db_laddms.make_sql_connection()` defaulted to `autocommit=True`, so the old
writer was **not** one transaction per batch: each message's `executemany`
(~22 rows) ran as its own implicit transaction, and the `commit()` after the
batch was a no-op. Per worker that is one commit (one WAL flush) and one
pipeline sync per message: ~44 commits/s/worker, ~130/s in total today. It
also explains why a failure mid-batch left a partial batch committed.

### What the new hot path does

Per batch: `BEGIN`; one `executemany` per table (psycopg >= 3.1 pipelines all
of a call's rows behind one Sync, with a prepared statement); `COMMIT`; one
synchronous offset commit per partition. The server executes the same
prepared INSERT once per row as before, so per-row server work is unchanged;
what drops is ~44 commits and ~44 sync round trips per second per worker,
replaced by one of each. There is no mechanism by which this is slower: the
same rows, the same statement, the same connection-per-process, fewer commits.
The added cost is the Kafka offset commit: one round trip per partition per
batch (milliseconds, about once a second at today's rate). Per-message Python
work (JSON decode in the connector, building 41-value tuples) is the same as
before; `poll()` per message instead of `consume(300)` costs microseconds per
message.

Not measured: no database or broker was touched while writing this. To
confirm in production, compare `batch_insert_seconds` / `batch_rows` (seconds
per row) against the old writer's "insert took" log lines, and watch
`consumer_lag_seconds` stay near 1 s.

### COPY vs executemany (`DB_INSERT_METHOD=copy`, opt-in)

`COPY ... FROM STDIN` streams rows without per-row protocol messages or
per-row executor startup, and TimescaleDB batches COPY into chunks. It is
commonly several times faster than executemany for bulk inserts. Expect the
gain mostly in server CPU per row, i.e. in `batch_insert_seconds`. Two
differences to check before switching:

1. **Stricter parsing.** COPY parses text; executemany sends typed parameters
   that Postgres casts. A JSON float like `12.0` into an integer column is
   accepted by executemany but rejected by COPY. A rejected batch falls back
   to message-by-message executemany, so nothing is lost, but if it happens
   systematically COPY would be slower, not faster (watch
   `batch_rejected_as_data_isolating_messages` warnings).
2. **`frame_timestamp_dt`'s column type.** If it is `timestamptz`, both
   methods store the same instant. If it is `timestamp` (without time zone),
   COPY stores the US/Central wall-clock time while executemany converts to
   the session `TimeZone`; they agree only if that is US/Central. Check with
   `\d laddms.object_detections` first.

Benchmark on scratch tables, never the live ones:

```sql
-- as a role that may create tables (THIS WRITES TO THE DATABASE)
CREATE SCHEMA bench;
CREATE TABLE bench.object_detections (LIKE laddms.object_detections INCLUDING DEFAULTS INCLUDING INDEXES);
CREATE TABLE bench.broadway_object_detections (LIKE laddms.broadway_object_detections INCLUDING DEFAULTS INCLUDING INDEXES);
SELECT create_hypertable('bench.object_detections', 'frame_timestamp_dt', chunk_time_interval => interval '1 day');
SELECT create_hypertable('bench.broadway_object_detections', 'frame_timestamp_dt', chunk_time_interval => interval '1 day');
GRANT INSERT ON bench.object_detections, bench.broadway_object_detections TO <writer role>;
```

```
python bench_insert.py --objects-table bench.object_detections \
    --broadway-table bench.broadway_object_detections --rounds 20
```

It runs the service's own `DetectionWriter` with both methods, alternating
round by round, on 300-message x 22-object batches, and prints median/p95
batch time and rows/s per worker. Run it while production is writing (that
is the load COPY has to beat), then `DROP SCHEMA bench CASCADE`. Switch only
if COPY is clearly faster and the two checks above pass; then compare
`batch_insert_seconds{method="copy"}` against the executemany baseline in
production.

## Scaling (towards ~20k rows/s)

Today: ~3,300 rows/s (peak maybe 4-5k), 3 partitions, 3 workers, so ~1,100
rows/s per worker. Dozens more installs means ~15-25k rows/s, i.e. ~7k rows/s
per worker if the topic stays at 3 partitions. Do not assume one connection
can do that. The path:

1. **More partitions on `my-topic`.** Consumer parallelism is capped at the
   partition count: a partition goes to one group member. This is a topic
   change (`kafka-topics --alter --partitions N`, or the Strimzi KafkaTopic),
   coordinated with the producer's owner (ouster2kafka). The producer keys by
   install id, so no producer code changes, but adding partitions remaps keys
   to partitions: per-install ordering is broken once at the moment of the
   change, which matters to the analytics consumers (lidar_zones,
   crowd_counts) more than to this writer. Size for the install count, e.g.
   12-24, so a hot install does not dominate a partition.
2. **More workers, same group.** This writer is already group-managed:
   raise k8s `replicas` (WORKERS=1 each) or `WORKERS` up to the partition
   count. Partitions are rebalanced automatically; nothing else changes.
3. **The insert method.** Benchmark COPY (above). If it wins, it buys the
   most per worker.
4. **The database.** Each worker is one connection inserting into an
   uncompressed hypertable with a `frame_timestamp_dt DESC` index;
   `max_wal_size` (1 GB, per the 2026-10-04 audit) forces very frequent
   checkpoints at this write volume. Those are DB-owner decisions, not code.

`batch_insert_seconds` approaching `BATCH_TIMEOUT_S` while batches sit at
300 messages means a worker is saturated: add partitions + workers.

## Deployment

**Never run two writers against the same data**: not the old and the new
writer, and not two copies of the new one with different `KAFKA_GROUP_ID`s.
Every row would be inserted twice and nothing de-duplicates. The old writer
assigned partitions by hand, so it is not a group member and the new
writer's startup group check cannot detect it.

Cut-over: `docker stop` the old container (and disable its restart policy),
then start the new writer with the same `KAFKA_GROUP_ID` (`ouster2db_objects`,
the default), on the Docker host or k8s but not both. It resumes from the old
writer's last committed offsets.

- **Docker host:** run with a restart policy (`--restart unless-stopped`):
  exiting non-zero is how the writer escalates a long database outage, and
  without a policy it would simply stay down.
  ```
  docker run -d --restart unless-stopped --env-file .env \
    -v $(pwd)/strimzi-ca.crt:/app/strimzi-ca.crt:ro \
    docker.mogi.io/ouster-kafka2db:<sha>
  ```
  `WORKERS=3` (default) gives one worker per partition in one container.
  `docker stop` waits 10 s by default, enough for a worker to finish its batch.
- **k8s:** `k8s-manifests-backend/2kafka/ouster-kafka2db/deployment.yaml`
  (WORKERS=1, replicas = partition count; committed at `replicas: 0` so a
  Drone image bump cannot start it before the old writer is stopped).

### Build

Drone (`.drone.yml`) clones the three lv_* connectors into the build context,
builds `docker.mogi.io/ouster-kafka2db:<commit-sha>` and bumps
`2kafka/ouster-kafka2db/deployment.yaml` in `k8s-manifests-backend`. Its
trigger block is commented out (fleet convention), so ANY branch push builds
and bumps. `.dockerignore` excludes `**/.git` so the clone step's tokenised
`.git/config` never reaches the image. A hand build needs the three
connector checkouts copied into the context first (`cp -R ../lv_* .`, then
remove their `.git` directories) and `--platform linux/amd64` on an arm64 Mac.

## Development

```
pip install -e ../lv_telemetry_connector -e ../lv_kafka_connector -e ../lv_db_connector
pip install -r <(grep -v '^\./' requirements.txt) pytest
pytest tests/        # fakes only: no database, no Kafka
```

`tests/conftest.py` disables `load_dotenv()` so a local `.env` with real
credentials is never read by the tests. Do not run `python ouster2db.py` from
a checkout whose `.env` points at production unless you mean to write to it.
