# Connector proposals from the ouster-kafka2db rewrite (2026-10-05)

The rewrite runs on lv_db_connector 1.1 and lv_kafka_connector 1.1 as they
are; none of the changes below is required for it to work. Each section says
what the service does in the meantime. Nothing here has been implemented in
the connector repos.

The common thread: this service is a high-rate Kafka -> Postgres sink with an
at-least-once contract, and both connectors are currently shaped around
low-rate producers. Their gaps pushed the service onto escape hatches (raw
psycopg for the insert path, and overriding two underscore methods of
`KafkaConsumer`). Each proposal is aimed at removing one of those.

---

## lv_db_connector

### 1. Bulk insert inside a transaction (`TransactionScope.insert_many` / `copy_rows`)

**Needed because.** The hot path writes one Kafka batch (up to 300 frames,
~6,600 rows today, into two tables) as ONE transaction, then commits the
Kafka offsets. Today the connector can do either "atomic" or "fast", not both:

- `Connector.transaction()` yields a `TransactionScope` with only `query`,
  `query_one` and `execute`. Inserting 6,600 rows through `execute` is 6,600
  round trips on the LAN, tens of times slower than the pipelined
  `executemany` the old writer used; it would not keep up.
- `Connector.insert(table, rows)` is fast (executemany below 500 rows, COPY at
  or above), but each call borrows a pool connection and commits by itself, so
  a batch spanning both detection tables would be two transactions. A crash
  between them leaves half a batch committed and the whole batch re-read.

**Proposed API.**

```python
with db.transaction() as txn:
    txn.insert_many("laddms.object_detections", rows, columns=COLUMNS)    # executemany
    txn.copy_rows("laddms.broadway_object_detections", rows, columns=COLUMNS)  # COPY
```

Both run on the scope's connection inside the open transaction; `insert_many`
uses `cursor.executemany` (pipelined by psycopg >= 3.1), `copy_rows` uses
`cursor.copy()` + `write_row`, exactly as `Driver.copy_rows` already does but
on the transaction's cursor. Also useful: `txn.savepoint()` (a nested
`conn.transaction()`), which this service uses to isolate a row Postgres
rejects as data without losing the rest of the batch.

Keep the COPY choice EXPLICIT here rather than threshold-driven
(`COPY_THRESHOLD = 500` in `_bulk.py`): COPY's text parsing is stricter than
executemany's typed parameters (a float `12.0` into an integer column, a
timestamptz into a `timestamp` column), so switching method changes what is
accepted and, for `timestamp without time zone`, what is stored.

**Meanwhile.** `DetectionWriter` in `ouster2db.py` holds one psycopg
connection itself, credentials still resolved through `DbEnvCredentials`.

### 2. Connection keepalives and timeouts as first-class options

**Needed because.** The old writer's connection died with the server; a
connection whose server vanishes WITHOUT a TCP reset (network partition, host
power loss) is worse: a blocking read waits forever and nothing errors. The
pool opens connections from `make_conninfo(**credentials.resolve())` with no
way to add libpq options except smuggling them through a custom
`DbCredentials`.

**Proposed API.** `Connector(..., keepalives=True, tcp_user_timeout_ms=30000,
statement_timeout_ms=None, application_name=None)`, applied to every pooled
connection (libpq `keepalives=1`, `keepalives_idle=10`, `keepalives_interval=5`,
`keepalives_count=3`, `tcp_user_timeout`, `options=-c statement_timeout=...`,
`application_name`). Sensible defaults ON for keepalives and
`tcp_user_timeout`, because the hang is never what anyone wants.
`application_name` defaulting to the service name would also have made the
2026-10-04 incident much faster to diagnose from `pg_stat_activity`.

**Meanwhile.** `DetectionWriter.open()` passes all of these to
`psycopg.connect` directly.

### 3. Explicit reconnect / health semantics

**Needed because.** `Connector._run()` retries a connection error once and
then raises, and `health_check()` runs `SELECT 1` on a pooled connection. A
sink needs to tell three things apart, and today it has to infer them from
exception types:

1. the database is unreachable or the connection died
   (`LvDbConnectionError`): retry the same batch later, never skip it;
2. the server refused the data (`DataError` / `IntegrityError`, class 22/23):
   skip that message;
3. everything else (`permission denied`, read-only server, undefined table...):
   not data, so not skippable; retry and eventually fail loudly.

Today `Driver.cursor()` maps every non-operational `psycopg.Error` to
`LvDbQueryError`, which merges cases 2 and 3. A caller that treats
`LvDbQueryError` as "bad message, skip it" reproduces the 2026-10-04 data loss
the first time a GRANT is revoked or the server goes read-only.

**Proposed API.** Split `LvDbQueryError` with a new subclass
`LvDbDataError(LvDbQueryError)` raised for SQLSTATE classes 22 and 23 only (it
stays a `LvDbQueryError`, so existing callers are unaffected), and document
that only `LvDbDataError` is safe to treat as "skip this record". Add
`Connector.reset()` to discard the pool's connections after an outage, so the
next call reconnects instead of failing once more on a stale one.

**Meanwhile.** The writer catches `psycopg.DataError` / `psycopg.IntegrityError`
for case 2 and treats every other `psycopg.Error` as case 3/1 (retry, then
exit non-zero).

---

## lv_kafka_connector

### 4. A manual-commit batch loop: `KafkaConsumer.run(commit="after_batch", on_failure="block")`

**Needed because.** `run()` is the documented path, and its contract is
"`process_batch` raised: retry twice, then `on_poison()` and commit past it".
For a database sink that is exactly the silent-loss behaviour this rewrite
exists to remove: three failed attempts during a 2-minute Postgres restart
and the batch is gone. `batches()` stores offsets when the loop body
returns, so a body that breaks out after an un-written batch must know not to
return normally.

**Proposed API.** Options on `run()`:

- `on_failure="block"`: keep retrying the SAME batch with capped exponential
  backoff instead of poisoning it, and stop fetching while doing so;
  `max_block_s=` then raise (so the process exits and restarts).
- `commit="after_batch"`: a synchronous `commit()` of the batch's last offset
  per partition right after `process_batch` returns, instead of
  `store_offsets()` + the 5 s auto-commit. That keeps the duplicate window
  after a crash at one batch instead of up to 5 s of batches.

**Meanwhile.** `DetectionsConsumer.run_forever()` builds batches with the
public `poll()`, retries the write itself, and calls the public
`commit(message, asynchronous=False)` per partition. It never calls
`store_offsets()`, so the connector's auto-commit and the commit on `close()`
have nothing to commit.

### 5. Public pause / resume

**Needed because.** While the database is down the worker holds its batch and
stops polling. librdkafka evicts a member that has not polled for
`max.poll.interval.ms` (300 s), so the service caps an in-process outage at
`DB_OUTAGE_EXIT_S` (180 s) and then exits and restarts. With `pause()`, the
worker could keep polling (serving heartbeats and rebalances, fetching
nothing) through an outage of any length, without restart churn.

**Proposed API.** `KafkaConsumer.pause(partitions=None)` /
`resume(partitions=None)` (None = current assignment), mapping to
`Consumer.pause/resume`; plus a note that a rebalance resets pause state, so
`on_assign` must re-pause if the caller is still paused.

**Meanwhile.** Stop polling and exit before the 300 s limit (validated at
startup).

### 6. Public rebalance hooks and offset introspection

**Needed because.** The lag guard ("if the committed offset is older than
`LAG_SKIP_AFTER_S`, start at the newest") and the "drop un-committed messages
of revoked partitions" rule both live in rebalance callbacks. The connector
binds `self._on_assign` / `self._on_revoke` at `subscribe()` and offers no
public hook, so the service overrides the underscore methods. It also needs
committed offsets and watermarks, which are only reachable through the raw
client librdkafka passes to the callback.

**Proposed API.**

- Overridable `on_assign(self, partitions)` / `on_revoke(self, partitions)` /
  `on_lost(self, partitions)` hooks (`on_lost` is not wired at all today;
  confluent-kafka then calls `on_revoke` for lost partitions).
- A way to choose start offsets from `on_assign`, e.g. returning
  `{partition: offset}`; the connector then calls `assign()` inside the
  callback, which librdkafka requires for modified offsets to take effect.
- `committed(partitions)` and `watermarks(topic, partition)` on
  `KafkaConsumer` (both exist on `ConsumerDriver` or the raw client only).
- Optionally a built-in `max_start_lag=timedelta(...)` subscribe option doing
  what the service's `_on_assign` does, since every sink that can fall behind
  a 1.3 h retention topic needs the same guard.

**Meanwhile.** `DetectionsConsumer._on_assign(consumer, partitions)` uses the
raw client librdkafka hands the callback (`committed`,
`get_watermark_offsets`, `assign`) and the public `offsets_for_times()`.

### 7. Expose `max.poll.interval.ms`

**Needed because.** Items 4 and 5 interact with it, and the service has to
hardcode the librdkafka default (300 s) to validate `DB_OUTAGE_EXIT_S`.

**Proposed API.** Either pin it in `_CONSUMER_DEFAULTS` (so the value is a
known contract, not a library default that could change) and export it as a
constant, or make it a constructor option for consumers that block.

**Meanwhile.** `KAFKA_MAX_POLL_INTERVAL_S = 300.0` in `ouster2db.py`.
