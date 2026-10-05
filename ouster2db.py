"""ouster-kafka2db — Ouster lidar object detections, Kafka -> Postgres.

Reads object_list frames from the lidar topic (`my-topic`, produced by
ouster2kafka) and writes one row per detected object into
`laddms.object_detections`, or `laddms.broadway_object_detections` for the
Broadway installs (keys 11-15 by default). Those two tables, their 41 columns
and the values in them are exactly what the pre-rewrite writer produced.

    Run:        python ouster2db.py
    Logs:       JSON on stdout (Loki-friendly)
    Metrics:    /metrics on :9100 (worker i of WORKERS serves :9100+i)
    Tests:      pytest tests/   (fakes only: no database, no Kafka)

Why this was rewritten (2026-10-05)
-----------------------------------
On 2026-10-04 Postgres was crash-restarted. The old writer held one connection
with no reconnect; every insert after that raised "the connection is lost",
which it logged as "Malformed insert." and skipped, while Kafka auto-commit
kept advancing the offsets. 17.5 hours of detections were consumed and thrown
away, and the topic only retains ~1.3 h, so they are gone. Everything below is
arranged so that cannot happen again:

  * A database error is never treated as a bad message. Only a message that
    cannot be parsed, or that Postgres rejects as data (DataError /
    IntegrityError), is skipped, counted and logged with topic/partition/offset.
  * Offsets are committed to Kafka only after the batch's transaction has
    committed in Postgres. Nothing else ever commits or stores an offset.
  * When the database is down the worker keeps the batch in hand, stops
    polling, reconnects with backoff and retries the SAME batch. If it is still
    down after DB_OUTAGE_EXIT_S the process exits non-zero and the container
    restarts; the uncommitted batch is read again from Kafka.

Delivery: at-least-once, and duplicates are possible
---------------------------------------------------
The detection tables have no unique key, so Postgres cannot drop a repeated
row. A crash (or a lost connection) between the Postgres COMMIT and the Kafka
offset commit makes the next owner of that partition read the batch again and
insert it a second time. The window is ONE batch per partition (at most
BATCH_MAX_MESSAGES messages; about 1 s of data at today's rates), because
offsets are committed synchronously after every batch. Data is never lost silently; in that one failure case it is
duplicated.

Per worker, per batch
---------------------
    poll up to BATCH_MAX_MESSAGES messages or BATCH_TIMEOUT_S, whichever first
    parse each message into rows (bad ones -> counted, skipped)
    BEGIN; one INSERT (executemany, or COPY) per table; COMMIT
    commit the Kafka offset of the last message of each partition in the batch

Workers and scaling
-------------------
Each worker is one process and one member of the Kafka consumer group
KAFKA_GROUP_ID; the broker spreads the topic's partitions over the members.
WORKERS (default 3) workers run in this container; on k8s run WORKERS=1 and
scale with replicas. Kafka gives a partition to at most one member, so the
total number of workers across every container/pod should equal the topic's
partition count (3 today). More workers than partitions just idle. To go past
3, the topic needs more partitions first (see README "Scaling").

NEVER run this alongside the old writer (or two copies of it with different
group ids): both would insert every row. The old writer assigned partitions
by hand, so the group-membership check at startup cannot see it.
"""

from __future__ import annotations

import datetime as dt
import json
import multiprocessing
import os
import signal
import sys
import time
from zoneinfo import ZoneInfo

import psycopg
from dotenv import load_dotenv

from lv_db_connector import DbEnvCredentials
from lv_kafka_connector import KafkaConsumer, KafkaEnvCredentials, LvKafkaCommitError
from lv_telemetry_connector import BATCH_SIZE_BUCKETS, configure_telemetry


# =============================================================================
# Configuration — env vars only.
# =============================================================================

load_dotenv()

# Underscores, not hyphens: the service name is also the metric prefix, and a
# hyphen is not legal in a Prometheus metric name.
SERVICE = os.getenv("SERVICE_NAME", "ouster_kafka2db")

# Input. `my-topic` is the real production topic (an early-testing name that
# stuck). ouster2kafka's redesign names it KAFKA_TOPIC_OBJECTS on its side; the
# two must agree, and a wrong name here is silent: the consumer joins the
# group and receives nothing.
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "my-topic")
# The old writer committed to this group too, so keeping the name lets the new
# writer pick up exactly where the old one stopped.
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ouster2db_objects")
# Only used when the group has no committed offset for a partition.
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

# Processes in this container (see module docstring). On k8s: 1, and replicas.
WORKERS = int(os.getenv("WORKERS", "3"))
# Worker i serves /metrics on TELEMETRY_METRICS_PORT + i.
METRICS_PORT_BASE = int(os.getenv("TELEMETRY_METRICS_PORT", "9100"))

# One batch = one Postgres transaction = one offset commit. Same values as the
# old writer (consume_batch(max_messages=300, timeout=1.0)).
BATCH_MAX_MESSAGES = int(os.getenv("BATCH_MAX_MESSAGES", "300"))
BATCH_TIMEOUT_S = float(os.getenv("BATCH_TIMEOUT_S", "1.0"))

# "executemany" (default; what the old writer used) or "copy" (opt-in,
# usually several times faster on the server; see README before switching).
DB_INSERT_METHOD = os.getenv("DB_INSERT_METHOD", "executemany").strip().lower()
# Server-side limit per statement, so a stuck insert ends instead of hanging
# the worker. A normal batch takes well under a second.
DB_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "60000"))
DB_CONNECT_TIMEOUT_S = int(os.getenv("DB_CONNECT_TIMEOUT_S", "10"))
# Give up (exit non-zero) if the database has been unwritable this long.
# While it waits the worker does not poll Kafka, and Kafka drops a member that
# has not polled for max.poll.interval.ms (300 s, librdkafka's default, which
# lv_kafka_connector does not change). So this plus one statement timeout must
# stay under 300 s; startup refuses a combination that does not.
DB_OUTAGE_EXIT_S = float(os.getenv("DB_OUTAGE_EXIT_S", "180"))
KAFKA_MAX_POLL_INTERVAL_S = 300.0

# Restart guard. When a partition is assigned and its committed offset is
# further behind than this, skip to the newest offset instead of spending
# hours catching up (logged loudly with the size of the gap). 0 disables the
# guard: always resume from the committed offset.
LAG_SKIP_AFTER_S = float(os.getenv("LAG_SKIP_AFTER_S", "3600"))

# Kafka message keys (install ids) whose rows go to the Broadway table. The
# default is the routing of the "tee" commit (b2fc309). Compared as strings,
# exactly as the old writer compared the key.
BROADWAY_INTERSECTION_IDS = frozenset(
    part.strip()
    for part in os.getenv("BROADWAY_INTERSECTION_IDS", "11,12,13,14,15").split(",")
    if part.strip()
)

# Written to every row's device_id column; same default as before.
DEVICE_ID = os.getenv("DEVICE_ID", "kafka_objects")

# frame_timestamp_dt is the Kafka message timestamp converted to this zone,
# as before. (The instant is what matters; the zone only shapes the value
# psycopg sends.) `tzdata` is a requirement so this name resolves in the image.
CENTRAL = ZoneInfo("US/Central")
UTC = dt.timezone.utc

TABLE_OBJECTS = "laddms.object_detections"
TABLE_BROADWAY = "laddms.broadway_object_detections"

# Column order of every row tuple built in rows_from_message(). Identical to
# the column list of the old INSERT statements.
COLUMNS = (
    "frame_timestamp", "frame_count", "device_id",
    "object_id", "object_uuid", "update_timestamp", "classification", "sub_classification",
    "length", "width", "height", "position_x", "position_y", "position_z",
    "velocity_x", "velocity_y", "velocity_z",
    "heading", "orient_qw", "orient_qx", "orient_qy", "orient_qz",
    "creation_timestamp", "object_frame_count", "initial_x", "initial_y", "initial_z",
    "primary_sensor", "distance_primary_sensor",
    "num_total_points", "num_primary_points", "num_failed_returns", "class_confidence",
    "uncertain_x", "uncertain_y", "uncertain_z", "uncertain_vx", "uncertain_vy", "uncertain_vz",
    "frame_timestamp_dt", "intersection_id",
)

PROGRESS_LOG_INTERVAL_S = 60.0


# =============================================================================
# Message -> rows.
# =============================================================================


class SkippedMessage(Exception):
    """A message that legitimately carries no detections (not an error)."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class MalformedMessage(Exception):
    """A message that cannot be turned into rows. Skipped and counted."""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


def rows_from_message(msg: dict, broadway_ids=BROADWAY_INTERSECTION_IDS,
                      device_id: str = DEVICE_ID) -> tuple[str, list[tuple]]:
    """Turn one Kafka message (an lv_kafka_connector message dict) into
    (table name, list of row tuples in COLUMNS order).

    The value is an Ouster hub frame, JSON, unchanged by ouster2kafka:
        {"object_list": [{"timestamp": <us>, "frame_count": N, "objects": [...]}]}
    Only object_list[0] is read, as before. The key is the install id and is
    written to intersection_id as-is (a string; Postgres casts it).

    Raises SkippedMessage for a message with no object_list at all (the old
    producer also sent zone configs to this topic) and MalformedMessage for
    anything that cannot be parsed. A frame with zero objects is neither: it
    returns no rows.
    """
    payload = msg.get("value")
    # lv_kafka_connector decodes JSON and falls back to the raw string. A
    # string here is either double-encoded JSON (the old writer accepted it,
    # so this one does) or not JSON at all.
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            raise MalformedMessage("not_json", payload[:80]) from None
    if payload is None:
        raise MalformedMessage("empty_value")
    if not isinstance(payload, dict):
        raise MalformedMessage("not_an_object", type(payload).__name__)
    if "object_list" not in payload or not payload["object_list"]:
        raise SkippedMessage("no_object_list")

    key = msg.get("key")
    table = TABLE_BROADWAY if key in broadway_ids else TABLE_OBJECTS

    try:
        frame = payload["object_list"][0]
        if "timestamp" not in frame or "frame_count" not in frame:
            raise MalformedMessage("missing_frame_fields")
        frame_timestamp = frame["timestamp"]
        frame_count = frame["frame_count"]

        # frame_timestamp_dt is the KAFKA message timestamp (ms), not the
        # frame's own timestamp: that is what the old writer stored. Only if a
        # message has no Kafka timestamp fall back to the frame's (us), as the
        # old insert function did.
        kafka_ts_ms = msg.get("timestamp")
        if kafka_ts_ms is not None and kafka_ts_ms > 0:
            frame_timestamp_dt = dt.datetime.fromtimestamp(kafka_ts_ms / 1000, tz=UTC).astimezone(CENTRAL)
        else:
            frame_timestamp_dt = dt.datetime.fromtimestamp(frame_timestamp / 1000000, tz=UTC).astimezone(CENTRAL)

        rows = []
        for obj in frame["objects"]:
            rows.append((
                frame_timestamp, frame_count, device_id,
                obj["id"], obj["uuid"], obj["update_ts"], obj["classification"], obj["sub_classification"],
                obj["dimensions"]["length"], obj["dimensions"]["width"], obj["dimensions"]["height"],
                obj["position"]["x"], obj["position"]["y"], obj["position"]["z"],
                obj["velocity"]["x"], obj["velocity"]["y"], obj["velocity"]["z"],
                obj["heading"],
                obj["orientation"]["qw"], obj["orientation"]["qx"],
                obj["orientation"]["qy"], obj["orientation"]["qz"],
                obj["creation_ts"], obj["frame_count"],
                obj["initial_position"]["x"], obj["initial_position"]["y"], obj["initial_position"]["z"],
                obj["primary_sensor"], obj["distance_to_primary_sensor"],
                obj["num_points"], obj["num_points_from_primary_sensor"], obj["num_failed_returns"],
                obj["classification_confidence"],
                obj["position_uncertainty"]["x"], obj["position_uncertainty"]["y"],
                obj["position_uncertainty"]["z"],
                obj["velocity_uncertainty"]["x"], obj["velocity_uncertainty"]["y"],
                obj["velocity_uncertainty"]["z"],
                frame_timestamp_dt, key,
            ))
    except KeyError as exc:
        raise MalformedMessage("missing_field", str(exc)) from None
    except (TypeError, IndexError, AttributeError, ValueError, OverflowError) as exc:
        raise MalformedMessage("bad_shape", f"{type(exc).__name__}: {exc}") from None
    return table, rows


# =============================================================================
# Metrics.
# =============================================================================


class Metrics:
    """Every metric this service defines, created once per worker process.

    All are prefixed with SERVICE (default `ouster_kafka2db_`). The freshness
    alert should use last_insert_timestamp_seconds:
        time() - max(ouster_kafka2db_last_insert_timestamp_seconds) > 600
    lv_kafka_connector adds lv_kafka_consumer_lag (messages behind, per
    partition) and lv_kafka_consume_total on its own.
    """

    def __init__(self, tel):
        self.rows_inserted = tel.counter(
            "rows_inserted_total",
            "Detection rows committed to Postgres, by table.",
            labelnames=("table",),
        )
        self.messages_malformed = tel.counter(
            "messages_malformed_total",
            "Kafka messages skipped because they could not be parsed or Postgres rejected "
            "their rows as data, by reason.",
            labelnames=("reason",),
        )
        self.messages_skipped = tel.counter(
            "messages_skipped_total",
            "Kafka messages that carry no detections (e.g. no object_list), by reason. Not errors.",
            labelnames=("reason",),
        )
        self.batch_insert_seconds = tel.histogram(
            "batch_insert_seconds",
            "Wall time of one batch's Postgres transaction, BEGIN through COMMIT, by insert method.",
            labelnames=("method",),
        )
        self.batch_messages = tel.histogram(
            "batch_messages",
            "Kafka messages per batch.",
            buckets=BATCH_SIZE_BUCKETS,
        )
        self.batch_rows = tel.histogram(
            "batch_rows",
            "Detection rows per batch.",
            buckets=(10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000),
        )
        self.consumer_lag_seconds = tel.gauge(
            "consumer_lag_seconds",
            "Age of the newest message in the last committed batch, per partition "
            "(now minus its Kafka timestamp).",
            labelnames=("partition",),
        )
        self.last_insert_timestamp = tel.gauge(
            "last_insert_timestamp_seconds",
            "Unix time of the last batch that committed at least one row. The freshness alert "
            "watches this.",
        )
        self.db_connected = tel.gauge(
            "db_connected",
            "1 while the worker holds a working Postgres connection, 0 after a failure until it "
            "reconnects.",
        )
        self.db_reconnects = tel.counter(
            "db_reconnects_total",
            "Postgres connections re-established after a failure.",
        )
        self.db_write_failures = tel.counter(
            "db_write_failures_total",
            "Batch write attempts that failed for a non-data reason (connection lost, timeout, "
            "permissions...). The batch is retried, never skipped. By exception class.",
            labelnames=("error",),
        )
        self.offset_commit_failures = tel.counter(
            "offset_commit_failures_total",
            "Kafka offset commits that failed AFTER the rows were committed to Postgres. Each one "
            "means that batch will be read and inserted again (duplicates).",
        )
        self.lag_skipped_messages = tel.counter(
            "lag_skipped_messages_total",
            "Messages deliberately skipped on partition assignment because the committed offset "
            "was older than LAG_SKIP_AFTER_S, or had already expired from Kafka. Lost data.",
            labelnames=("partition",),
        )


# =============================================================================
# Postgres.
# =============================================================================


class DetectionWriter:
    """Writes parsed batches into the two detection tables.

    Holds ONE psycopg connection per worker process, opened lazily and
    replaced after any failure.

    Why raw psycopg here and not an lv_db_connector Connector subclass:
    lv_db_connector 1.1 cannot express this hot path without slowing it down.
    Its TransactionScope offers execute() only (one round trip per row, no
    executemany, no COPY), and Connector.insert() borrows a pool connection
    and commits once per call, i.e. once per table, so a batch spanning both
    tables could not be one transaction. The old writer's speed came from
    executemany on one long-lived connection, so that is kept. Credentials
    still come from lv_db_connector (DbEnvCredentials, i.e. DB_HOST, DB_PORT,
    DB_USER, DB_PASSWORD, DB_DBNAME). docs/connector_proposals.md describes the
    connector additions that would let this become a Connector subclass.

    Error contract of write_batch():
      * DataError / IntegrityError (Postgres rejects a row as data): the
        batch is re-run message by message, each in a SAVEPOINT, inside one
        transaction; messages whose rows are rejected are returned, the rest
        commit. Costs nothing unless it happens.
      * Any other psycopg error (connection lost, statement timeout,
        permission denied, read-only...): the transaction is rolled back (or
        died with the connection), the connection is dropped, and the error is
        raised for the caller to retry the same batch. Never skipped.
    """

    def __init__(self, conninfo: dict, *, metrics: Metrics, log, insert_method: str = DB_INSERT_METHOD,
                 statement_timeout_ms: int = DB_STATEMENT_TIMEOUT_MS,
                 connect_timeout_s: int = DB_CONNECT_TIMEOUT_S, application_name: str = SERVICE,
                 table_objects: str = TABLE_OBJECTS, table_broadway: str = TABLE_BROADWAY,
                 connect=psycopg.connect):
        if insert_method not in ("executemany", "copy"):
            raise ValueError(f"DB_INSERT_METHOD must be 'executemany' or 'copy', got {insert_method!r}")
        self.conninfo = dict(conninfo)
        # `docker run --env-file` keeps quotes literally, and the old service's
        # env files quoted the host ('10.80.3.22'); the old code stripped them.
        self.conninfo["host"] = str(self.conninfo["host"]).strip().strip('"').strip("'")
        self.metrics = metrics
        self.log = log
        self.insert_method = insert_method
        self.statement_timeout_ms = statement_timeout_ms
        self.connect_timeout_s = connect_timeout_s
        self.application_name = application_name
        self.connect = connect
        self.conn = None
        self.ever_connected = False
        self.tables = (table_objects, table_broadway)
        cols = ", ".join(COLUMNS)
        placeholders = ", ".join(["%s"] * len(COLUMNS))
        self.insert_sql = {t: f"INSERT INTO {t} ({cols}) VALUES ({placeholders})" for t in self.tables}
        self.copy_sql = {t: f"COPY {t} ({cols}) FROM STDIN" for t in self.tables}

    def open(self) -> None:
        """Connect if not connected. Raises psycopg.OperationalError on failure."""
        if self.conn is not None:
            return
        self.conn = self.connect(
            **self.conninfo,
            # Explicit BEGIN/COMMIT via conn.transaction() only; nothing is
            # ever left idle in a transaction.
            autocommit=True,
            connect_timeout=self.connect_timeout_s,
            application_name=self.application_name,
            # Bound the server side: a stuck statement is cancelled.
            options=f"-c statement_timeout={self.statement_timeout_ms}",
            # Bound the client side: without these a half-open TCP connection
            # (server gone without a RST) blocks a read forever. Keepalives
            # notice an idle dead peer in ~25 s; tcp_user_timeout drops a
            # connection whose sent data goes unacknowledged for 30 s.
            keepalives=1,
            keepalives_idle=10,
            keepalives_interval=5,
            keepalives_count=3,
            tcp_user_timeout=30000,
        )
        if self.ever_connected:
            self.metrics.db_reconnects.inc()
            self.log.info("db_reconnected", extra={"host": self.conninfo["host"]})
        else:
            self.log.info("db_connected", extra={"host": self.conninfo["host"],
                                                 "dbname": self.conninfo.get("dbname")})
        self.ever_connected = True
        self.metrics.db_connected.set(1)

    def close(self) -> None:
        conn, self.conn = self.conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def write_batch(self, parsed: list) -> tuple[dict, list, float]:
        """One attempt to write a batch in ONE transaction.

        parsed: [(msg, table, rows), ...] from rows_from_message().
        Returns ({table: rows committed}, [(msg, exc) rejected by Postgres], seconds).
        """
        rows_by_table = {t: [] for t in self.tables}
        for _msg, table, rows in parsed:
            rows_by_table[table].extend(rows)

        t0 = time.monotonic()
        try:
            self.open()
            try:
                # The hot path. One transaction per batch, one statement stream
                # per table: executemany pipelines every row's INSERT behind a
                # single Sync (psycopg >= 3.1), so the batch costs ~2 round
                # trips plus BEGIN/COMMIT instead of one per message as before.
                with self.conn.transaction():
                    with self.conn.cursor() as cur:
                        for table, rows in rows_by_table.items():
                            if not rows:
                                continue
                            if self.insert_method == "copy":
                                with cur.copy(self.copy_sql[table]) as copy:
                                    for row in rows:
                                        copy.write_row(row)
                            else:
                                cur.executemany(self.insert_sql[table], rows)
                inserted = {t: len(r) for t, r in rows_by_table.items()}
                rejected = []
            except (psycopg.DataError, psycopg.IntegrityError) as exc:
                # One bad row aborted (and rolled back) the whole batch. Find
                # it: redo the batch message by message, each in a SAVEPOINT,
                # still in one transaction. Always executemany here, so a row
                # COPY's stricter text parsing refused still gets its chance.
                self.log.warning("batch_rejected_as_data_isolating_messages",
                                 extra={"error": f"{type(exc).__name__}: {exc}"[:300],
                                        "messages": len(parsed)})
                inserted = {t: 0 for t in self.tables}
                rejected = []
                with self.conn.transaction():
                    with self.conn.cursor() as cur:
                        for msg, table, rows in parsed:
                            if not rows:
                                continue
                            try:
                                with self.conn.transaction():  # SAVEPOINT
                                    cur.executemany(self.insert_sql[table], rows)
                                inserted[table] += len(rows)
                            except (psycopg.DataError, psycopg.IntegrityError) as row_exc:
                                rejected.append((msg, row_exc))
        except psycopg.Error:
            # Not a data problem: connection lost, timeout, permissions, a
            # read-only server... The transaction rolled back (or died with
            # the connection). Drop the connection so the retry reconnects.
            self.close()
            self.metrics.db_connected.set(0)
            raise
        return inserted, rejected, time.monotonic() - t0


# =============================================================================
# Kafka.
# =============================================================================


class DetectionsConsumer(KafkaConsumer):
    """One group member: poll a batch, write it, commit its offsets, repeat.

    Uses lv_kafka_connector's group-managed subscription and only its
    poll() / commit() escape hatches, NOT run(): run()'s contract is "retry a
    failing batch twice, then skip it and commit", which is exactly the
    silent-loss behaviour this service must not have. Offsets are never
    stored for auto-commit (enable.auto.offset.store stays false and
    store_offsets() is never called), so the connector's background
    auto-commit and the commit on close() have nothing to commit; the only
    offset commits are the explicit synchronous ones after a Postgres COMMIT.
    """

    def __init__(self, credentials, *, writer: DetectionWriter, metrics: Metrics, log,
                 group_id: str = KAFKA_GROUP_ID, topics=None, client_id: str = SERVICE,
                 auto_offset_reset: str = KAFKA_AUTO_OFFSET_RESET, persistent: bool = True,
                 batch_max_messages: int = BATCH_MAX_MESSAGES, batch_timeout_s: float = BATCH_TIMEOUT_S,
                 outage_exit_s: float = DB_OUTAGE_EXIT_S, lag_skip_after_s: float = LAG_SKIP_AFTER_S,
                 broadway_ids=BROADWAY_INTERSECTION_IDS, device_id: str = DEVICE_ID):
        # Set before super().__init__(), which subscribes when persistent=True.
        self.writer = writer
        self.metrics = metrics
        self.log = log
        self.batch_max_messages = batch_max_messages
        self.batch_timeout_s = batch_timeout_s
        self.outage_exit_s = outage_exit_s
        self.lag_skip_after_s = lag_skip_after_s
        self.broadway_ids = broadway_ids
        self.device_id = device_id
        # Messages polled but not yet committed. _on_revoke() drops the ones
        # whose partitions are taken away, since they will be re-read by the
        # partition's next owner from the committed offset.
        self.pending = []
        self.progress = self._empty_progress()
        super().__init__(credentials, group_id=group_id, topics=topics, client_id=client_id,
                         auto_offset_reset=auto_offset_reset, persistent=persistent)

    @staticmethod
    def _empty_progress() -> dict:
        return {"batches": 0, "messages": 0, "rows": {TABLE_OBJECTS: 0, TABLE_BROADWAY: 0},
                "malformed": 0, "skipped": 0, "insert_s": 0.0, "max_lag_s": 0.0}

    def run_forever(self) -> None:
        """Poll / write / commit until shutdown. Returns on SIGTERM/SIGINT;
        raises SystemExit(1) if the database stays down past outage_exit_s."""
        last_progress = time.monotonic()
        while not _shutdown:
            # Collect one batch: up to batch_max_messages, or whatever arrived
            # within batch_timeout_s. Rebalance callbacks run inside poll().
            deadline = time.monotonic() + self.batch_timeout_s
            while len(self.pending) < self.batch_max_messages and not _shutdown:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                msg = self.poll(timeout=min(remaining, 0.5))
                if msg is not None:
                    self.pending.append(msg)

            if self.pending:
                if not self.process_batch(self.pending):
                    return  # shutdown arrived while the database was down; nothing committed
                self.pending = []

            if time.monotonic() - last_progress >= PROGRESS_LOG_INTERVAL_S:
                p = self.progress
                self.log.info("progress", extra={
                    "interval_s": round(time.monotonic() - last_progress, 1),
                    "batches": p["batches"], "messages": p["messages"],
                    "rows_object_detections": p["rows"][TABLE_OBJECTS],
                    "rows_broadway_object_detections": p["rows"][TABLE_BROADWAY],
                    "malformed": p["malformed"], "skipped": p["skipped"],
                    "insert_s_total": round(p["insert_s"], 2),
                    "max_lag_s": round(p["max_lag_s"], 1),
                    "assignment": [tp.partition for tp in self.assignment()],
                })
                self.progress = self._empty_progress()
                last_progress = time.monotonic()

    def process_batch(self, batch: list) -> bool:
        """Parse, write (retrying through a database outage), then commit
        offsets. Returns False only if shutdown arrived while the database was
        down, in which case nothing was committed anywhere."""
        # 1. Parse. A bad message costs only itself.
        parsed = []
        malformed = []
        for msg in batch:
            try:
                table, rows = rows_from_message(msg, self.broadway_ids, self.device_id)
            except SkippedMessage as exc:
                self.metrics.messages_skipped.labels(reason=exc.reason).inc()
                self.progress["skipped"] += 1
                continue
            except MalformedMessage as exc:
                malformed.append((msg, exc.reason, str(exc)))
                continue
            parsed.append((msg, table, rows))

        # 2. Write, in one transaction. Through a database outage the batch
        # stays in hand and nothing is polled (so nothing more is consumed)
        # until it lands, the outage limit is reached, or shutdown.
        inserted = {}
        rejected = []
        insert_s = 0.0
        if any(rows for _m, _t, rows in parsed):
            outage_started = None
            attempt = 0
            while True:
                try:
                    inserted, rejected, insert_s = self.writer.write_batch(parsed)
                    break
                except psycopg.Error as exc:
                    attempt += 1
                    now = time.monotonic()
                    self.metrics.db_write_failures.labels(error=type(exc).__name__).inc()
                    first = outage_started is None
                    if first:
                        outage_started = now
                    elapsed = now - outage_started
                    backoff = min(30.0, 2.0 ** (attempt - 1))
                    where = {"attempt": attempt, "outage_s": round(elapsed, 1),
                             "messages_held": len(batch),
                             "partitions": sorted({m["partition"] for m in batch}),
                             "error": f"{type(exc).__name__}: {exc}"[:300]}
                    if elapsed + backoff >= self.outage_exit_s:
                        self.log.critical(
                            "db_unwritable_too_long_exiting",
                            extra={**where, "limit_s": self.outage_exit_s,
                                   "note": "nothing from this batch was committed; it will be "
                                           "re-read from Kafka after the restart"})
                        raise SystemExit(1)
                    if first:
                        self.log.error("db_write_failed_retrying_same_batch",
                                       extra={**where, "retry_in_s": backoff})
                    else:
                        self.log.warning("db_write_failed_retrying_same_batch",
                                         extra={**where, "retry_in_s": backoff})
                    _sleep_responsively(backoff)
                    if _shutdown:
                        self.log.warning("shutdown_while_db_unwritable",
                                         extra={**where, "note": "batch not committed; offsets "
                                                                  "untouched, it will be re-read"})
                        return False
            if outage_started is not None:
                self.log.info("db_write_recovered", extra={
                    "outage_s": round(time.monotonic() - outage_started, 1), "attempts": attempt})

        # 3. Account for what landed and what did not.
        for msg, exc in rejected:
            malformed.append((msg, "db_rejected", f"{type(exc).__name__}: {exc}"[:300]))
        for msg, reason, _detail in malformed:
            self.metrics.messages_malformed.labels(reason=reason).inc()
        if malformed:
            self.progress["malformed"] += len(malformed)
            self.log.warning("malformed_messages_skipped", extra={
                "count": len(malformed),
                "messages": [{"topic": m.get("topic"), "partition": m.get("partition"),
                              "offset": m.get("offset"), "key": m.get("key"),
                              "reason": reason, "detail": detail[:200]}
                             for m, reason, detail in malformed[:20]],
            })
        total_rows = 0
        for table, n in inserted.items():
            if n:
                self.metrics.rows_inserted.labels(table=table).inc(n)
                self.progress["rows"][table] = self.progress["rows"].get(table, 0) + n
                total_rows += n
        if total_rows:
            self.metrics.last_insert_timestamp.set(time.time())
            self.metrics.batch_insert_seconds.labels(method=self.writer.insert_method).observe(insert_s)
        self.metrics.batch_messages.observe(len(batch))
        self.metrics.batch_rows.observe(total_rows)
        self.progress["batches"] += 1
        self.progress["messages"] += len(batch)
        self.progress["insert_s"] += insert_s

        # 4. Only now, with the rows committed in Postgres, commit the Kafka
        # offsets: the last message of each partition in this batch (skipped
        # and malformed messages count as handled). Synchronous, so a crash
        # can replay at most this one batch.
        last_per_partition = {}
        for msg in batch:
            last_per_partition[(msg["topic"], msg["partition"])] = msg
        now = time.time()
        for (_topic, partition), msg in last_per_partition.items():
            try:
                self.commit(message=msg, asynchronous=False)
            except LvKafkaCommitError as exc:
                # The rows are in Postgres; this batch will be read and
                # inserted again by whoever owns the partition next.
                self.metrics.offset_commit_failures.inc()
                self.log.warning("offset_commit_failed_batch_will_be_duplicated", extra={
                    "partition": partition, "offset": msg["offset"], "error": str(exc)[:300]})
            if msg.get("timestamp"):
                lag_s = max(0.0, now - msg["timestamp"] / 1000)
                self.metrics.consumer_lag_seconds.labels(partition=str(partition)).set(lag_s)
                self.progress["max_lag_s"] = max(self.progress["max_lag_s"], lag_s)
        return True

    def _on_assign(self, consumer, partitions) -> None:
        """Rebalance callback (overrides lv_kafka_connector's logging-only one).

        `consumer` is the underlying client librdkafka hands to the callback.
        For each newly assigned partition decide where to start:
          * no committed offset        -> auto.offset.reset (KAFKA_AUTO_OFFSET_RESET)
          * committed offset expired   -> out of retention; that data is gone.
                                          Start at the newest offset if the lag
                                          guard is on, else the oldest still held.
          * committed offset older     -> skip to the newest offset (lag guard)
            than LAG_SKIP_AFTER_S
          * otherwise                  -> resume at the committed offset
        Every skip is logged at ERROR with the number of messages skipped and
        counted in lag_skipped_messages_total. If any lookup fails the guard
        is not applied and the partition resumes from its committed offset.
        """
        decisions = []
        try:
            committed = {tp.partition: tp.offset for tp in consumer.committed(partitions, timeout=10.0)}
            cutoff = {}
            if self.lag_skip_after_s > 0 and partitions:
                cutoff_time = dt.datetime.now(UTC) - dt.timedelta(seconds=self.lag_skip_after_s)
                cutoff = self.offsets_for_times(partitions[0].topic, cutoff_time,
                                                partitions=[tp.partition for tp in partitions])
            for tp in partitions:
                low, high = consumer.get_watermark_offsets(tp, timeout=10.0)
                start = committed.get(tp.partition, -1001)
                decision = {"partition": tp.partition, "committed": start, "low": low, "high": high}
                if start < 0:
                    decision["action"] = f"no_committed_offset_using_{KAFKA_AUTO_OFFSET_RESET}"
                elif start < low:
                    decision["expired_messages"] = low - start
                    tp.offset = high if self.lag_skip_after_s > 0 else low
                    decision["action"] = "expired_skip_to_latest" if self.lag_skip_after_s > 0 else "expired_resume_at_oldest"
                    skipped = tp.offset - start
                    self.metrics.lag_skipped_messages.labels(partition=str(tp.partition)).inc(skipped)
                    self.log.error("committed_offset_expired_data_lost", extra={
                        **decision, "messages_lost": skipped,
                        "note": "the committed offset fell out of Kafka retention before it was read"})
                else:
                    # First offset whose timestamp is within the window; no such
                    # message (-1) means everything left is older than the window.
                    first_recent = cutoff.get(tp.partition, -1) if self.lag_skip_after_s > 0 else None
                    if first_recent is not None and first_recent < 0:
                        first_recent = high
                    if first_recent is not None and start < first_recent and start < high:
                        tp.offset = high
                        decision["action"] = "lag_guard_skip_to_latest"
                        self.metrics.lag_skipped_messages.labels(partition=str(tp.partition)).inc(high - start)
                        self.log.error("lag_guard_skipped_to_latest", extra={
                            **decision, "messages_skipped": high - start,
                            "messages_older_than_limit": first_recent - start,
                            "limit_s": self.lag_skip_after_s,
                            "note": "these messages are still in Kafka until retention removes them, "
                                    "but this writer will not insert them"})
                    else:
                        decision["action"] = "resume_committed"
                        decision["lag_messages"] = high - start
                decisions.append(decision)
        except Exception as exc:
            self.log.warning("lag_guard_failed_resuming_committed", extra={
                "error": f"{type(exc).__name__}: {exc}"[:300]})
            for tp in partitions:
                tp.offset = -1001  # OFFSET_INVALID: start from the committed offset
        # Applying modified start offsets requires assigning inside the callback.
        consumer.assign(partitions)
        self.log.info("partitions_assigned", extra={"partitions": decisions or
                                                    [tp.partition for tp in partitions]})

    def _on_revoke(self, consumer, partitions) -> None:
        """Rebalance callback. Polled-but-uncommitted messages of the revoked
        partitions are dropped, NOT written: their offsets were never
        committed, so the partition's next owner (possibly this worker again)
        reads them from the committed offset. Writing them here instead would
        insert them twice."""
        revoked = {(tp.topic, tp.partition) for tp in partitions}
        before = len(self.pending)
        self.pending = [m for m in self.pending if (m["topic"], m["partition"]) not in revoked]
        self.log.info("partitions_revoked", extra={
            "partitions": sorted(p for _t, p in revoked),
            "uncommitted_messages_dropped_for_reread": before - len(self.pending)})


# =============================================================================
# Lifecycle — graceful shutdown.
# =============================================================================

_shutdown = False


def _on_signal(_signum, _frame) -> None:
    """SIGTERM / SIGINT handler. Flip the flag; the loops notice."""
    global _shutdown
    _shutdown = True


def _connector_versions() -> dict[str, str]:
    """Version of every lv_* connector this process has imported, for the `connectors`
    startup log line.

    The connectors are not version-pinned: an image gets whatever each connector repo's
    default branch held when it was built, so this line is the only way to tell, from a
    running service, which connector code it actually has. It reads each module's own
    `__version__` (what is running), not pip's metadata, which an editable install
    freezes at install time. A version is not a commit: two builds can share one.
    """
    return {
        name: str(getattr(module, "__version__", "unknown"))
        for name, module in sorted(sys.modules.items())
        if name.startswith("lv_") and "." not in name
    }


def _sleep_responsively(seconds: float) -> None:
    """Sleep in small chunks so SIGTERM is responsive.

    Never `time.sleep(N)` directly — k8s SIGTERMs and waits
    `terminationGracePeriodSeconds` (default 30 s) before SIGKILL.
    """
    deadline = time.monotonic() + seconds
    while not _shutdown:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(0.5, remaining))


def _check_config() -> None:
    """Refuse to start on a configuration that would misbehave later."""
    problems = []
    if DB_INSERT_METHOD not in ("executemany", "copy"):
        problems.append(f"DB_INSERT_METHOD={DB_INSERT_METHOD!r}: must be 'executemany' or 'copy'")
    if DB_OUTAGE_EXIT_S + DB_STATEMENT_TIMEOUT_MS / 1000 >= KAFKA_MAX_POLL_INTERVAL_S:
        problems.append(
            f"DB_OUTAGE_EXIT_S ({DB_OUTAGE_EXIT_S:g}) + DB_STATEMENT_TIMEOUT_MS/1000 "
            f"({DB_STATEMENT_TIMEOUT_MS / 1000:g}) must stay under {KAFKA_MAX_POLL_INTERVAL_S:g} s: "
            f"the worker does not poll Kafka while the database is down, and Kafka evicts a "
            f"member that has not polled for max.poll.interval.ms")
    if DB_OUTAGE_EXIT_S <= 0 or DB_STATEMENT_TIMEOUT_MS <= 0:
        problems.append("DB_OUTAGE_EXIT_S and DB_STATEMENT_TIMEOUT_MS must be > 0")
    if WORKERS < 1 or BATCH_MAX_MESSAGES < 1 or BATCH_TIMEOUT_S <= 0:
        problems.append("WORKERS and BATCH_MAX_MESSAGES must be >= 1 and BATCH_TIMEOUT_S > 0")
    if KAFKA_AUTO_OFFSET_RESET not in ("latest", "earliest"):
        problems.append(f"KAFKA_AUTO_OFFSET_RESET={KAFKA_AUTO_OFFSET_RESET!r}: must be 'latest' or 'earliest'")
    if LAG_SKIP_AFTER_S < 0:
        problems.append("LAG_SKIP_AFTER_S must be >= 0 (0 disables the guard)")
    if problems:
        raise RuntimeError("invalid configuration:\n  " + "\n  ".join(problems))


# =============================================================================
# Main.
# =============================================================================


def worker_main(worker_index: int = 0) -> None:
    """One worker process: one Postgres connection, one Kafka group member."""
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    tel = configure_telemetry(service=SERVICE, metrics_port=METRICS_PORT_BASE + worker_index)
    log = tel.get_logger("worker")
    log.info("connectors", extra={"connectors": _connector_versions()})
    log.info("startup", extra={
        "service": SERVICE, "worker": worker_index, "workers": WORKERS,
        "metrics_port": METRICS_PORT_BASE + worker_index,
        "topic": KAFKA_TOPIC, "group_id": KAFKA_GROUP_ID, "auto_offset_reset": KAFKA_AUTO_OFFSET_RESET,
        "batch_max_messages": BATCH_MAX_MESSAGES, "batch_timeout_s": BATCH_TIMEOUT_S,
        "db_insert_method": DB_INSERT_METHOD, "db_statement_timeout_ms": DB_STATEMENT_TIMEOUT_MS,
        "db_outage_exit_s": DB_OUTAGE_EXIT_S, "lag_skip_after_s": LAG_SKIP_AFTER_S,
        "broadway_intersection_ids": sorted(BROADWAY_INTERSECTION_IDS), "device_id": DEVICE_ID,
        "tables": [TABLE_OBJECTS, TABLE_BROADWAY],
    })

    metrics = Metrics(tel)
    metrics.db_connected.set(0)
    writer = DetectionWriter(DbEnvCredentials().resolve(), metrics=metrics, log=log,
                             application_name=f"{SERVICE}-w{worker_index}")

    # Connect to Postgres BEFORE joining the consumer group, so a database
    # that is down at startup never holds partitions (or churns the group's
    # rebalances) while this worker cannot write.
    started = time.monotonic()
    attempt = 0
    while not _shutdown:
        try:
            writer.open()
            break
        except psycopg.Error as exc:
            attempt += 1
            backoff = min(30.0, 2.0 ** (attempt - 1))
            elapsed = time.monotonic() - started
            if elapsed + backoff >= DB_OUTAGE_EXIT_S:
                log.critical("db_unreachable_at_startup_exiting", extra={
                    "attempts": attempt, "elapsed_s": round(elapsed, 1),
                    "error": f"{type(exc).__name__}: {exc}"[:300]})
                raise SystemExit(1)
            log.error("db_unreachable_at_startup_retrying", extra={
                "attempt": attempt, "retry_in_s": backoff, "error": f"{type(exc).__name__}: {exc}"[:300]})
            _sleep_responsively(backoff)
    if _shutdown:
        writer.close()
        return

    consumer = DetectionsConsumer(KafkaEnvCredentials(), writer=writer, metrics=metrics, log=log,
                                  topics=[KAFKA_TOPIC])
    try:
        # Two services sharing a group would split the partitions between them.
        # This sees group MEMBERS only; the old writer assigned partitions by
        # hand and is invisible to it.
        consumer.verify_no_foreign_members()
        consumer.run_forever()
    finally:
        consumer.close(timeout=10.0)
        writer.close()
        log.info("shutdown", extra={"worker": worker_index})


def supervise_workers(n_workers: int, log) -> int:
    """Run n_workers worker processes and stay their parent. Returns the exit code.

    If any worker exits while we are not shutting down, stop all of them and
    exit 1, so the container restarts as a whole (Docker restart policy / k8s)
    and the failure is visible as a restart, rather than limping on with fewer
    workers. On SIGTERM/SIGINT, forward SIGTERM and wait for each worker to
    finish its current batch.

    `spawn`, not fork: each worker builds its own librdkafka and libpq state.
    """
    ctx = multiprocessing.get_context("spawn")
    procs = []
    for i in range(n_workers):
        p = ctx.Process(target=worker_main, args=(i,), name=f"{SERVICE}-w{i}")
        p.start()
        procs.append(p)
    log.info("workers_started", extra={"workers": [(p.name, p.pid) for p in procs]})

    exit_code = 0
    while not _shutdown:
        dead = [p for p in procs if not p.is_alive()]
        if dead:
            log.error("worker_exited_stopping_all", extra={
                "exited": [(p.name, p.exitcode) for p in dead]})
            exit_code = 1
            break
        time.sleep(0.5)

    for p in procs:
        if p.is_alive():
            p.terminate()  # SIGTERM: the worker finishes its batch and leaves the group
    deadline = time.monotonic() + 20.0
    for p in procs:
        p.join(timeout=max(0.0, deadline - time.monotonic()))
    for p in procs:
        if p.is_alive():
            log.warning("worker_killed_after_timeout", extra={"worker": p.name})
            p.kill()
            p.join(timeout=5.0)
    log.info("supervisor_exit", extra={"exit_code": exit_code,
                                       "workers": [(p.name, p.exitcode) for p in procs]})
    return exit_code


def main() -> None:
    _check_config()
    if WORKERS == 1:
        worker_main(0)
        return
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    # The parent only supervises: logs, no metrics port (the workers own
    # :9100, :9101, ...), no Kafka or database clients.
    tel = configure_telemetry(service=SERVICE, metrics=False)
    log = tel.get_logger("supervisor")
    sys.exit(supervise_workers(WORKERS, log))


if __name__ == "__main__":
    main()
