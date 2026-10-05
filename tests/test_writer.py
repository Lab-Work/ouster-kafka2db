"""The writer must never lose data silently on a database failure.

On 2026-10-04 a Postgres crash-restart dropped the old writer's only
connection; it logged every failed insert as "Malformed insert.", kept
consuming, and Kafka auto-commit threw away 17.5 hours of detections. These
tests pin the replacement behaviour with fakes only (no database, no Kafka):
the real DetectionWriter and DetectionsConsumer run against a fake psycopg
connection and a fake offset commit.
"""

from __future__ import annotations

import datetime as dt
import logging
from contextlib import contextmanager

import psycopg
import pytest

from lv_kafka_connector import KafkaExplicitCredentials, LvKafkaCommitError

import ouster2db as svc


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeMetric:
    """Counter / gauge / histogram stand-in. Records per label set."""

    def __init__(self):
        self.values = {}

    def labels(self, **labels):
        key = tuple(sorted(labels.items()))
        metric = self

        class _Child:
            def inc(self, amount=1):
                metric.values[key] = metric.values.get(key, 0) + amount

            def set(self, value):
                metric.values[key] = value

            def observe(self, value):
                metric.values.setdefault(key, []).append(value)

        return _Child()

    def inc(self, amount=1):
        self.labels().inc(amount)

    def set(self, value):
        self.labels().set(value)

    def observe(self, value):
        self.labels().observe(value)

    def get(self, **labels):
        return self.values.get(tuple(sorted(labels.items())))


class FakeTel:
    def counter(self, name, documentation, *, labelnames=()):
        return FakeMetric()

    def gauge(self, name, documentation, *, labelnames=()):
        return FakeMetric()

    def histogram(self, name, documentation, *, labelnames=(), buckets=None):
        return FakeMetric()


class FakeDb:
    """A Postgres stand-in with real transaction / savepoint semantics.

    `events` is shared with the fake Kafka commit, so tests can assert the
    order of database commits and offset commits.

    fail_on_statement: {n: exception} raises on the n-th INSERT/COPY statement
        (1-based, counted across connections) - e.g. a connection loss.
    reject_classification: rows with this classification raise DataError, as
        Postgres would for a value it cannot store.
    """

    def __init__(self, events):
        self.events = events
        self.committed = {}  # table -> [row, ...]
        self.connects = 0
        self.connect_kwargs = []
        self.statements = 0
        self.fail_on_statement = {}
        self.fail_connects = 0
        self.reject_classification = None

    def connect(self, **kwargs):
        self.connect_kwargs.append(kwargs)
        if self.fail_connects:
            self.fail_connects -= 1
            raise psycopg.OperationalError("connection refused")
        self.connects += 1
        return FakeConn(self)

    def rows(self, table):
        return self.committed.get(table, [])


class FakeConn:
    def __init__(self, db):
        self.db = db
        self.closed = False
        self.stack = []  # one list of staged (table, row) per open transaction/savepoint

    @contextmanager
    def transaction(self):
        if self.closed:
            raise psycopg.OperationalError("the connection is closed")
        depth = len(self.stack)
        self.db.events.append(("begin",) if depth == 0 else ("savepoint",))
        self.stack.append([])
        try:
            yield None
        except BaseException:
            self.stack.pop()
            self.db.events.append(("rollback",) if depth == 0 else ("rollback_to_savepoint",))
            raise
        frame = self.stack.pop()
        if self.stack:
            self.stack[-1].extend(frame)
            self.db.events.append(("release_savepoint",))
        else:
            for table, row in frame:
                self.db.committed.setdefault(table, []).append(row)
            self.db.events.append(("db_commit", len(frame)))

    @contextmanager
    def cursor(self):
        yield FakeCursor(self)

    def close(self):
        self.closed = True


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def _statement(self, sql, rows):
        db = self.conn.db
        if not self.conn.stack:
            raise AssertionError("insert outside a transaction")
        db.statements += 1
        exc = db.fail_on_statement.pop(db.statements, None)
        if exc is not None:
            if isinstance(exc, psycopg.OperationalError):
                self.conn.closed = True  # a lost connection stays lost
            raise exc
        table = sql.split()[1] if sql.startswith("COPY ") else sql.split()[2]
        for row in rows:
            if db.reject_classification is not None and row[6] == db.reject_classification:
                raise psycopg.errors.InvalidTextRepresentation("invalid input syntax")
        self.conn.stack[-1].extend((table, row) for row in rows)

    def executemany(self, sql, rows):
        assert sql.startswith("INSERT INTO ")
        self._statement(sql, list(rows))

    @contextmanager
    def copy(self, sql):
        assert sql.startswith("COPY ")
        written = []

        class _Copy:
            def write_row(self, row):
                written.append(row)

        yield _Copy()
        self._statement(sql, written)


def make_object(i, classification="CAR"):
    xyz = {"x": 1.0 + i, "y": 2.0, "z": 0.5}
    return {
        "id": 1000 + i, "uuid": f"00000000-0000-0000-0000-{i:012d}", "update_ts": 1759670400000000,
        "classification": classification, "sub_classification": "NONE",
        "dimensions": {"length": 4.5, "width": 1.8, "height": 1.5},
        "position": dict(xyz), "velocity": dict(xyz), "heading": 0.25,
        "orientation": {"qw": 1.0, "qx": 0.0, "qy": 0.0, "qz": 0.0},
        "creation_ts": 1759670300000000, "frame_count": 77, "initial_position": dict(xyz),
        "primary_sensor": "sensor-a", "distance_to_primary_sensor": 12.5,
        "num_points": 300, "num_points_from_primary_sensor": 250, "num_failed_returns": 3,
        "classification_confidence": 0.9,
        "position_uncertainty": dict(xyz), "velocity_uncertainty": dict(xyz),
    }


KAFKA_TS_MS = 1759670400123  # 2025-10-05 13:20:00.123 UTC


def make_msg(offset, *, partition=0, key="3", n_objects=2, value=None, classification="CAR"):
    if value is None:
        value = {"object_list": [{
            "timestamp": 1759670400100000, "frame_count": 5000 + offset,
            "objects": [make_object(i, classification) for i in range(n_objects)],
        }]}
    return {"topic": "my-topic", "partition": partition, "offset": offset,
            "timestamp_type": 1, "timestamp": KAFKA_TS_MS, "key": key, "value": value,
            "headers": {}, "_raw": None}


@pytest.fixture
def events():
    return []


@pytest.fixture
def db(events):
    return FakeDb(events)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Backoff sleeps are recorded, not slept. Also reset the shutdown flag."""
    sleeps = []
    monkeypatch.setattr(svc, "_sleep_responsively", lambda s: sleeps.append(s))
    monkeypatch.setattr(svc, "_shutdown", False)
    return sleeps


def make_consumer(db, events, *, insert_method="executemany", outage_exit_s=180.0,
                  lag_skip_after_s=3600.0, commit_error=None):
    metrics = svc.Metrics(FakeTel())
    log = logging.getLogger("test")
    writer = svc.DetectionWriter(
        {"host": "'10.0.0.1'", "port": 5432, "user": "u", "password": "p", "dbname": "NDOT"},
        metrics=metrics, log=log, insert_method=insert_method, connect=db.connect)
    consumer = svc.DetectionsConsumer(
        KafkaExplicitCredentials(bootstrap="localhost:1", user="u", password="p"),
        writer=writer, metrics=metrics, log=log, group_id="test-group", client_id="test",
        persistent=False, outage_exit_s=outage_exit_s, lag_skip_after_s=lag_skip_after_s)

    def fake_commit(message=None, *, asynchronous=False):
        assert asynchronous is False, "offsets must be committed synchronously"
        if commit_error is not None:
            raise commit_error
        events.append(("kafka_commit", message["partition"], message["offset"]))

    consumer.commit = fake_commit
    return consumer, metrics


# ---------------------------------------------------------------------------
# Database connection loss
# ---------------------------------------------------------------------------


def test_connection_loss_mid_batch_rolls_back_reconnects_and_commits_the_same_batch_once(db, events, no_sleep):
    consumer, metrics = make_consumer(db, events)
    # Rows for BOTH tables, so the first table's INSERT succeeds and the
    # connection dies on the second: genuinely mid-batch.
    batch = [make_msg(10, key="3"), make_msg(11, key="12"), make_msg(12, key="3")]
    db.fail_on_statement = {2: psycopg.OperationalError("the connection is lost")}

    assert consumer.process_batch(batch) is True

    # The first attempt rolled back: no rows from it survive. The retry
    # committed every row exactly once.
    assert events.index(("rollback",)) < events.index(("db_commit", 6))
    assert len(db.rows(svc.TABLE_OBJECTS)) == 4
    assert len(db.rows(svc.TABLE_BROADWAY)) == 2
    assert [e for e in events if e[0] == "db_commit"] == [("db_commit", 6)]
    # Reconnected once, after one backoff, and said so in the metrics.
    assert db.connects == 2
    assert no_sleep == [1.0]
    assert metrics.db_reconnects.get() == 1
    assert metrics.db_write_failures.get(error="OperationalError") == 1
    assert metrics.db_connected.get() == 1
    # The offset was committed once, after the only database commit.
    assert [e for e in events if e[0] == "kafka_commit"] == [("kafka_commit", 0, 12)]
    assert events.index(("db_commit", 6)) < events.index(("kafka_commit", 0, 12))
    assert metrics.rows_inserted.get(table=svc.TABLE_OBJECTS) == 4
    assert metrics.messages_malformed.values == {}


def test_db_down_longer_than_the_limit_exits_nonzero_without_committing_offsets(db, events, no_sleep,
                                                                                monkeypatch):
    # A fake clock that the (recorded, not slept) backoff advances.
    clock = [1000.0]
    monkeypatch.setattr(svc.time, "monotonic", lambda: clock[0])

    def sleep(seconds):
        no_sleep.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(svc, "_sleep_responsively", sleep)
    consumer, _metrics = make_consumer(db, events, outage_exit_s=20.0)
    db.fail_on_statement = {1: psycopg.OperationalError("the connection is lost")}
    db.fail_connects = 1000  # never comes back

    with pytest.raises(SystemExit) as info:
        consumer.process_batch([make_msg(1), make_msg(2)])
    assert info.value.code == 1
    assert not any(e[0] == "kafka_commit" for e in events)
    assert not any(e[0] == "db_commit" for e in events)
    # Backoff 1, 2, 4, 8 (= 15 s); the next 16 s would cross the 20 s limit.
    assert no_sleep == [1.0, 2.0, 4.0, 8.0]


def test_shutdown_during_an_outage_commits_nothing(db, events, monkeypatch):
    consumer, _metrics = make_consumer(db, events)
    db.fail_on_statement = {1: psycopg.OperationalError("the connection is lost")}

    def sleep_then_sigterm(_s):
        monkeypatch.setattr(svc, "_shutdown", True)

    monkeypatch.setattr(svc, "_sleep_responsively", sleep_then_sigterm)
    assert consumer.process_batch([make_msg(1)]) is False
    assert not any(e[0] in ("kafka_commit", "db_commit") for e in events)


def test_a_non_connection_db_error_is_retried_never_skipped(db, events, no_sleep):
    # e.g. a revoked GRANT: not a data problem, so the batch must not be skipped.
    consumer, metrics = make_consumer(db, events)
    db.fail_on_statement = {1: psycopg.errors.InsufficientPrivilege("permission denied")}
    assert consumer.process_batch([make_msg(1)]) is True
    assert len(db.rows(svc.TABLE_OBJECTS)) == 2
    assert metrics.db_write_failures.get(error="InsufficientPrivilege") == 1
    assert metrics.messages_malformed.values == {}
    assert events[-1] == ("kafka_commit", 0, 1)


def test_connection_settings_bound_every_wait(db, events):
    consumer, _metrics = make_consumer(db, events)
    consumer.process_batch([make_msg(1)])
    kwargs = db.connect_kwargs[0]
    assert kwargs["host"] == "10.0.0.1"  # quotes from a docker --env-file stripped
    assert kwargs["autocommit"] is True
    assert kwargs["options"] == "-c statement_timeout=60000"
    assert kwargs["keepalives"] == 1 and kwargs["tcp_user_timeout"] == 30000
    assert kwargs["connect_timeout"] == 10


# ---------------------------------------------------------------------------
# Malformed messages
# ---------------------------------------------------------------------------


def test_malformed_message_is_skipped_and_counted_while_the_rest_commit(db, events, caplog):
    consumer, metrics = make_consumer(db, events)
    bad = make_msg(21)
    del bad["value"]["object_list"][0]["objects"][1]["heading"]
    batch = [make_msg(20), bad, make_msg(22, value="this is not json"),
             make_msg(23, value={"zones": []}), make_msg(24)]

    with caplog.at_level(logging.WARNING, logger="test"):
        assert consumer.process_batch(batch) is True

    assert len(db.rows(svc.TABLE_OBJECTS)) == 4  # messages 20 and 24, two objects each
    assert metrics.messages_malformed.get(reason="missing_field") == 1
    assert metrics.messages_malformed.get(reason="not_json") == 1
    # A zone-config message has no detections: skipped, not malformed.
    assert metrics.messages_skipped.get(reason="no_object_list") == 1
    record = next(r for r in caplog.records if r.getMessage() == "malformed_messages_skipped")
    assert [(m["partition"], m["offset"]) for m in record.messages] == [(0, 21), (0, 22)]
    # The batch's offsets still advance past the bad messages.
    assert events[-1] == ("kafka_commit", 0, 24)


def test_message_postgres_rejects_as_data_is_isolated_and_the_rest_commit(db, events):
    consumer, metrics = make_consumer(db, events)
    db.reject_classification = "BAD"
    batch = [make_msg(30), make_msg(31, classification="BAD"), make_msg(32, key="11")]

    assert consumer.process_batch(batch) is True

    assert len(db.rows(svc.TABLE_OBJECTS)) == 2
    assert len(db.rows(svc.TABLE_BROADWAY)) == 2
    assert metrics.messages_malformed.get(reason="db_rejected") == 1
    # The whole-batch attempt rolled back; the isolation pass committed once.
    assert [e for e in events if e[0] == "db_commit"] == [("db_commit", 4)]
    assert ("rollback_to_savepoint",) in events
    assert events[-1] == ("kafka_commit", 0, 32)


def test_copy_method_writes_the_same_rows_and_falls_back_on_data_errors(db, events):
    consumer, metrics = make_consumer(db, events, insert_method="copy")
    assert consumer.process_batch([make_msg(1), make_msg(2, key="14")]) is True
    assert len(db.rows(svc.TABLE_OBJECTS)) == 2
    assert len(db.rows(svc.TABLE_BROADWAY)) == 2
    assert metrics.batch_insert_seconds.get(method="copy")

    db.reject_classification = "BAD"
    assert consumer.process_batch([make_msg(3), make_msg(4, classification="BAD")]) is True
    assert len(db.rows(svc.TABLE_OBJECTS)) == 4
    assert metrics.messages_malformed.get(reason="db_rejected") == 1


# ---------------------------------------------------------------------------
# Offset semantics
# ---------------------------------------------------------------------------


def test_offsets_are_committed_only_after_the_db_commit_one_per_partition(db, events):
    consumer, metrics = make_consumer(db, events)
    batch = [make_msg(5, partition=0), make_msg(9, partition=1), make_msg(6, partition=0),
             make_msg(10, partition=1)]
    consumer.process_batch(batch)

    db_commit = events.index(("db_commit", 8))
    kafka = [i for i, e in enumerate(events) if e[0] == "kafka_commit"]
    assert len(kafka) == 2 and all(i > db_commit for i in kafka)
    assert sorted(e for e in events if e[0] == "kafka_commit") == [
        ("kafka_commit", 0, 6), ("kafka_commit", 1, 10)]
    assert metrics.last_insert_timestamp.get() is not None
    assert metrics.consumer_lag_seconds.get(partition="0") > 0


def test_a_failed_offset_commit_is_counted_not_raised(db, events):
    consumer, metrics = make_consumer(db, events, commit_error=LvKafkaCommitError("rebalance"))
    assert consumer.process_batch([make_msg(1)]) is True
    assert len(db.rows(svc.TABLE_OBJECTS)) == 2
    assert metrics.offset_commit_failures.get() == 1


def test_a_batch_with_no_rows_commits_offsets_without_touching_the_db(db, events):
    consumer, _metrics = make_consumer(db, events)
    consumer.process_batch([make_msg(1, n_objects=0), make_msg(2, value={"zones": []})])
    assert db.connects == 0
    assert events == [("kafka_commit", 0, 2)]


def test_run_forever_polls_a_batch_writes_it_and_stops_on_shutdown(db, events, monkeypatch):
    consumer, _metrics = make_consumer(db, events)
    feed = [make_msg(1), make_msg(2), None]

    def fake_poll(timeout=1.0):
        if feed:
            return feed.pop(0)
        monkeypatch.setattr(svc, "_shutdown", True)
        return None

    consumer.poll = fake_poll
    consumer.run_forever()
    assert len(db.rows(svc.TABLE_OBJECTS)) == 4
    assert events[-1] == ("kafka_commit", 0, 2)
    assert consumer.pending == []


def test_revoke_drops_uncommitted_messages_of_revoked_partitions(db, events):
    consumer, _metrics = make_consumer(db, events)
    consumer.pending = [make_msg(1, partition=0), make_msg(2, partition=1), make_msg(3, partition=0)]
    consumer._on_revoke(None, [FakeTp("my-topic", 0)])
    assert [(m["partition"], m["offset"]) for m in consumer.pending] == [(1, 2)]


# ---------------------------------------------------------------------------
# Broadway routing and the row contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key,table", [
    ("11", svc.TABLE_BROADWAY), ("12", svc.TABLE_BROADWAY), ("13", svc.TABLE_BROADWAY),
    ("14", svc.TABLE_BROADWAY), ("15", svc.TABLE_BROADWAY),
    ("1", svc.TABLE_OBJECTS), ("10", svc.TABLE_OBJECTS), ("16", svc.TABLE_OBJECTS),
    ("276", svc.TABLE_OBJECTS), ("011", svc.TABLE_OBJECTS), (None, svc.TABLE_OBJECTS),
])
def test_broadway_routing_matches_the_tee_commit(key, table):
    assert svc.BROADWAY_INTERSECTION_IDS == frozenset({"11", "12", "13", "14", "15"})
    got_table, rows = svc.rows_from_message(make_msg(1, key=key))
    assert got_table == table
    assert all(row[-1] == key for row in rows)  # intersection_id is the key, unchanged


def test_broadway_ids_are_configurable():
    ids = frozenset({"11", "99"})
    assert svc.rows_from_message(make_msg(1, key="99"), ids)[0] == svc.TABLE_BROADWAY
    assert svc.rows_from_message(make_msg(1, key="12"), ids)[0] == svc.TABLE_OBJECTS


def test_row_values_match_the_old_writer():
    msg = make_msg(7, key="3", n_objects=1)
    _table, rows = svc.rows_from_message(msg, device_id="kafka_objects")
    row = dict(zip(svc.COLUMNS, rows[0]))
    assert len(rows[0]) == len(svc.COLUMNS) == 41
    obj = msg["value"]["object_list"][0]["objects"][0]
    assert row["frame_timestamp"] == 1759670400100000 and row["frame_count"] == 5007
    assert row["device_id"] == "kafka_objects"
    assert row["object_id"] == obj["id"] and row["object_uuid"] == obj["uuid"]
    assert row["length"] == 4.5 and row["orient_qw"] == 1.0
    assert row["distance_primary_sensor"] == 12.5 and row["num_primary_points"] == 250
    assert row["class_confidence"] == 0.9 and row["uncertain_vz"] == 0.5
    # frame_timestamp_dt is the Kafka message timestamp, in US/Central.
    expected = dt.datetime.fromtimestamp(KAFKA_TS_MS / 1000, tz=dt.timezone.utc)
    assert row["frame_timestamp_dt"] == expected
    assert row["frame_timestamp_dt"].tzinfo.key == "US/Central"
    assert row["intersection_id"] == "3"


def test_double_encoded_json_is_still_accepted():
    import json
    msg = make_msg(1)
    msg["value"] = json.dumps(msg["value"])
    assert len(svc.rows_from_message(msg)[1]) == 2


# ---------------------------------------------------------------------------
# Restart / lag guard
# ---------------------------------------------------------------------------


class FakeTp:
    def __init__(self, topic, partition, offset=-1001):
        self.topic = topic
        self.partition = partition
        self.offset = offset


class FakeRawConsumer:
    def __init__(self, committed, watermarks):
        self._committed = committed
        self._watermarks = watermarks
        self.assigned = None

    def committed(self, partitions, timeout=None):
        return [FakeTp(tp.topic, tp.partition, self._committed.get(tp.partition, -1001))
                for tp in partitions]

    def get_watermark_offsets(self, tp, timeout=None):
        return self._watermarks[tp.partition]

    def assign(self, partitions):
        self.assigned = [(tp.partition, tp.offset) for tp in partitions]


def assign(consumer, raw, cutoff):
    consumer.offsets_for_times = lambda topic, ts, partitions=None: dict(cutoff)
    consumer._on_assign(raw, [FakeTp("my-topic", p) for p in sorted(raw._watermarks)])
    return dict(raw.assigned)


def test_lag_guard_resumes_from_committed_when_recent(db, events):
    consumer, metrics = make_consumer(db, events)
    raw = FakeRawConsumer({0: 900}, {0: (100, 1000)})
    # The first message inside the window is at 800; committed 900 is newer.
    assert assign(consumer, raw, {0: 800}) == {0: -1001}  # -1001 = use the committed offset
    assert metrics.lag_skipped_messages.values == {}


def test_lag_guard_skips_to_latest_when_committed_offset_is_too_old(db, events, caplog):
    consumer, metrics = make_consumer(db, events)
    raw = FakeRawConsumer({0: 200, 1: 950}, {0: (100, 1000), 1: (100, 1000)})
    with caplog.at_level(logging.ERROR, logger="test"):
        got = assign(consumer, raw, {0: 800, 1: 800})
    assert got == {0: 1000, 1: -1001}
    assert metrics.lag_skipped_messages.get(partition="0") == 800
    record = next(r for r in caplog.records if r.getMessage() == "lag_guard_skipped_to_latest")
    assert record.partition == 0 and record.messages_skipped == 800
    assert record.messages_older_than_limit == 600


def test_lag_guard_treats_an_idle_partition_as_all_old(db, events):
    consumer, _metrics = make_consumer(db, events)
    raw = FakeRawConsumer({0: 500}, {0: (100, 1000)})
    # -1: no message is newer than the cutoff, so everything left is older.
    assert assign(consumer, raw, {0: -1}) == {0: 1000}


def test_lag_guard_disabled_always_resumes(db, events):
    consumer, metrics = make_consumer(db, events, lag_skip_after_s=0)
    raw = FakeRawConsumer({0: 200}, {0: (100, 1000)})
    assert assign(consumer, raw, {}) == {0: -1001}
    assert metrics.lag_skipped_messages.values == {}


def test_expired_committed_offset_is_reported_as_lost(db, events, caplog):
    consumer, metrics = make_consumer(db, events)
    raw = FakeRawConsumer({0: 50}, {0: (100, 1000)})
    with caplog.at_level(logging.ERROR, logger="test"):
        assert assign(consumer, raw, {0: 800}) == {0: 1000}
    assert metrics.lag_skipped_messages.get(partition="0") == 950
    assert any(r.getMessage() == "committed_offset_expired_data_lost" for r in caplog.records)

    consumer, _metrics = make_consumer(db, events, lag_skip_after_s=0)
    raw = FakeRawConsumer({0: 50}, {0: (100, 1000)})
    assert assign(consumer, raw, {}) == {0: 100}  # guard off: oldest message still held


def test_no_committed_offset_leaves_auto_offset_reset_in_charge(db, events):
    consumer, metrics = make_consumer(db, events)
    raw = FakeRawConsumer({}, {0: (100, 1000)})
    assert assign(consumer, raw, {0: 800}) == {0: -1001}
    assert metrics.lag_skipped_messages.values == {}


def test_lag_guard_lookup_failure_resumes_from_committed(db, events):
    consumer, _metrics = make_consumer(db, events)
    raw = FakeRawConsumer({0: 200}, {0: (100, 1000)})

    def boom(*args, **kwargs):
        raise RuntimeError("broker timeout")

    consumer.offsets_for_times = boom
    consumer._on_assign(raw, [FakeTp("my-topic", 0)])
    assert raw.assigned == [(0, -1001)]


# ---------------------------------------------------------------------------
# Startup configuration check
# ---------------------------------------------------------------------------


def test_config_refuses_an_outage_limit_kafka_would_evict_us_for(monkeypatch):
    svc._check_config()  # defaults are valid
    monkeypatch.setattr(svc, "DB_OUTAGE_EXIT_S", 280.0)
    with pytest.raises(RuntimeError, match="max.poll.interval.ms"):
        svc._check_config()
    monkeypatch.setattr(svc, "DB_OUTAGE_EXIT_S", 180.0)
    monkeypatch.setattr(svc, "DB_INSERT_METHOD", "bulk")
    with pytest.raises(RuntimeError, match="DB_INSERT_METHOD"):
        svc._check_config()
