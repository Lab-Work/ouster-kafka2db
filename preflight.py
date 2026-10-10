"""ouster-kafka2db preflight: checks everything the new writer needs WITHOUT consuming or writing.

Reads only: environment, one SELECT on Postgres, Kafka metadata, the group's committed offsets.
Never subscribes, never joins the consumer group, never commits, never inserts.
"""
import os
import sys
import time

ok = True


def check(label, fn):
    global ok
    try:
        print(f"[ OK ] {label}: {fn()}")
    except Exception as exc:  # report and keep going: the point is a full picture
        ok = False
        print(f"[FAIL] {label}: {type(exc).__name__}: {exc}")


def env():
    required = ["KAFKA_BOOTSTRAP", "KAFKA_USER", "KAFKA_PASSWORD", "DB_HOST", "DB_PORT",
                "DB_DBNAME", "DB_USER", "DB_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"missing {missing}")
    ca = os.environ.get("KAFKA_CA_CERT") or os.environ.get("KAFKA_CA_LOCATION", "strimzi-ca.crt")
    if not os.environ.get("KAFKA_CA_CERT") and not os.path.exists(ca):
        raise RuntimeError(f"KAFKA_CA_LOCATION={ca!r} does not exist inside the container (mount it)")
    return (f"topic={os.environ.get('KAFKA_TOPIC', 'my-topic')} "
            f"group={os.environ.get('KAFKA_GROUP_ID', 'ouster2db_objects')} "
            f"workers={os.environ.get('WORKERS', '3')} db={os.environ['DB_HOST']}/{os.environ['DB_DBNAME']} "
            f"db_user={os.environ['DB_USER']} kafka_user={os.environ['KAFKA_USER']}")


def db():
    from lv_db_connector import Connector, DbEnvCredentials
    c = Connector(DbEnvCredentials(), persistent=True)
    try:
        r = c.query_one("""
            SELECT now() AS now,
                   has_table_privilege('laddms.object_detections', 'INSERT') AS ins_main,
                   has_table_privilege('laddms.broadway_object_detections', 'INSERT') AS ins_broadway""")
    finally:
        c.close()
    if not (r["ins_main"] and r["ins_broadway"]):
        raise RuntimeError(f"no INSERT privilege: main={r['ins_main']} broadway={r['ins_broadway']}")
    return "connected; INSERT allowed on both detection tables"


def kafka():
    from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient
    from lv_kafka_connector import KafkaEnvCredentials
    from lv_kafka_connector._driver import _security_config

    conf = _security_config(KafkaEnvCredentials().resolve())
    topic = os.environ.get("KAFKA_TOPIC", "my-topic")
    group = os.environ.get("KAFKA_GROUP_ID", "ouster2db_objects")
    admin = AdminClient(conf)
    md = admin.list_topics(topic, timeout=15)
    parts = sorted(md.topics[topic].partitions)
    if not parts:
        raise RuntimeError(f"topic {topic!r} has no partitions (wrong name?)")
    lines = [f"{topic}: {len(parts)} partitions"]

    # Committed offsets of the group: where the new writer will resume. AdminClient only, no join.
    fut = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group, [TopicPartition(topic, p) for p in parts])])
    committed = {tp.partition: tp.offset for tp in fut[group].result(timeout=15).topic_partitions}

    # Newest offsets, and the timestamp of the message at each committed offset, via a consumer
    # that is never subscribed (assign + get_watermark_offsets / offsets_for_times only).
    probe = Consumer({**conf, "group.id": "ouster-preflight-readonly", "enable.auto.commit": False})
    now_ms = int(time.time() * 1000)
    try:
        for p in parts:
            low, high = probe.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
            c = committed.get(p, -1001)
            behind_msgs = high - c if c >= 0 else None
            # oldest retained message's time, to show the topic's real retention
            oldest = probe.offsets_for_times([TopicPartition(topic, p, 0)], timeout=15)[0]
            lines.append(
                f"  p{p}: committed={c} newest={high} behind={behind_msgs} msgs"
                + ("" if c >= 0 else "  (NO committed offset: new writer would start at 'latest')"))
            if oldest.offset >= 0:
                probe.assign([TopicPartition(topic, p, oldest.offset)])
                m = probe.poll(10)
                if m is not None and m.timestamp()[1] > 0:
                    lines.append(f"       oldest retained message is {(now_ms - m.timestamp()[1]) / 3.6e6:.1f} h old")
                probe.unassign()
    finally:
        probe.close()
    return "\n" + "\n".join(lines)


check("environment", env)
check("postgres", db)
check("kafka", kafka)
print("\nALL CHECKS PASSED" if ok else "\nSOMETHING FAILED - fix before the cutover")
sys.exit(0 if ok else 1)
