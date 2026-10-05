"""bench_insert.py — time DB_INSERT_METHOD=executemany against =copy.

WRITES ROWS into the two tables you name. Point it at SCRATCH copies of the
detection tables, never the live ones (it refuses the two laddms names). It
uses the service's own DetectionWriter and rows_from_message(), so it times
exactly the code path the service runs, against synthetic frames shaped like
the real ones (22 objects per frame by default).

    python bench_insert.py --objects-table bench.object_detections \
        --broadway-table bench.broadway_object_detections --rounds 10

Reads DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_DBNAME like the service.
README "COPY vs executemany" has the scratch-table SQL and how to read the
result. Alternates the two methods round by round so drift in server load
hits both equally.
"""

from __future__ import annotations

import argparse
import statistics
import time

from lv_db_connector import DbEnvCredentials
from lv_telemetry_connector import configure_telemetry

import ouster2db as svc


def synthetic_batch(n_messages: int, objects_per_message: int, broadway_share: float, offset0: int):
    now_ms = int(time.time() * 1000)
    batch = []
    for m in range(n_messages):
        key = "12" if (m % 100) < broadway_share * 100 else "3"
        objects = []
        for i in range(objects_per_message):
            xyz = {"x": 10.0 + i * 0.37, "y": -4.25 + m * 0.01, "z": 0.8}
            objects.append({
                "id": 40000 + i, "uuid": f"6f1c2a3e-0000-4000-8000-{m * 1000 + i:012d}",
                "update_ts": now_ms * 1000, "classification": "VEHICLE", "sub_classification": "CAR",
                "dimensions": {"length": 4.61, "width": 1.92, "height": 1.55},
                "position": dict(xyz), "velocity": {"x": 3.1, "y": -0.2, "z": 0.0}, "heading": 1.234,
                "orientation": {"qw": 0.98, "qx": 0.0, "qy": 0.0, "qz": 0.17},
                "creation_ts": now_ms * 1000 - 5000000, "frame_count": 120 + m,
                "initial_position": dict(xyz), "primary_sensor": "122233000123",
                "distance_to_primary_sensor": 23.75, "num_points": 412,
                "num_points_from_primary_sensor": 390, "num_failed_returns": 7,
                "classification_confidence": 0.93,
                "position_uncertainty": {"x": 0.05, "y": 0.05, "z": 0.02},
                "velocity_uncertainty": {"x": 0.1, "y": 0.1, "z": 0.01},
            })
        value = {"object_list": [{"timestamp": now_ms * 1000, "frame_count": 900000 + m,
                                  "objects": objects}]}
        batch.append({"topic": "bench", "partition": 0, "offset": offset0 + m,
                      "timestamp": now_ms, "key": key, "value": value})
    return batch


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--objects-table", required=True)
    ap.add_argument("--broadway-table", required=True)
    ap.add_argument("--rounds", type=int, default=10, help="batches per method")
    ap.add_argument("--messages", type=int, default=300, help="messages per batch (BATCH_MAX_MESSAGES)")
    ap.add_argument("--objects-per-message", type=int, default=22)
    ap.add_argument("--broadway-share", type=float, default=0.3)
    args = ap.parse_args()
    for table in (args.objects_table, args.broadway_table):
        if table in (svc.TABLE_OBJECTS, svc.TABLE_BROADWAY):
            raise SystemExit(f"refusing to benchmark against the live table {table}")

    tel = configure_telemetry(service="ouster_kafka2db_bench", metrics=False)
    log = tel.get_logger("bench")
    metrics = svc.Metrics(tel)
    conninfo = DbEnvCredentials().resolve()
    writers = {method: svc.DetectionWriter(conninfo, metrics=metrics, log=log, insert_method=method,
                                           application_name="ouster_kafka2db_bench",
                                           table_objects=args.objects_table,
                                           table_broadway=args.broadway_table)
               for method in ("executemany", "copy")}
    timings = {method: [] for method in writers}
    rows_per_batch = 0
    try:
        for r in range(args.rounds):
            for method, writer in writers.items():
                batch = synthetic_batch(args.messages, args.objects_per_message, args.broadway_share,
                                        r * args.messages)
                parsed = [(msg, *svc.rows_from_message(msg)) for msg in batch]
                inserted, rejected, seconds = writer.write_batch(parsed)
                if rejected:
                    raise SystemExit(f"{method}: {len(rejected)} messages rejected: {rejected[0][1]}")
                rows_per_batch = sum(inserted.values())
                timings[method].append(seconds)
    finally:
        for writer in writers.values():
            writer.close()

    print(f"{args.rounds} batches per method, {args.messages} messages x "
          f"{args.objects_per_message} objects = {rows_per_batch} rows per batch")
    for method, ts in timings.items():
        ts = sorted(ts)
        p95 = ts[min(len(ts) - 1, int(round(0.95 * (len(ts) - 1))))]
        print(f"  {method:12s} median {statistics.median(ts) * 1000:8.1f} ms  p95 {p95 * 1000:8.1f} ms  "
              f"-> {rows_per_batch / statistics.median(ts):9.0f} rows/s per worker")


if __name__ == "__main__":
    main()
