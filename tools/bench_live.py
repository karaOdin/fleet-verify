"""How many positions per second does live_trips handle?

    python -m tools.bench_live --mode file              # the rules only (as --file --speed 0), no Kafka
    python -m tools.bench_live --mode kafka             # through Kafka and Quix Streams (fresh topic and group)
    python -m tools.bench_live --mode file --profile    # plus a cProfile summary

Uses 07:00-10:00 of the demo day (the busiest hours). Writes nothing to the demo's state files;
the Kafka mode deletes its topics when done.
"""
from __future__ import annotations

import argparse
import cProfile
import io
import os
import pstats
import shutil
import time
import uuid

import polars as pl

DAY_FILE = os.environ.get("VIZ_DEMO_FILE", "data/inbox/positions_2026-11-19.csv.gz")


def _window(start: str, end: str) -> pl.DataFrame:
    from stream.common import before_local_time, from_local_time
    return before_local_time(from_local_time(pl.read_csv(DAY_FILE).sort("fix_time_utc"), start), end)


def _quiet(lt):
    from stream.heartbeat import Heartbeat
    lt.HB = Heartbeat(f"bench-{uuid.uuid4().hex[:6]}")
    lt.append_events = lambda e: None
    lt.append_alerts = lambda e: None
    return lt.HB.path


def bench_file(df: pl.DataFrame) -> float:
    import stream.live_trips as lt
    lt.setup()
    hb = _quiet(lt)
    rows = list(df.iter_rows(named=True))
    states: dict = {}
    t = time.perf_counter()
    for r in rows:
        events, states[r["device_id"]] = lt.step(r, states.get(r["device_id"]))
        lt._account(r, events, "bench")
    dt = time.perf_counter() - t
    hb.unlink(missing_ok=True)
    return len(rows) / dt


def bench_kafka(df: pl.DataFrame) -> float:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient

    import stream.live_trips as lt
    from stream.common import BROKER, DATA, event_ms, message
    lt.setup()
    hb = _quiet(lt)
    tag = uuid.uuid4().hex[:8]
    src = f"bench.positions.{tag}"
    p = Producer({"bootstrap.servers": BROKER, "linger.ms": 20})
    for r in df.iter_rows(named=True):
        k, v = message(r["device_id"], r["fix_time_utc"], r["lat"], r["lon"], r["speed_kn"])
        while True:
            try:
                p.produce(src, key=k, value=v, timestamp=event_ms(r["fix_time_utc"]))
                break
            except BufferError:
                p.poll(0.2)
    p.flush(60)
    group = f"bench-{tag}"
    app, _ = lt.build_app(group, "earliest", src, f"bench.events.{tag}", verbose=False, alerts_topic=f"bench.alerts.{tag}")
    lt.COUNT["processed"] = 0
    first: dict = {}

    def mark(*_):                                    # the clock starts at the first message, not at the group join
        first.setdefault("t", time.perf_counter())
    app._on_message_processed = mark
    idle = 5
    t0 = time.perf_counter()
    try:
        app.run(timeout=idle)
    finally:
        admin = AdminClient({"bootstrap.servers": BROKER})
        mine = [t for t in admin.list_topics(timeout=5).topics if tag in t]
        if mine:
            admin.delete_topics(mine, operation_timeout=10)
        shutil.rmtree(DATA / "quix-state" / group, ignore_errors=True)
        hb.unlink(missing_ok=True)
    dt = time.perf_counter() - first.get("t", t0) - idle
    return lt.COUNT["processed"] / dt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["file", "kafka"], default="file")
    ap.add_argument("--from", dest="start", default="07:00")
    ap.add_argument("--to", dest="end", default="10:00")
    ap.add_argument("--profile", action="store_true")
    args = ap.parse_args()
    df = _window(args.start, args.end)
    run = bench_file if args.mode == "file" else bench_kafka
    if args.profile:
        prof = cProfile.Profile()
        rate = prof.runcall(run, df)
        out = io.StringIO()
        pstats.Stats(prof, stream=out).sort_stats("tottime").print_stats(15)
        print(out.getvalue())
    else:
        rate = run(df)
    print(f"{args.mode}: {df.height} positions, {rate:,.0f} positions/s")


if __name__ == "__main__":
    main()
