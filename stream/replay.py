"""Replay a day of positions into Kafka in time order, N times faster than real life.

    python -m stream.replay data/inbox/positions_2026-11-19.csv.gz --speed 120
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime

import polars as pl
from confluent_kafka import Producer

from stream.common import BROKER, POSITIONS, from_local_time, message
from stream.heartbeat import Heartbeat


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--speed", type=float, default=60, help="x times real time (0 = as fast as possible)")
    ap.add_argument("--from", dest="start", help="local time to start from, e.g. 05:30 (skips the night)")
    args = ap.parse_args()
    df = pl.read_csv(args.file).sort("fix_time_utc")
    if args.start:
        df = from_local_time(df, args.start)
    p = Producer({"bootstrap.servers": BROKER, "linger.ms": 20})
    hb = Heartbeat("replay")
    t0_data = t0_wall = None
    for i, r in enumerate(df.iter_rows(named=True)):
        t = datetime.fromisoformat(r["fix_time_utc"].replace("Z", "+00:00")).timestamp()
        if args.speed > 0:
            if t0_data is None:
                t0_data, t0_wall = t, time.monotonic()
            wait = (t - t0_data) / args.speed - (time.monotonic() - t0_wall)
            if wait > 0:
                time.sleep(wait)
        k, v = message(r["device_id"], r["fix_time_utc"], r["lat"], r["lon"], r["speed_kn"])
        p.produce(POSITIONS, key=k, value=v)
        hb.beat(sent=i + 1, total=df.height, data_time=r["fix_time_utc"], speed=args.speed, file=args.file)
        if i % 5000 == 0:
            p.poll(0)
            print(f"{i:>7} sent, data time {r['fix_time_utc']}", flush=True)
    p.flush()
    hb.stop(sent=df.height, total=df.height, speed=args.speed, file=args.file)
    print(f"done: {df.height} positions")


if __name__ == "__main__":
    main()
