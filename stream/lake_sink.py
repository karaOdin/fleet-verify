"""Kafka fleet.positions -> Parquet files, one folder per local day:
data/lake/positions/date=YYYY-MM-DD/part-*.parquet

The daily Dagster run reads that folder. Offsets are committed only after the
file is written, so a crash can repeat messages but never lose them; the
pipeline drops the repeats.

    python -m stream.lake_sink
"""
from __future__ import annotations

import json
import time
from collections import defaultdict

import polars as pl
from confluent_kafka import Consumer, TopicPartition

from stream.common import BROKER, DATA, POSITIONS, local_time
from stream.heartbeat import Heartbeat

FLUSH_S, FLUSH_N = 30, 20_000


def main():
    c = Consumer({"bootstrap.servers": BROKER, "group.id": "lake-sink", "enable.auto.commit": False,
                  "auto.offset.reset": "earliest"})
    c.subscribe([POSITIONS])
    buf, last, upto = [], time.monotonic(), {}      # upto: partition -> next offset to read, for what buf holds
    hb, written, files = Heartbeat("lake_sink"), 0, 0
    try:
        while True:
            m = c.poll(1.0)
            if m is not None and not m.error():
                buf.append(json.loads(m.value()))
                upto[m.partition()] = m.offset() + 1
            hb.beat(buffered=len(buf), written=written, files=files)
            if buf and (len(buf) >= FLUSH_N or time.monotonic() - last >= FLUSH_S):
                by_day = defaultdict(list)
                for r in buf:
                    by_day[local_time(r["fix_time_utc"]).date().isoformat()].append(r)
                for day, rows in by_day.items():
                    d = DATA / "lake" / "positions" / f"date={day}"
                    d.mkdir(parents=True, exist_ok=True)
                    pl.DataFrame(rows).write_parquet(d / f"part-{time.time_ns()}.parquet")
                # commit exactly what was written. A plain commit() uses the consumer's offset store, which is
                # empty after an offset reset (topics recreated by reset_demo --kafka): it failed with
                # _NO_OFFSET, the container restarted and wrote the same positions again, in a loop.
                c.commit(offsets=[TopicPartition(POSITIONS, p, o) for p, o in upto.items()], asynchronous=False)
                written += len(buf)
                files += len(by_day)
                print(f"wrote {len(buf)} positions ({', '.join(f'{k}: {len(v)}' for k, v in by_day.items())})", flush=True)
                buf, last, upto = [], time.monotonic(), {}
    finally:
        c.close()


if __name__ == "__main__":
    main()
