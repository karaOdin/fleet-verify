"""Bring the demo back to its starting point before the talk, without touching the prepared data.

    python -m tools.reset_demo                 # day 2026-11-19 (VIZ_DAY)
    python -m tools.reset_demo --day 2026-11-19 --kafka

Removes only what the demo creates:
  - live state: heartbeats, trip events, alerts, paper checks (data/state)
  - the replay's partial lake for that day (data/lake/positions/date=<day>)
  - that day's batch results (data/warehouse/<asset>/<day>.parquet) and its rows in PostgreSQL,
    so the batch can be run live on stage and the day appears in Superset
  - with --kafka: the demo topics are deleted and recreated empty
Keeps: data/inbox (the real inputs), the other days' results (the week in Superset), Dagster history.
"""
from __future__ import annotations

import argparse
import os
import shutil
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = Path(os.environ.get("FLEET_DATA", ROOT / "data"))
DAILY_ASSETS = ["raw_positions", "road_papers", "route_fit", "gps_trips", "device_identity", "trip_verification",
                "km_report", "bus_day_report", "postgres_export"]
PG_TABLES = ["bus_day_report", "trip_verification", "km_report", "device_identity"]
TOPICS = ["fleet.positions", "fleet.trip-events", "fleet.alerts", "fleet.road-papers", "fleet.paper-checks"]


def reset(day: str, kafka: bool) -> list[str]:
    done = []
    state = DATA / "state"
    if state.exists():
        for f in state.iterdir():
            if f.name != "papers_cursor.txt":
                f.unlink() if f.is_file() else shutil.rmtree(f)
        done.append("live state cleared")
    quix = DATA / "quix-state"            # Quix Streams' local state store (one folder per consumer group)
    if quix.exists():
        shutil.rmtree(quix)
        done.append("Quix state store removed")
    lake = DATA / "lake" / "positions" / f"date={day}"
    if lake.exists():
        shutil.rmtree(lake)
        done.append(f"lake for {day} removed")
    n = 0
    for a in DAILY_ASSETS:
        f = DATA / "warehouse" / a / f"{day}.parquet"
        if f.exists():
            f.unlink()
            n += 1
    done.append(f"{n} batch outputs for {day} removed")
    url = os.environ.get("POSTGRES_URL")
    if url:
        try:
            import psycopg2
            with psycopg2.connect(url, connect_timeout=3) as conn, conn.cursor() as cur:
                for t in PG_TABLES:
                    cur.execute("select to_regclass(%s)", (t,))
                    if cur.fetchone()[0]:
                        cur.execute(f'delete from "{t}" where work_date = %s', (day,))
            done.append(f"PostgreSQL rows for {day} removed")
        except Exception as e:
            done.append(f"PostgreSQL NOT reset: {e}")
    if kafka:
        try:
            from confluent_kafka.admin import AdminClient, NewTopic
            broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
            admin = AdminClient({"bootstrap.servers": broker})
            existing = set(admin.list_topics(timeout=5).topics)
            # plus the changelog topics Quix Streams keeps for each consumer group's state
            gone = [t for t in existing if t in TOPICS or t.startswith("changelog__live-trips")]
            for f in (admin.delete_topics(gone, operation_timeout=10).values() if gone else []):
                f.result()
            time.sleep(3)
            for f in admin.create_topics([NewTopic(t, num_partitions=3, replication_factor=1) for t in TOPICS]).values():
                try:
                    f.result()
                except Exception:
                    pass
            done.append(f"Kafka topics recreated empty: {', '.join(TOPICS)}")
        except Exception as e:
            done.append(f"Kafka NOT reset: {e}")
    return done


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default=os.environ.get("VIZ_DAY", "2026-11-19"))
    ap.add_argument("--kafka", action="store_true", help="also delete and recreate the demo topics")
    args = ap.parse_args()
    for line in reset(args.day, args.kafka):
        print("-", line)
    print("\nNow: restart the viz page (or press Clean start). The top lane is idle, the bottom lane is grey for",
          args.day, "and Superset shows the other days.")
