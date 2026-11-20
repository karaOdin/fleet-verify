"""Live source: poll Traccar for each device's latest position and send new ones to Kafka.

    python -m stream.traccar_poll --every 10

TRACCAR_URL and TRACCAR_TOKEN come from the environment (.env), never the command line.
"""
from __future__ import annotations

import argparse
import time

from confluent_kafka import Producer

from sources import traccar
from stream.heartbeat import Heartbeat
from stream.common import BROKER, POSITIONS, event_ms, message


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--every", type=float, default=10, help="seconds between polls")
    args = ap.parse_args()
    if not traccar.configured():
        raise SystemExit("Set TRACCAR_URL and TRACCAR_TOKEN in the environment.")
    p = Producer({"bootstrap.servers": BROKER})
    last: dict[int, str] = {}
    hb, total = Heartbeat("traccar_poll", every=0), 0
    while True:
        try:
            sent = 0
            for r in traccar.latest():
                if last.get(r["device_id"]) == r["fix_time_utc"]:
                    continue
                last[r["device_id"]] = r["fix_time_utc"]
                k, v = message(r["device_id"], r["fix_time_utc"], r["lat"], r["lon"], r["speed_kn"])
                p.produce(POSITIONS, key=k, value=v, timestamp=event_ms(r["fix_time_utc"]))
                sent += 1
            p.flush(5)
            total += sent
            hb.beat(last_batch=sent, total=total)
            print(f"{time.strftime('%H:%M:%S')} {sent} new positions", flush=True)
        except Exception as e:          # network blips: keep polling
            print(f"poll failed: {e}", flush=True)
        time.sleep(args.every)


if __name__ == "__main__":
    main()
