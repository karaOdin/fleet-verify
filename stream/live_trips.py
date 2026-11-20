"""Live trip detection: Kafka fleet.positions -> fleet.trip-events, one event the
moment a bus reaches the opposite terminal (or ends an excursion).

    python -m stream.live_trips                 # Kafka (Quix Streams, state per device)
    python -m stream.live_trips --file data/inbox/positions_2026-11-19.csv.gz   # no Kafka, same logic

The rules are fleet/live.py, which a test proves identical to the batch.
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime

from fleet.live import init_state, process
from fleet.settings import terminals
from stream.common import BROKER, CFG, POSITIONS, TRIP_EVENTS, device_assignments, hhmm, load_lines, local_time
from stream.heartbeat import Heartbeat, append_events

LINES = None
ASSIGN: dict[int, dict] = {}
TERM = terminals()
HB = None
COUNT = {"processed": 0, "trips": 0, "excursions": 0, "data_time": None}


def _account(value: dict, events: list[dict], mode: str) -> None:
    COUNT["processed"] += 1
    COUNT["data_time"] = value["fix_time_utc"]
    for e in events:
        COUNT["trips" if e["event"] == "trip" else "excursions"] += e["event"] in ("trip", "excursion")
    append_events(events)
    HB.beat(mode=mode, **COUNT)


def setup():
    global LINES, ASSIGN
    LINES, ASSIGN = load_lines(), device_assignments()


def step(value: dict, st: dict | None) -> tuple[list[dict], dict | None]:
    """One position in, zero or more events out. `st` is this device's state (or None)."""
    a = ASSIGN.get(int(value["device_id"]))
    if a is None or a["line"] not in LINES:
        return [], st
    t = local_time(value["fix_time_utc"])
    h0, h1 = CFG["service_hours"]
    if not h0 <= t.hour <= h1:
        return [], st
    day = t.date().isoformat()
    if st is None or st.get("day") != day:           # a new service day starts from scratch
        st = {**init_state(), "day": day}
    line = LINES[a["line"]]
    from fleet.settings import plane
    xy = plane().xy([value["lat"]], [value["lon"]])[0]
    s = t.hour * 3600 + t.minute * 60 + t.second
    out = []
    for e in process(st, {"device_id": int(value["device_id"]), "s": s}, line, xy, CFG):
        names = TERM.get(line.code, {})
        out.append({**e, "bus": a["bus"], "day": day, "dep_time": hhmm(e["dep"]), "arr_time": hhmm(e["arr"]),
                    "from_name": names.get(e["frm"], e["frm"]), "to_name": names.get(e["to"], e["to"]),
                    "minutes": round((e["arr"] - e["dep"]) / 60)})
    return out, st


def run_kafka():
    from quixstreams import Application

    import os

    # LIVE_GROUP / LIVE_OFFSET_RESET let the visual pipeline start a clean demo (new group, latest offset)
    app = Application(broker_address=BROKER, consumer_group=os.environ.get("LIVE_GROUP", "live-trips"),
                      auto_offset_reset=os.environ.get("LIVE_OFFSET_RESET", "earliest"))
    src = app.topic(POSITIONS, key_deserializer="str", value_deserializer="json")
    dst = app.topic(TRIP_EVENTS, key_serializer="str", value_serializer="json")
    sdf = app.dataframe(src)

    def stateful(value, state):
        events, st = step(value, state.get("trip", None))
        if st is not None:
            state.set("trip", st)
        _account(value, events, "kafka")
        return events

    sdf = sdf.apply(stateful, stateful=True, expand=True)
    sdf = sdf.update(lambda e: print(f"{e['day']} {e['bus']:>6} {e['event']:9} {e['dep_time']} {e['from_name']} -> "
                                     f"{e['arr_time']} {e['to_name']} ({e['minutes']} min)"))
    sdf = sdf.to_topic(dst)
    app.run()


def run_file(path: str, speed: float = 0, start: str | None = None):
    """Same processing without Kafka. With --speed N the day is paced N times faster
    than real life, so it looks live on stage (the fallback when Kafka is down)."""
    import polars as pl

    from stream.common import from_local_time

    df = pl.read_csv(path).sort("fix_time_utc")
    if start:
        df = from_local_time(df, start)
    states: dict[int, dict] = {}
    t0 = w0 = None
    for r in df.iter_rows(named=True):
        if speed > 0:
            t = datetime.fromisoformat(r["fix_time_utc"].replace("Z", "+00:00")).timestamp()
            t0, w0 = (t, time.monotonic()) if t0 is None else (t0, w0)
            wait = (t - t0) / speed - (time.monotonic() - w0)
            if wait > 0:
                time.sleep(wait)
        events, states[r["device_id"]] = step(r, states.get(r["device_id"]))
        _account(r, events, "file")
        for e in events:
            print(f"{e['bus']:>6} {e['event']:9} {e['dep_time']} {e['from_name']} -> {e['arr_time']} {e['to_name']} ({e['minutes']} min)")
    HB.stop(mode="file", **COUNT)
    print(f"{COUNT['trips']} half-trips")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", help="replay a positions export without Kafka")
    ap.add_argument("--speed", type=float, default=0, help="with --file: x times real time (0 = as fast as possible)")
    ap.add_argument("--from", dest="start", help="with --file: local time to start from, e.g. 05:30")
    args = ap.parse_args()
    setup()
    HB = Heartbeat("live_trips")
    run_file(args.file, args.speed, args.start) if args.file else run_kafka()
