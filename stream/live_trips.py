"""Live trip detection: Kafka fleet.positions -> fleet.trip-events, one event the
moment a bus reaches the opposite terminal (or ends an excursion).

    python -m stream.live_trips                 # Kafka (Quix Streams, state per device)
    python -m stream.live_trips --file data/inbox/positions_2026-11-19.csv.gz   # no Kafka, same logic

The rules are fleet/live.py, which a test proves identical to the batch.
"""
from __future__ import annotations

import argparse
import os
import time
from datetime import datetime

import numpy as np

from fleet import alerts
from fleet.geo import PointProjector
from fleet.live import init_state, process
from fleet.settings import plane, terminals
from stream.common import ALERTS, BROKER, CFG, DATA, POSITIONS, TRIP_EVENTS, device_assignments, hhmm, load_lines, local_time
from stream.heartbeat import Heartbeat, append_alerts, append_events

LINES = None
ASSIGN: dict[int, dict] = {}
TERM = terminals()
PLANE = plane()
HB = None
COUNT = {"processed": 0, "trips": 0, "excursions": 0, "alerts": 0, "data_time": None}
# Stream clocks for `silent`, one per Kafka partition: Kafka orders messages within a partition only, so a
# device is compared with the devices that share its partition (as Kafka Streams keeps "stream time" per task).
CLOCKS: dict[int, dict] = {}
OPEN: dict[str, dict] = {}           # open alerts by alert_id, for the heartbeat


def _account(value: dict, events: list[dict], mode: str) -> None:
    global HB
    if HB is None:
        HB = Heartbeat("live_trips")
    COUNT["processed"] += 1
    COUNT["data_time"] = value["fix_time_utc"]
    trips = [e for e in events if "event" in e]
    for e in trips:
        COUNT["trips" if e["event"] == "trip" else "excursions"] += e["event"] in ("trip", "excursion")
    found = [e for e in events if "alert" in e]
    for e in found:
        if e["state"] == "opened":
            COUNT["alerts"] += 1
            OPEN[e["alert_id"]] = {k: e[k] for k in ("alert", "bus", "line", "start_time", "alert_id")}
        else:
            OPEN.pop(e["alert_id"], None)
    append_events(trips)
    append_alerts(found)
    HB.beat(mode=mode, alerts_open=len(OPEN), open_alerts=list(OPEN.values())[-20:], **COUNT)


def _alert_out(e: dict, day: str) -> dict:
    a = ASSIGN.get(e["device_id"], {})
    out = {**e, "bus": a.get("bus"), "day": day, "start_time": hhmm(e["start"]), "at_time": hhmm(e["at"]),
           "note": "for the dispatcher to review"}   # a recommendation for the dispatcher, never a sanction
    return out


_PROJ: dict[str, PointProjector] = {}


def _projector(line) -> PointProjector:
    if line.code not in _PROJ or _PROJ[line.code].cum is not line.cum:
        _PROJ[line.code] = PointProjector(line.pts, line.cum)
    return _PROJ[line.code]


def setup():
    global LINES, ASSIGN
    LINES, ASSIGN = load_lines(), device_assignments()


def step(value: dict, st: dict | None, partition: int = 0) -> tuple[list[dict], dict | None]:
    """One position in, zero or more events out. `st` is this device's state (or None);
    `partition` is the Kafka partition the position came from (its stream clock)."""
    a = ASSIGN.get(int(value["device_id"]))
    if a is None or a["line"] not in LINES:
        return [], st
    t = local_time(value["fix_time_utc"])
    h0, h1 = CFG["service_hours"]
    if not h0 <= t.hour <= h1:
        return [], st
    day = t.date().isoformat()
    ts = t.timestamp()
    rewound = st is not None and ts < st.get("ts", ts) - CFG["alerts"]["rewind_min"] * 60   # the day replayed again
    if st is None or st.get("day") != day or rewound:  # a new service day (or a replay) starts from scratch
        st = {**init_state(), "day": day, "alerts": alerts.init_state()}
    st["ts"] = max(ts, st.get("ts", ts))
    line = LINES[a["line"]]
    xy = ((value["lon"] - PLANE.lon0) * PLANE.kx, (value["lat"] - PLANE.lat0) * PLANE.ky)   # same as Plane.xy, one point
    s = t.hour * 3600 + t.minute * 60 + t.second
    pt = {"device_id": int(value["device_id"]), "s": s, "ts": ts, "kmh": float(value["speed_kn"] or 0) * 1.852,
          "lat": value["lat"], "lon": value["lon"]}
    q = np.asarray(xy, dtype=float)                 # measured once per point, shared by the trip and alert rules
    proj = (*_projector(line)(*xy), float(np.hypot(*(q - line.S))), float(np.hypot(*(q - line.E))))
    out = []
    found = alerts.process(st["alerts"], pt, line, xy, CFG, proj)
    clock = CLOCKS.setdefault(partition, alerts.clock_init())
    found += alerts.observe(clock, pt, line.code, st["alerts"], CFG)
    found += alerts.check_silent(clock, s, CFG)
    out += [_alert_out(e, day) for e in found]
    for e in process(st, pt, line, xy, CFG, proj):
        names = TERM.get(line.code, {})
        out.append({**e, "bus": a["bus"], "day": day, "dep_time": hhmm(e["dep"]), "arr_time": hhmm(e["arr"]),
                    "from_name": names.get(e["frm"], e["frm"]), "to_name": names.get(e["to"], e["to"]),
                    "from_name_en": names.get("name_en", {}).get(e["frm"], e["frm"]),   # display only
                    "to_name_en": names.get("name_en", {}).get(e["to"], e["to"]),
                    "minutes": round((e["arr"] - e["dep"]) / 60)})
    return out, st


def build_app(group: str, offset_reset: str = "earliest", src_topic: str = POSITIONS, dst_topic: str = TRIP_EVENTS,
              verbose: bool = True, alerts_topic: str = ALERTS):
    """The Quix Streams application: positions in, trip events and alerts out, state per device (the message key)."""
    from quixstreams import Application
    from quixstreams.models import TopicConfig

    # The device state lives in RocksDB under data/quix-state and survives a restart on this machine. A Kafka
    # changelog (one extra message per position) is only needed to move the app to another host: LIVE_CHANGELOG=1.
    changelog = os.environ.get("LIVE_CHANGELOG", "0") == "1"
    app = Application(broker_address=BROKER, consumer_group=group, auto_offset_reset=offset_reset, commit_interval=1.0,
                      use_changelog_topics=changelog,
                      # let the broker batch ~0.1 s of positions per fetch: a caught-up consumer otherwise sends
                      # thousands of tiny fetch requests a second, which costs more CPU than the trip rules
                      consumer_extra_config={"fetch.min.bytes": 65536, "fetch.wait.max.ms": 100},
                      state_dir=str(DATA / "quix-state"), loglevel="INFO" if verbose else "WARNING")
    tc = TopicConfig(num_partitions=3, replication_factor=1)    # only used if Quix creates the topic
    src = app.topic(src_topic, key_deserializer="str", value_deserializer="json", config=tc)
    dst = app.topic(dst_topic, key_serializer="str", value_serializer="json", config=tc)
    alt = app.topic(alerts_topic, key_serializer="str", value_serializer="json", config=tc)
    sdf = app.dataframe(src)

    from quixstreams import message_context

    def stateful(value, state):
        events, st = step(value, state.get("trip", None), message_context().partition)
        if st is not None:
            state.set("trip", st)
        _account(value, events, "kafka")
        return events

    sdf = sdf.apply(stateful, stateful=True, expand=True)
    # two branches of the same stream: trip events and alerts go to their own topics
    trips = sdf.filter(lambda e: "event" in e)
    found = sdf.filter(lambda e: "alert" in e)
    if verbose:
        trips = trips.update(lambda e: print(f"{e['day']} {e['bus']:>6} {e['event']:9} {e['dep_time']} {e['from_name']} -> "
                                             f"{e['arr_time']} {e['to_name']} ({e['minutes']} min)", flush=True))
        found = found.update(lambda e: print(f"{e['day']} {e['bus']:>6} ALERT {e['alert']} {e['state']} "
                                             f"since {e['start_time']}" + (f" ({e['duration_min']} min)" if 'duration_min' in e else ""),
                                             flush=True))
    trips.to_topic(dst)
    found.to_topic(alt, key=lambda e: str(e["device_id"]))     # keyed by the device the alert is about
    return app, sdf


def run_kafka():
    # LIVE_GROUP / LIVE_OFFSET_RESET let the visual pipeline start a clean demo (new group, latest offset)
    group, reset = os.environ.get("LIVE_GROUP", "live-trips"), os.environ.get("LIVE_OFFSET_RESET", "earliest")
    while True:
        app, _ = build_app(group, reset)
        try:
            app.run()                    # returns on SIGTERM / Ctrl-C: stop for good
            if HB is not None:
                HB.stop(mode="kafka", **COUNT)   # writes the last buffered events too
            return
        except Exception as e:           # a broker hiccup (e.g. "Unknown member" after a session timeout):
            print(f"live_trips: {type(e).__name__}: {e}; restarting in 3 s from the last committed offset",
                  flush=True)            # state and offsets are in Kafka, so it resumes where it was
            time.sleep(3)


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
        for e in (x for x in events if "event" in x):
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
