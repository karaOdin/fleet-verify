"""Through a real Kafka broker: a two-hour window of 19 Nov is produced into a fresh topic,
the live_trips Quix Streams app consumes it with a fresh consumer group, and the trip
events read back from Kafka must equal the batch's trips for the same window.

Skipped unless KAFKA_BROKER (default localhost:9092) answers.
"""
import json
import os
import shutil
import time
import uuid

import polars as pl
import pytest

BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
# A 30-minute window holds no complete trip on this day (a trip is ~30 min plus the terminal
# episode before it), so the window is two hours: ~37k messages, a few seconds through Kafka.
WINDOW = ("06:00", "08:00")


def _broker_up() -> bool:
    try:
        from confluent_kafka.admin import AdminClient
        AdminClient({"bootstrap.servers": BROKER, "socket.timeout.ms": 2000}).list_topics(timeout=3)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _broker_up(), reason=f"no Kafka broker at {BROKER}")


def _consume_all(topic: str, idle_s: float = 5.0) -> list[dict]:
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": BROKER, "group.id": f"e2e-read-{uuid.uuid4().hex}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    out, last = [], time.monotonic()
    try:
        while time.monotonic() - last < idle_s:
            m = c.poll(0.5)
            if m is None or m.error():
                continue
            out.append(json.loads(m.value()))
            last = time.monotonic()
    finally:
        c.close()
    return out


def test_kafka_live_equals_batch_window(day):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient

    from fleet.positions import normalise, with_xy
    from fleet.settings import plane
    from fleet.trips import detect, zone_points
    import stream.live_trips as lt
    from stream.common import DATA, before_local_time, from_local_time, message
    from stream.heartbeat import Heartbeat

    cfg = day["cfg"]
    raw = pl.read_csv("data/inbox/positions_2026-11-19.csv.gz").sort("fix_time_utc")
    win = before_local_time(from_local_time(raw, WINDOW[0]), WINDOW[1])
    dl = day["best"].filter("on_network").select("device_id", "line")

    # batch rules on exactly the same points
    pts = with_xy(normalise(win, cfg["timezone"]), plane())
    batch = detect(zone_points(pts, day["lines"], dl, cfg), cfg)
    assert batch.filter(pl.col("kind") == "trip").height > 20      # the window is not trivially empty

    # fresh topics and group
    tag = uuid.uuid4().hex[:8]
    src, dst = f"e2e.positions.{tag}", f"e2e.trip-events.{tag}"
    p = Producer({"bootstrap.servers": BROKER, "linger.ms": 20})
    for r in win.iter_rows(named=True):
        k, v = message(r["device_id"], r["fix_time_utc"], r["lat"], r["lon"], r["speed_kn"])
        while True:
            try:
                p.produce(src, key=k, value=v)
                break
            except BufferError:
                p.poll(0.2)
    assert p.flush(30) == 0

    # the live app, with the same device -> line assignment the batch used
    lt.LINES = day["lines"]
    lt.ASSIGN = {int(r["device_id"]): {"bus": str(r["device_id"]), "line": r["line"]} for r in dl.iter_rows(named=True)}
    lt.HB = Heartbeat(f"e2e-{tag}")
    lt.append_events = lambda events: None           # keep the demo's event file untouched
    lt.COUNT.update(processed=0, trips=0, excursions=0)
    group = f"e2e-{tag}"
    app, _ = lt.build_app(group, "earliest", src, dst, verbose=False)
    admin = AdminClient({"bootstrap.servers": BROKER})
    try:
        app.run(timeout=15)                          # stops once no new message arrives for 15 s
        events = _consume_all(dst)
    finally:
        lt.HB.path.unlink(missing_ok=True)
        mine = [t for t in admin.list_topics(timeout=5).topics if tag in t]   # ours plus Quix's changelog
        admin.delete_topics(mine, operation_timeout=10)
        shutil.rmtree(DATA / "quix-state" / group, ignore_errors=True)

    assert lt.COUNT["processed"] == win.height       # every message went through the Kafka consumer
    live = (pl.DataFrame(events).select("device_id", pl.col("event").alias("kind"), "frm", "to", "dep", "arr")
            .sort(["device_id", "dep", "arr"]))
    ref = batch.select("device_id", "kind", "frm", "to", "dep", "arr").sort(["device_id", "dep", "arr"])
    assert live.height == ref.height
    assert live.equals(ref.cast(live.schema))
