"""Replays a day point by point through the live rules (trips and alerts), interleaved in time
order as Kafka would deliver them. Shared by tests/test_alerts.py and tools/alerts_report.py."""
from __future__ import annotations

import polars as pl

from fleet import alerts
from fleet.live import init_state, process


def replay_alerts(day: dict, pos: pl.DataFrame | None = None, partition_of: dict | None = None) -> list[dict]:
    """pos: positions with x/y (default: the whole day). partition_of: device -> Kafka partition,
    for one stream clock per partition as live_trips keeps them (default: one clock)."""
    cfg, lines = day["cfg"], day["lines"]
    h0, h1 = cfg["service_hours"]
    dl = day["best"].filter("on_network").select("device_id", "line")
    stream = ((day["pos"] if pos is None else pos).join(dl, on="device_id")
              .filter(pl.col("t").dt.hour().is_between(h0, h1))
              .sort("t", maintain_order=True))
    trip_st, alert_st, clocks, out = {}, {}, {}, []
    for r in stream.iter_rows(named=True):
        ln = lines[r["line"]]
        xy = (r["x"], r["y"])
        process(trip_st.setdefault(r["device_id"], init_state()), r, ln, xy, cfg)
        a = alert_st.setdefault(r["device_id"], alerts.init_state())
        out += alerts.process(a, r, ln, xy, cfg)
        clock = clocks.setdefault((partition_of or {}).get(r["device_id"], 0), alerts.clock_init())
        out += alerts.observe(clock, r, ln.code, a, cfg)
        out += alerts.check_silent(clock, r["s"], cfg)
    return out
