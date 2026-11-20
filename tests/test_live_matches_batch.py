"""The streaming state machine must find exactly the trips the batch finds."""
import polars as pl

from fleet.live import init_state, process


def test_live_equals_batch(day):
    cfg, lines = day["cfg"], day["lines"]
    h0, h1 = cfg["service_hours"]
    dl = day["best"].filter("on_network").select("device_id", "line")
    stream = (day["pos"].join(dl, on="device_id")
              .filter(pl.col("t").dt.hour().is_between(h0, h1))
              .sort("t", maintain_order=True))          # interleaved as Kafka would deliver them
    states, events = {}, []
    for r in stream.iter_rows(named=True):
        st = states.setdefault(r["device_id"], init_state())
        events += process(st, r, lines[r["line"]], (r["x"], r["y"]), cfg)
    live = pl.DataFrame(events).select("device_id", pl.col("event").alias("kind"), "frm", "to", "dep", "arr").sort(["device_id", "dep", "arr"])
    batch = day["trips"].select("device_id", "kind", "frm", "to", "dep", "arr").sort(["device_id", "dep", "arr"])
    assert live.height == batch.height
    assert live.equals(batch.cast(live.schema))
