"""Fleet verification dashboard.

    streamlit run dashboard/app.py

Daily tab: the reports Dagster wrote to data/warehouse.
Live tab: trip events arriving on Kafka (fleet.trip-events).
"""
import json
import sys
import time
from pathlib import Path

import polars as pl
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from stream.common import BROKER, DATA, TRIP_EVENTS  # noqa: E402
from fleet.settings import place_en  # noqa: E402

WH = DATA / "warehouse"
STATUS = {"ok": "✅ confirmed", "ok_dep": "✅ departure confirmed", "diff": "⚠️ time differs",
          "dup": "🔁 no separate trip", "missing": "❌ not found", "extra": "➕ not declared", "nogps": "— no GPS"}

st.set_page_config(page_title="Fleet verification", layout="wide")
daily, live = st.tabs(["Daily report", "Live"])


def days():
    d = WH / "bus_day_report"
    return sorted((p.stem for p in d.glob("*.parquet")), reverse=True) if d.exists() else []


with daily:
    ds = days()
    if not ds:
        st.info("No reports yet. Run the daily job in Dagster first.")
    else:
        day = st.selectbox("Day", ds)
        rep = pl.read_parquet(WH / "bus_day_report" / f"{day}.parquet")
        ver = pl.read_parquet(WH / "trip_verification" / f"{day}.parquet")
        km = pl.read_parquet(WH / "km_report" / f"{day}.parquet")
        ident = pl.read_parquet(WH / "device_identity" / f"{day}.parquet")
        decl = ver.filter(pl.col("paper_id").is_not_null())
        c = dict(ver.group_by("status").len().iter_rows())
        cols = st.columns(6)
        cols[0].metric("Declared trips", decl.height)
        cols[1].metric("Confirmed", c.get("ok", 0) + c.get("ok_dep", 0))
        cols[2].metric("Time differs", c.get("diff", 0))
        cols[3].metric("Not found", c.get("missing", 0))
        cols[4].metric("Total km", f"{km['total_km'].sum():,.0f}")
        cols[5].metric("Commercial", f"{100 * km['commercial_km'].sum() / km['total_km'].sum():.0f}%")

        st.subheader("Every bus")
        st.dataframe(rep.drop("work_date", "settings_hash").sort("bus"), width="stretch", hide_index=True)

        st.subheader("Trips of one bus")
        bus = st.selectbox("Bus", sorted(ver["bus"].unique().to_list()))
        b = (ver.filter(pl.col("bus") == bus)
             .select(pl.col("paper_id").alias("Paper"), pl.col("shift").alias("Shift"), pl.col("direction").alias("Written direction"),
                     pl.format("{} → {}", "dep_time", "arr_time").alias("Declared"),
                     pl.format("{} → {}", "gps_dep_time", "gps_arr_time").alias("GPS"),
                     pl.col("actual_direction").map_elements(place_en, return_dtype=pl.Utf8).alias("Actual direction"),
                     pl.col("delta_dep_min").alias("Departure diff (min)"), pl.col("delta_arr_min").alias("Arrival diff (min)"),
                     pl.col("tickets").alias("Tickets"), pl.col("status").replace_strict(STATUS).alias("Status")))
        st.dataframe(b, width="stretch", hide_index=True)

        st.subheader("Bus identity (ER)")
        st.dataframe(ident.filter(pl.col("verdict") != "confirmed").drop("work_date", "settings_hash"),
                     width="stretch", hide_index=True)

with live:
    st.caption(f"Kafka {BROKER} · topic {TRIP_EVENTS}")
    if st.toggle("Run", value=False):
        from confluent_kafka import Consumer, TopicPartition

        if "events" not in st.session_state:
            st.session_state.events = []
            c = Consumer({"bootstrap.servers": BROKER, "group.id": f"dashboard-{time.time_ns()}",
                          "auto.offset.reset": "earliest", "enable.auto.commit": False})
            c.subscribe([TRIP_EVENTS])
            st.session_state.consumer = c
        c = st.session_state.consumer
        for _ in range(500):
            m = c.poll(0.05)
            if m is None:
                break
            if not m.error():
                st.session_state.events.append(json.loads(m.value()))
        ev = st.session_state.events[-200:]
        if ev:
            df = (pl.DataFrame(ev).select("day", "bus", "event", "dep_time", "from_name", "arr_time", "to_name", "minutes")
                  .with_columns(pl.col("from_name", "to_name").map_elements(place_en, return_dtype=pl.Utf8)))
            st.metric("Half-trips today", sum(e["event"] == "trip" for e in st.session_state.events))
            st.dataframe(df.reverse(), width="stretch", hide_index=True)
        else:
            st.info("No events yet. Start stream.replay or stream.traccar_poll, then stream.live_trips.")
        time.sleep(2)
        st.rerun()
