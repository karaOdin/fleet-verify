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

WH = DATA / "warehouse"
STATUS = {"ok": "✅ مؤكدة", "ok_dep": "✅ الانطلاق مؤكد", "diff": "⚠️ الوقت مختلف",
          "dup": "🔁 بلا رحلة مستقلة", "missing": "❌ غير موجودة", "extra": "➕ بلا تصريح", "nogps": "— بلا GPS"}

st.set_page_config(page_title="تحقق الأسطول", layout="wide")
daily, live = st.tabs(["التقرير اليومي", "مباشر"])


def days():
    d = WH / "bus_day_report"
    return sorted((p.stem for p in d.glob("*.parquet")), reverse=True) if d.exists() else []


with daily:
    ds = days()
    if not ds:
        st.info("لا توجد تقارير بعد. شغّل المهمة اليومية في Dagster أولًا.")
    else:
        day = st.selectbox("اليوم", ds)
        rep = pl.read_parquet(WH / "bus_day_report" / f"{day}.parquet")
        ver = pl.read_parquet(WH / "trip_verification" / f"{day}.parquet")
        km = pl.read_parquet(WH / "km_report" / f"{day}.parquet")
        ident = pl.read_parquet(WH / "device_identity" / f"{day}.parquet")
        decl = ver.filter(pl.col("paper_id").is_not_null())
        c = dict(ver.group_by("status").len().iter_rows())
        cols = st.columns(6)
        cols[0].metric("رحلات مصرّح بها", decl.height)
        cols[1].metric("مؤكدة", c.get("ok", 0) + c.get("ok_dep", 0))
        cols[2].metric("الوقت مختلف", c.get("diff", 0))
        cols[3].metric("غير موجودة", c.get("missing", 0))
        cols[4].metric("كم كلي", f"{km['total_km'].sum():,.0f}")
        cols[5].metric("تجاري", f"{100 * km['commercial_km'].sum() / km['total_km'].sum():.0f}%")

        st.subheader("كل حافلة")
        st.dataframe(rep.drop("work_date", "settings_hash").sort("bus"), width="stretch", hide_index=True)

        st.subheader("رحلات حافلة")
        bus = st.selectbox("الحافلة", sorted(ver["bus"].unique().to_list()))
        b = (ver.filter(pl.col("bus") == bus)
             .select(pl.col("paper_id").alias("الورقة"), pl.col("shift").alias("الفترة"), pl.col("direction").alias("المكتوب"),
                     pl.format("{} → {}", "dep_time", "arr_time").alias("المصرّح به"),
                     pl.format("{} → {}", "gps_dep_time", "gps_arr_time").alias("GPS"),
                     pl.col("actual_direction").alias("الاتجاه الفعلي"),
                     pl.col("delta_dep_min").alias("فرق الانطلاق"), pl.col("delta_arr_min").alias("فرق الوصول"),
                     pl.col("tickets").alias("التذاكر"), pl.col("status").replace_strict(STATUS).alias("الحالة")))
        st.dataframe(b, width="stretch", hide_index=True)

        st.subheader("هوية الحافلات (ER)")
        st.dataframe(ident.filter(pl.col("verdict") != "confirmed").drop("work_date", "settings_hash"),
                     width="stretch", hide_index=True)

with live:
    st.caption(f"Kafka {BROKER} · topic {TRIP_EVENTS}")
    if st.toggle("تشغيل", value=False):
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
            df = pl.DataFrame(ev).select("day", "bus", "event", "dep_time", "from_name", "arr_time", "to_name", "minutes")
            st.metric("أنصاف رحلات اليوم", sum(e["event"] == "trip" for e in st.session_state.events))
            st.dataframe(df.reverse(), width="stretch", hide_index=True)
        else:
            st.info("لا أحداث بعد. شغّل stream.replay أو stream.traccar_poll ثم stream.live_trips.")
        time.sleep(2)
        st.rerun()
