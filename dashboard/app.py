"""Fleet verification dashboard.

    streamlit run dashboard/app.py

Daily tab: the reports Dagster wrote to data/warehouse (the same warehouse as the batch).
Live tab: trip events arriving on Kafka (fleet.trip-events).
Buses are shown as on the pipeline page (Bus A, B, ...): see dashboard/views.py for what is never shown.
"""
import json
import sys
import time
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from stream.common import BROKER, DATA, TRIP_EVENTS  # noqa: E402
from dashboard import views  # noqa: E402

WH = DATA / "warehouse"

st.set_page_config(page_title="Fleet verification", layout="wide")
# readable on a projector: bigger metrics and headings (Streamlit's own theme sets the base size)
st.markdown("""<style>
[data-testid="stMetricValue"] { font-size: 3.2rem; font-family: "IBM Plex Mono", ui-monospace, monospace; }
[data-testid="stMetricLabel"] p { font-size: 1.15rem; }
h1 { font-size: 2.6rem !important; } h2, h3 { font-size: 1.9rem !important; }
</style>""", unsafe_allow_html=True)

st.title("Fleet verification")
daily, live = st.tabs(["Daily report", "Live"])

with daily:
    ds = views.days(WH)
    if not ds:
        @st.fragment(run_every="5s")      # only this box re-runs; the page fills in when the batch finishes
        def wait_for_batch():
            if views.days(WH):            # the batch's last output (bus_day_report) is there
                st.rerun()
            st.info("No results yet. Run the batch first (the pipeline page’s “Run the batch” button).")

        wait_for_batch()
    else:
        day = st.selectbox("Day", ds)
        cols = st.columns(6)
        for col, (label, value) in zip(cols, views.metrics(WH, day) or []):
            col.metric(label, value)

        st.subheader("Every bus")
        st.dataframe(views.every_bus(WH, day), width="stretch", hide_index=True)

        st.subheader("Trips of one bus")
        bus = st.selectbox("Bus", views.bus_labels(WH, day))
        st.dataframe(views.trips_of(WH, day, bus), width="stretch", hide_index=True)

        st.subheader("Bus identity (ER): devices to review")
        st.caption("Which device really drove each bus. Not a sanction: a person reviews each case.")
        st.dataframe(views.identity_review(WH, day), width="stretch", hide_index=True)

with live:
    st.caption(f"Kafka {BROKER} · topic {TRIP_EVENTS}")
    if st.toggle("Run", value=False):
        from confluent_kafka import Consumer

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
            st.metric("Half-trips today", sum(e["event"] == "trip" for e in st.session_state.events))
            st.dataframe(views.live_events(ev).reverse(), width="stretch", hide_index=True)
        else:
            st.info("No events yet. Start the replay on the pipeline page.")
        time.sleep(2)
        st.rerun()
