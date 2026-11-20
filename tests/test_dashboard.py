"""The Streamlit dashboard on 19 Nov: the same six numbers as the pipeline page, the same bus labels, and
nothing it must not show (real bus numbers, device names, receivers, Arabic). Every cell of every table
is checked, not only what fits on a screen."""
import json
import re

import polars as pl
import pytest

from dashboard import views
from viz import public, server

DAY = "2026-11-19"
ARABIC = re.compile(r"[؀-ۿ]")


@pytest.fixture(scope="module")
def wh(day, tmp_path_factory):
    from dagster import build_asset_context
    from pipeline.assets import bus_day_report

    w = tmp_path_factory.mktemp("warehouse")
    stamp = lambda df: df.with_columns(pl.lit(DAY).alias("work_date"), pl.lit("test").alias("settings_hash"))
    ver, ident = stamp(day["ver"]), stamp(day["ident"])
    bus_of = day["ident"].drop_nulls("resolved_device_id").select(pl.col("resolved_device_id").alias("device_id"), "bus")
    km = stamp(day["km"].join(bus_of, on="device_id", how="left"))               # as the km_report asset does
    rep = bus_day_report(build_asset_context(partition_key=DAY), device_identity=ident, trip_verification=ver, km_report=km)
    for name, df in {"trip_verification": ver, "device_identity": ident, "km_report": km, "bus_day_report": rep}.items():
        (w / name).mkdir()
        df.write_parquet(w / name / f"{DAY}.parquet")
    return w


@pytest.fixture(scope="module")
def real_buses(day) -> set[str]:
    from sources.cloudroutes import registry
    names = {str(b["bus"]) for b in registry()} | {str(b) for b in day["ident"]["bus"].drop_nulls()}
    return names | {str(b) for b in day["papers"]["bus"].drop_nulls()} if "bus" in day["papers"].columns else names


@pytest.fixture(scope="module")
def device_names(day) -> set[str]:
    i = day["ident"]
    return {str(x) for c in ("registered_device", "alt_device") if c in i.columns for x in i[c].drop_nulls()}


def _all_tables(wh) -> dict[str, pl.DataFrame]:
    out = {"every_bus": views.every_bus(wh, DAY), "identity": views.identity_review(wh, DAY)}
    for label in views.bus_labels(wh, DAY):
        out[f"trips {label}"] = views.trips_of(wh, DAY, label)
    out["live"] = views.live_events([{"day": DAY, "bus": "22606", "event": "trip", "dep_time": "08:18", "from_name": "لاروكاد",
                                      "arr_time": "08:51", "to_name": "القطب الجامعي", "minutes": 33, "device_id": 130}])
    return out


def test_six_numbers_equal_the_pipeline_page(wh):
    assert views.metrics(wh, DAY) == [("Declared trips", "668"), ("Confirmed by GPS", "71%"), ("Time differs", "116"),
                                      ("Not found", "14"), ("Km driven", "6,907.5"), ("Commercial km", "75%")]
    page = public.day_results(wh, DAY)                  # what the page's "Result of the day" shows
    assert (page["declared"], page["confirmed_pct"], page["status"]["diff"], page["status"]["missing"],
            page["total_km"], page["commercial_pct"]) == (668, 71, 116, 14, 6907.5, 75)


def test_same_bus_labels_as_the_pipeline_page(wh):
    assert server.show_bus is public.show_bus
    trip = server._public_trip({"bus": "22606", "event": "trip"})
    assert trip["bus"] == public.show_bus("22606")
    labels = views.bus_labels(wh, DAY)
    assert labels and all(re.fullmatch(r"Bus [A-Z]+", x) for x in labels)
    assert views.trips_of(wh, DAY, public.show_bus("22606")).height > 0


def test_nothing_private_in_any_table(wh, real_buses, device_names):
    for name, df in _all_tables(wh).items():
        assert not (set(df.columns) & views.HIDDEN), name
        assert not any(re.search(r"receiver|device name|registered_device|alt_device|note", c, re.I) for c in df.columns), name
        text = df.write_csv()
        assert not ARABIC.search(text), name
        found = {b for b in real_buses if re.search(rf"(?<![\w.]){re.escape(b)}(?![\w.])", text)}
        assert not found, (name, sorted(found)[:5])
        assert not [d for d in device_names if d in text], name


def test_empty_before_the_batch(tmp_path):
    assert views.days(tmp_path) == []
    assert views.metrics(tmp_path, DAY) is None


def test_page_results_follow_km_report(tmp_path, monkeypatch, wh):
    """A result read while trip_verification exists but km_report not yet must update once km_report is written."""
    import shutil
    monkeypatch.setattr(server, "WH", tmp_path)
    server._results_cache.clear()
    for name in ("trip_verification", "device_identity"):
        shutil.copytree(wh / name, tmp_path / name)
    assert server._results(DAY)["total_km"] is None
    shutil.copytree(wh / "km_report", tmp_path / "km_report")
    assert server._results(DAY)["total_km"] == 6907.5
    server._results_cache.clear()
