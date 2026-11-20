"""The tables the dashboard shows, as plain functions (tested in tests/test_dashboard.py).

Privacy, until the operator agrees otherwise: buses by the pipeline page's labels (Bus A, B, ...; viz/public.py),
other buses named in a row too; no receiver, no Traccar device name (registered_device, alt_device hold names
such as "17/H014"), no paper notes. Paper ids and device ids may stay. Place names in English (name_en).
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

from fleet.settings import place_en
from viz.public import day_results, label_order, show_bus

STATUS = {"ok": "confirmed", "ok_dep": "departure confirmed", "diff": "time differs",
          "dup": "no separate trip", "missing": "not found", "extra": "not declared", "nogps": "no GPS"}
# columns that may name a person or a device: never shown
HIDDEN = {"registered_device", "alt_device", "receiver", "receiver_id", "receiver_name", "notes", "note",
          "work_date", "settings_hash"}


def days(wh: Path) -> list[str]:
    d = wh / "bus_day_report"
    return sorted((p.stem for p in d.glob("*.parquet")), reverse=True) if d.exists() else []


def _bus(df: pl.DataFrame, *cols: str) -> pl.DataFrame:
    return df.with_columns(pl.col(c).map_elements(show_bus, return_dtype=pl.Utf8) for c in cols if c in df.columns)


def _visible(df: pl.DataFrame) -> pl.DataFrame:
    return df.drop([c for c in df.columns if c in HIDDEN])


def _by_label(df: pl.DataFrame, col: str = "Bus") -> pl.DataFrame:
    return df.with_columns(pl.col(col).map_elements(lambda s: label_order(s)[0], return_dtype=pl.Int64).alias("_n")) \
             .sort(["_n", col]).drop("_n")


def metrics(wh: Path, day: str) -> list[tuple[str, str]] | None:
    """The six numbers of "Result of the day" on the pipeline page, formatted the same way."""
    r = day_results(wh, day)
    if r is None:
        return None
    st = r["status"]
    pct = lambda v: "—" if v is None else f"{v}%"
    km = "—" if r["total_km"] is None else f"{r['total_km']:,.1f}".rstrip("0").rstrip(".")
    return [("Declared trips", f"{r['declared']:,}"), ("Confirmed by GPS", pct(r["confirmed_pct"])),
            ("Time differs", f"{st.get('diff', 0):,}"), ("Not found", f"{st.get('missing', 0):,}"),
            ("Km driven", km), ("Commercial km", pct(r["commercial_pct"]))]


def every_bus(wh: Path, day: str) -> pl.DataFrame:
    rep = _visible(_bus(pl.read_parquet(wh / "bus_day_report" / f"{day}.parquet"), "bus"))
    rep = rep.rename({"bus": "Bus", "paper_lines": "Paper lines", "resolved_line": "Line driven", "verdict": "Identity",
                      "needs_review": "Review", "declared": "Declared", "ok": "Confirmed", "ok_dep": "Departure only",
                      "diff": "Time differs", "dup": "No separate trip", "missing": "Not found", "extra": "Not declared",
                      "nogps": "No GPS", "direction_differs": "Direction differs", "total_km": "Km",
                      "commercial_km": "Commercial km", "commercial_pct": "Commercial %",
                      "waiting_off_line_km": "Km waiting off line", "confirmed_share": "Confirmed share"}, strict=False)
    return _by_label(rep)


def bus_labels(wh: Path, day: str) -> list[str]:
    v = pl.read_parquet(wh / "trip_verification" / f"{day}.parquet", columns=["bus"])
    return sorted({show_bus(b) for b in v["bus"].unique().to_list() if b is not None}, key=label_order)


def trips_of(wh: Path, day: str, label: str) -> pl.DataFrame:
    v = _bus(pl.read_parquet(wh / "trip_verification" / f"{day}.parquet"), "bus").filter(pl.col("bus") == label)
    return v.sort("dep_time", nulls_last=True).select(
        pl.col("paper_id").alias("Paper"), pl.col("shift").alias("Shift"), pl.col("trip_number").alias("Trip"),
        pl.col("direction").alias("Written direction"),
        pl.format("{} → {}", "dep_time", "arr_time").alias("Declared"),
        pl.format("{} → {}", "gps_dep_time", "gps_arr_time").alias("GPS"),
        pl.col("actual_direction").map_elements(place_en, return_dtype=pl.Utf8).alias("Actual direction"),
        pl.col("delta_dep_min").alias("Departure diff (min)"), pl.col("delta_arr_min").alias("Arrival diff (min)"),
        pl.col("tickets").alias("Tickets"), pl.col("device_id").alias("Device id"),
        pl.col("status").replace_strict(STATUS, default=pl.col("status")).alias("Status"))


def identity_review(wh: Path, day: str) -> pl.DataFrame:
    """Buses whose device is not simply confirmed (ER), with no device names."""
    ident = pl.read_parquet(wh / "device_identity" / f"{day}.parquet").filter(pl.col("verdict") != "confirmed")
    ident = _visible(_bus(ident, "bus", "used_by"))
    ident = ident.rename({"bus": "Bus", "registry_line": "Registry line", "paper_lines": "Paper lines", "papers": "Papers",
                          "declared": "Declared", "registered_device_id": "Registered device id",
                          "registered_status": "Device status", "registered_gps_line": "Line it drove",
                          "registered_route_fit": "Route fit", "registered_trip_match": "Trip match",
                          "alt_trip_match": "Other device match", "alt_runner_up": "Runner-up",
                          "resolved_device_id": "Resolved device id", "resolved_line": "Resolved line",
                          "verdict": "Verdict", "needs_review": "Review", "paper_line_differs": "Paper line differs",
                          "used_by": "Device used by"}, strict=False)
    return _by_label(ident)


def live_events(events: list[dict]) -> pl.DataFrame:
    """Trip events from Kafka (fleet.trip-events), anonymised and in English."""
    if not events:
        return pl.DataFrame()
    df = pl.DataFrame(events).select("day", "bus", "event", "dep_time", "from_name", "arr_time", "to_name", "minutes")
    df = _bus(df, "bus").with_columns(pl.col("from_name", "to_name").map_elements(place_en, return_dtype=pl.Utf8))
    return df.rename({"day": "Day", "bus": "Bus", "event": "Event", "dep_time": "Departure", "from_name": "From",
                      "arr_time": "Arrival", "to_name": "To", "minutes": "Minutes"})
