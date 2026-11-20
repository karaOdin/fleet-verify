"""The daily verification as Dagster assets, one partition per local work day.

  lines ─┐                         ┌─ road_papers ──────────────┐
         ├─ route_fit ─ gps_trips ─┼─ device_identity (ER) ─────┼─ trip_verification ─┐
raw_positions ─────────────────────┘        │                   │                     ├─ bus_day_report ─ postgres_export
fleet_registry, traccar_devices ────────────┘                   └─ km_report ─────────┘
"""

import json
import os
from datetime import date
from pathlib import Path

import polars as pl
from dagster import (AssetCheckResult, AssetCheckSeverity, AssetCheckSpec, AssetExecutionContext,
                     DailyPartitionsDefinition, MetadataValue, Output, asset)

from fleet.identity import resolve
from fleet.km import km_by_category
from fleet.lines import build_lines, lines_frame
from fleet.match import verify
from fleet.papers import flatten
from fleet.positions import normalise, with_xy
from fleet.route_fit import best_line, route_fit as fit_routes
from fleet.settings import ROOT, plane, settings, settings_hash, terminals
from fleet.trips import detect, zone_points
from sources import cloudroutes, traccar

CFG = settings()
DAILY = DailyPartitionsDefinition(start_date="2026-11-01", timezone=CFG["timezone"])
DATA = Path(os.environ.get("FLEET_DATA", ROOT / "data"))
STAMP = lambda df, day: df.with_columns(pl.lit(day).alias("work_date"), pl.lit(settings_hash()).alias("settings_hash"))


def _positions(raw: pl.DataFrame) -> pl.DataFrame:
    return with_xy(normalise(raw, CFG["timezone"]), plane())


# ---------- reference data (not partitioned) ----------

@asset(group_name="reference", description="Bus lines from CloudRoutes: route polyline and both terminal geofences.")
def lines() -> pl.DataFrame:
    return lines_frame(cloudroutes.lines())


@asset(group_name="reference", description="CloudRoutes fleet registry: bus, assigned line, configured Traccar device.")
def fleet_registry() -> pl.DataFrame:
    return pl.DataFrame(cloudroutes.registry(), schema={"bus": pl.Utf8, "line": pl.Utf8, "traccar_device_id": pl.Int64})


@asset(group_name="reference", description="Traccar devices (id, name, status).")
def traccar_devices() -> pl.DataFrame:
    rows = traccar.devices() if traccar.configured() else json.loads((DATA / "inbox" / "devices.json").read_text())
    return pl.DataFrame(rows).select("id", "name", "status")


# ---------- daily inputs ----------

@asset(partitions_def=DAILY, group_name="daily_inputs",
       check_specs=[AssetCheckSpec("all_devices_fetched", asset="raw_positions",
                                   description="Warns when the Traccar API failed for some devices.")],
       description="Every GPS fix of the local day. Read from the Kafka lake if the stream wrote it, "
                   "else from an export in data/inbox, else fetched from the Traccar API.")
def raw_positions(context: AssetExecutionContext, traccar_devices: pl.DataFrame) -> pl.DataFrame:
    day = context.partition_key
    lake = DATA / "lake" / "positions" / f"date={day}"
    inbox = DATA / "inbox" / f"positions_{day}.csv.gz"
    failed: list[int] = []
    use_lake = os.environ.get("FLEET_POSITIONS_PREFER", "lake") == "lake" or not inbox.exists()
    if use_lake and lake.exists() and any(lake.glob("*.parquet")):
        raw, source = pl.read_parquet(lake / "*.parquet"), "kafka lake"
        raw = raw.with_columns(pl.col("fix_time_utc").cast(pl.Utf8)) if raw.schema["fix_time_utc"] != pl.Utf8 else raw
        raw = raw.unique(["device_id", "fix_time_utc"], keep="first")
    elif inbox.exists():
        raw, source = pl.read_csv(inbox), "inbox export"
    elif traccar.configured():
        raw, failed = traccar.positions_day(traccar_devices["id"].to_list(), date.fromisoformat(day), CFG["timezone"])
        source = "traccar api"
    else:
        raise FileNotFoundError(f"No positions for {day}: no lake partition, no {inbox.name}, Traccar not configured")
    yield Output(raw.select("device_id", "fix_time_utc", "lat", "lon", "speed_kn"),
                 metadata={"source": source, "devices": raw["device_id"].n_unique(), "failed_devices": MetadataValue.json(failed)})
    yield AssetCheckResult(check_name="all_devices_fetched", passed=not failed, severity=AssetCheckSeverity.WARN,
                           metadata={"failed_devices": MetadataValue.json(failed)})


@asset(partitions_def=DAILY, group_name="daily_inputs",
       description="Road papers of the day, one row per declared trip (papers without trips keep one empty row).")
def road_papers(context: AssetExecutionContext) -> pl.DataFrame:
    df = flatten(cloudroutes.road_papers(context.partition_key))
    context.add_output_metadata({"papers": df["paper_id"].n_unique(), "declared_trips": df["dep"].is_not_null().sum(),
                                 "buses": df["bus"].n_unique()})
    return df


# ---------- daily analysis ----------

@asset(partitions_def=DAILY, group_name="daily_analysis",
       description="For every device and line: how much of the device's driving lies on the route, and how much of the route it covered.")
def route_fit(context: AssetExecutionContext, raw_positions: pl.DataFrame, lines: pl.DataFrame) -> pl.DataFrame:
    fit = fit_routes(_positions(raw_positions), build_lines(lines, plane()), CFG)
    return STAMP(fit, context.partition_key)


@asset(partitions_def=DAILY, group_name="daily_analysis",
       description="Half-trips, excursions and loops detected from GPS on each device's best-fitting line.")
def gps_trips(context: AssetExecutionContext, raw_positions: pl.DataFrame, lines: pl.DataFrame, route_fit: pl.DataFrame) -> pl.DataFrame:
    best = best_line(route_fit.drop("work_date", "settings_hash"), CFG["route_fit"]["min_f"])
    pts = zone_points(_positions(raw_positions), build_lines(lines, plane()),
                      best.filter("on_network").select("device_id", "line"), CFG)
    trips = detect(pts, CFG)
    context.add_output_metadata({"devices": trips["device_id"].n_unique(),
                                 "half_trips": trips.filter(pl.col("kind") == "trip").height})
    return STAMP(trips, context.partition_key)


@asset(partitions_def=DAILY, group_name="daily_analysis",
       check_specs=[AssetCheckSpec("one_bus_per_device", asset="device_identity",
                                   description="A device is used for at most one bus.")],
       description="Entity resolution: which device drove each bus today, from the registry, the papers and GPS behaviour.")
def device_identity(context: AssetExecutionContext, fleet_registry: pl.DataFrame, traccar_devices: pl.DataFrame,
                    route_fit: pl.DataFrame, gps_trips: pl.DataFrame, road_papers: pl.DataFrame) -> pl.DataFrame:
    best = best_line(route_fit.drop("work_date", "settings_hash"), CFG["route_fit"]["min_f"])
    ident = resolve(fleet_registry, traccar_devices, best, gps_trips.drop("work_date", "settings_hash"), road_papers, CFG)
    counts = dict(ident.group_by("verdict").len().iter_rows())
    yield Output(STAMP(ident, context.partition_key),
                 metadata={"verdicts": MetadataValue.json(counts),
                           "needs_review": MetadataValue.json(ident.filter("needs_review")["bus"].to_list())})
    r = ident.drop_nulls("resolved_device_id")
    dup = r.filter(pl.col("resolved_device_id").is_duplicated())["bus"].to_list()
    yield AssetCheckResult(check_name="one_bus_per_device", passed=not dup, metadata={"buses_sharing_a_device": MetadataValue.json(dup)})


@asset(partitions_def=DAILY, group_name="daily_reports",
       check_specs=[AssetCheckSpec("every_declared_trip_checked", asset="trip_verification",
                                   description="Every declared trip of the day has a verification row.")],
       description="Every declared trip against the GPS trips of the bus's resolved device; plus GPS trips nobody declared.")
def trip_verification(context: AssetExecutionContext, road_papers: pl.DataFrame, gps_trips: pl.DataFrame,
                      device_identity: pl.DataFrame) -> pl.DataFrame:
    v = verify(road_papers, gps_trips.drop("work_date", "settings_hash"), device_identity, terminals(), CFG)
    yield Output(STAMP(v, context.partition_key), metadata={"status": MetadataValue.json(dict(v.group_by("status").len().iter_rows()))})
    declared, checked = int(road_papers["dep"].is_not_null().sum()), int(v["paper_id"].is_not_null().sum())
    yield AssetCheckResult(check_name="every_declared_trip_checked", passed=declared == checked,
                           metadata={"declared": declared, "checked": checked})


@asset(partitions_def=DAILY, group_name="daily_reports",
       check_specs=[AssetCheckSpec("km_categories_add_up", asset="km_report",
                                   description="The five kilometre categories add up to the total for every device.")],
       description="Commercial and non-commercial kilometres per device, labelled with the bus it served.")
def km_report(context: AssetExecutionContext, raw_positions: pl.DataFrame, gps_trips: pl.DataFrame,
              device_identity: pl.DataFrame) -> pl.DataFrame:
    km = km_by_category(_positions(raw_positions), gps_trips.drop("work_date", "settings_hash"), CFG["max_speed_kmh"])
    bus_of = device_identity.drop_nulls("resolved_device_id").select(pl.col("resolved_device_id").alias("device_id"), "bus")
    km = km.join(bus_of, on="device_id", how="left")
    yield Output(STAMP(km, context.partition_key),
                 metadata={"total_km": round(km["total_km"].sum(), 1), "commercial_km": round(km["commercial_km"].sum(), 1)})
    cats = ["commercial_km", "depot_to_first_km", "waiting_off_line_km", "at_terminals_km", "last_to_depot_km"]
    gap = float((km["total_km"] - sum(km[c] for c in cats)).abs().max() or 0) if km.height else 0.0
    yield AssetCheckResult(check_name="km_categories_add_up", passed=gap <= 0.5, metadata={"max_gap_km": gap})


@asset(partitions_def=DAILY, group_name="daily_reports",
       description="One line per bus: identity verdict, trips declared and confirmed, kilometres.")
def bus_day_report(context: AssetExecutionContext, device_identity: pl.DataFrame, trip_verification: pl.DataFrame,
                   km_report: pl.DataFrame) -> pl.DataFrame:
    v = trip_verification.group_by("bus").agg(
        pl.col("paper_id").is_not_null().sum().alias("declared"),
        *[(pl.col("status") == s).sum().alias(s) for s in ["ok", "ok_dep", "diff", "dup", "missing", "extra", "nogps"]],
        pl.col("direction_differs").sum().alias("direction_differs"))
    k = km_report.drop_nulls("bus").select("bus", "total_km", "commercial_km", "commercial_pct", "waiting_off_line_km")
    r = (device_identity.select("bus", "paper_lines", "resolved_line", "verdict", "needs_review")
         .join(v, on="bus", how="left").join(k, on="bus", how="left")
         .with_columns(((pl.col("ok") + pl.col("ok_dep")) / (pl.col("declared") - pl.col("nogps"))).round(2).alias("confirmed_share")))
    return STAMP(r, context.partition_key)


@asset(partitions_def=DAILY, group_name="publish",
       description="Writes the day's reports to PostgreSQL (replaces the day). Skipped when POSTGRES_URL is not set.")
def postgres_export(context: AssetExecutionContext, bus_day_report: pl.DataFrame, trip_verification: pl.DataFrame,
                    km_report: pl.DataFrame, device_identity: pl.DataFrame) -> pl.DataFrame:
    from pipeline.pg import replace_day
    url = os.environ.get("POSTGRES_URL")
    tables = {"bus_day_report": bus_day_report, "trip_verification": trip_verification,
              "km_report": km_report, "device_identity": device_identity}
    if not url:
        context.log.warning("POSTGRES_URL not set: reports stay in data/warehouse only")
        written = {k: 0 for k in tables}
    else:
        written = {k: replace_day(url, k, df, context.partition_key) for k, df in tables.items()}
    return pl.DataFrame({"table": list(written), "rows": list(written.values())})
