"""Commercial and non-commercial kilometres per device, as table operations.

Each GPS step (point to next point) is assigned by its midpoint time:
inside a trip = commercial; inside an excursion = waiting off the line;
before the first trip = depot to first trip; after the last trip = back to
depot; anything else = moving around the terminals between trips.
Steps with no elapsed time or faster than `max_speed_kmh` are dropped.
"""
from __future__ import annotations

import polars as pl


def _inside(steps: pl.DataFrame, intervals: pl.DataFrame, name: str) -> pl.DataFrame:
    iv = intervals.select("device_id", pl.col("dep").cast(pl.Float64).alias("_d"),
                          pl.col("arr").cast(pl.Float64).alias(f"_a_{name}")).sort("_d")
    return (steps.sort("mid")
            .join_asof(iv, left_on="mid", right_on="_d", by="device_id", strategy="backward", check_sortedness=False)
            .with_columns((pl.col(f"_a_{name}").is_not_null() & (pl.col("mid") <= pl.col(f"_a_{name}"))).alias(name))
            .drop("_d", f"_a_{name}"))


def km_by_category(pos: pl.DataFrame, trips: pl.DataFrame, max_kmh: float) -> pl.DataFrame:
    devices = trips.select("device_id", "line").unique()
    steps = (
        pos.join(devices, on="device_id")
        .sort(["device_id", "t"], maintain_order=True)
        .with_columns(
            (pl.col("s") - pl.col("s").shift(1).over("device_id")).alias("dt"),
            ((pl.col("x") - pl.col("x").shift(1).over("device_id")).pow(2)
             + (pl.col("y") - pl.col("y").shift(1).over("device_id")).pow(2)).sqrt().alias("d"),
            ((pl.col("s") + pl.col("s").shift(1).over("device_id")) / 2).alias("mid"))
        .drop_nulls("dt")
        .with_columns(pl.when((pl.col("dt") > 0) & (pl.col("d") / pl.col("dt") * 3.6 <= max_kmh))
                        .then(pl.col("d")).otherwise(0.0).alias("d"))
        .select("device_id", "line", "mid", "d")
    )
    tr = trips.filter(pl.col("kind") == "trip")
    ex = trips.filter(pl.col("kind") == "excursion")
    bounds = tr.group_by("device_id").agg(pl.col("dep").min().alias("first_dep"), pl.col("arr").max().alias("last_arr"))
    steps = _inside(_inside(steps, tr, "in_trip"), ex, "in_exc")
    steps = steps.join(bounds, on="device_id", how="left").with_columns(
        pl.when(pl.col("in_trip")).then(pl.lit("commercial"))
          .when(pl.col("in_exc")).then(pl.lit("waiting_off_line"))
          .when(pl.col("first_dep").is_null() | (pl.col("mid") < pl.col("first_dep"))).then(pl.lit("depot_to_first"))
          .when(pl.col("mid") > pl.col("last_arr")).then(pl.lit("last_to_depot"))
          .otherwise(pl.lit("at_terminals")).alias("category"))
    cats = ["commercial", "depot_to_first", "waiting_off_line", "at_terminals", "last_to_depot"]
    km = lambda c: (pl.col("d").filter(pl.col("category") == c).sum() / 1000).round(1).alias(f"{c}_km")
    fmt = lambda c: pl.format("{}:{}", (pl.col(c) // 3600).cast(pl.Utf8).str.zfill(2),
                              (pl.col(c) % 3600 // 60).cast(pl.Utf8).str.zfill(2))
    return (
        steps.group_by(["device_id", "line"])
        .agg((pl.col("d").sum() / 1000).round(1).alias("total_km"), *[km(c) for c in cats])
        .join(bounds, on="device_id", how="left")
        .join(tr.group_by("device_id").len("half_trips"), on="device_id", how="left")
        .with_columns((100 * pl.col("commercial_km") / pl.col("total_km")).round(0).cast(pl.Int64).alias("commercial_pct"),
                      fmt("first_dep").alias("first_trip"), fmt("last_arr").alias("last_trip_end"),
                      pl.col("half_trips").fill_null(0))
        .drop("first_dep", "last_arr")
        .sort(["line", "device_id"])
    )
