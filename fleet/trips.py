"""Half-trips from GPS, as table operations.

1. Each point gets a zone: S terminal, E terminal or none. A point is in a
   terminal if it is within `terminal_radius_m` of the terminal centre, or on
   the route within the first/last few percent of its length (buses often turn
   before the drawn centre).
2. Consecutive points in the same terminal form an episode; a new episode
   starts when the zone changes or after `episode_gap_s` without a terminal point.
3. Between two consecutive episodes lies a segment. It is a trip when it joins
   different terminals, covers at least `trip_min_span` of the route and stays
   mostly on it; an excursion when most of it is off the route; otherwise a loop.
"""
from __future__ import annotations

import numpy as np
import polars as pl

from .geo import project
from .lines import Line

TRIPS_SCHEMA = {"device_id": pl.Int64, "line": pl.Utf8, "seq": pl.Int64, "kind": pl.Utf8,
                "frm": pl.Utf8, "to": pl.Utf8, "dep": pl.Int64, "arr": pl.Int64,
                "off_share": pl.Float64, "span": pl.Float64}


def zone_points(pos: pl.DataFrame, lines: dict[str, Line], device_line: pl.DataFrame, cfg: dict) -> pl.DataFrame:
    """Add route distance, progress and terminal zone to each point of each device
    that has a line. The projection is numpy; everything after is Polars."""
    h0, h1 = cfg["service_hours"]
    p0, p1 = cfg["terminal_progress"]
    R, ONR = cfg["terminal_radius_m"], cfg["on_route_m"]
    day = pos.filter(pl.col("t").dt.hour().is_between(h0, h1)).join(device_line, on="device_id")
    parts = []
    for (dev, code), g in day.group_by(["device_id", "line"], maintain_order=True):
        ln = lines[code]
        q = np.column_stack([g["x"].to_numpy(), g["y"].to_numpy()])
        off, along = project(q, ln.pts, ln.cum)
        parts.append(g.with_columns(pl.Series("off", off), pl.Series("along", along),
                                    pl.lit(ln.length).alias("L"),
                                    pl.Series("dS", np.hypot(*(q - ln.S).T)),
                                    pl.Series("dE", np.hypot(*(q - ln.E).T))))
    if not parts:
        return pl.DataFrame()
    pts = pl.concat(parts).sort(["device_id", "t"], maintain_order=True)
    on = pl.col("off") <= ONR
    return pts.with_columns(
        pl.int_range(pl.len()).over("device_id").alias("i"),
        on.alias("on"),
        pl.when((pl.col("dS") <= R) | (on & (pl.col("along") <= p0 * pl.col("L")))).then(1)
          .when((pl.col("dE") <= R) | (on & (pl.col("along") >= p1 * pl.col("L")))).then(2)
          .otherwise(0).alias("z"),
    )


def detect(pts: pl.DataFrame, cfg: dict) -> pl.DataFrame:
    if pts.is_empty():
        return pl.DataFrame(schema=TRIPS_SCHEMA)
    gap = cfg["episode_gap_s"]
    # 1. terminal episodes
    eps = (
        pts.filter(pl.col("z") > 0)
        .with_columns(
            ((pl.col("z") != pl.col("z").shift(1).over("device_id"))
             | ((pl.col("s") - pl.col("s").shift(1).over("device_id")) >= gap)
             | pl.col("z").shift(1).over("device_id").is_null()).alias("new"))
        .with_columns(pl.col("new").cum_sum().over("device_id").alias("ep"))
        .group_by(["device_id", "line", "ep"], maintain_order=True)
        .agg(pl.col("z").first(), pl.col("s").first().alias("s0"), pl.col("s").last().alias("s1"),
             pl.col("i").first().alias("i0"), pl.col("i").last().alias("i1"))
        .sort(["device_id", "ep"])
    )
    # 2. segments between consecutive episodes: from the last point of one to the first point of the next
    nxt = lambda c: pl.col(c).shift(-1).over("device_id")
    seg = (eps.with_columns(nxt("z").alias("z_to"), nxt("s0").alias("arr"), nxt("i0").alias("i_end"))
              .drop_nulls("z_to")
              .select("device_id", "line", pl.int_range(pl.len()).over("device_id").alias("seq"),
                      pl.col("z").alias("z_from"), "z_to", pl.col("s1").alias("dep"), "arr",
                      pl.col("i1").alias("i_start"), "i_end"))
    # 3. every point of every segment (a boundary point may belong to two segments)
    members = (seg.select("device_id", "seq", pl.int_ranges("i_start", pl.col("i_end") + 1).alias("i"))
                  .explode("i", empty_as_null=True)
                  .join(pts.select("device_id", "i", "on", "along", "L"), on=["device_id", "i"]))
    stats = members.group_by(["device_id", "seq"]).agg(
        (1 - pl.col("on").cast(pl.Float64).mean()).alias("off_share"),
        ((pl.col("along").filter(pl.col("on")).max() - pl.col("along").filter(pl.col("on")).min())
         / pl.col("L").first()).fill_null(0.0).alias("span"),
    )
    term = pl.when(pl.col("z_from") == 1).then(pl.lit("S")).otherwise(pl.lit("E"))
    term_to = pl.when(pl.col("z_to") == 1).then(pl.lit("S")).otherwise(pl.lit("E"))
    return (
        seg.join(stats, on=["device_id", "seq"])
        .with_columns(
            pl.when((pl.col("z_from") != pl.col("z_to")) & (pl.col("span") >= cfg["trip_min_span"])
                    & (pl.col("off_share") < cfg["excursion_off_share"])).then(pl.lit("trip"))
              .when(pl.col("off_share") >= cfg["excursion_off_share"]).then(pl.lit("excursion"))
              .otherwise(pl.lit("loop")).alias("kind"),
            term.alias("frm"), term_to.alias("to"))
        .select(list(TRIPS_SCHEMA))
        .cast(TRIPS_SCHEMA)
        .sort(["device_id", "seq"])
    )
