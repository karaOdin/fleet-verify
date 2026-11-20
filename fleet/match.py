"""Each declared trip against the GPS trips of the bus's resolved device.

Matching is one-to-one (one GPS trip can confirm only one declared trip), so
it is an assignment problem solved per bus with the Hungarian method. A Polars
`join_asof` would let two declared trips claim the same GPS trip.

Cost of pairing a declared trip with a GPS trip, in seconds:
  |dep difference| + |arr difference|, or 2 x |dep difference| when the declared
  trip is a stub (<= short_trip_min) whose arrival is not a real time.
Pairs costing more than max_cost_min are not allowed.
"""
from __future__ import annotations

import numpy as np
import polars as pl
from scipy.optimize import linear_sum_assignment

STATUS = {
    "ok": "confirmed", "ok_dep": "departure confirmed (written arrival not real)", "diff": "time differs",
    "dup": "no separate GPS trip (partial trip or shift handover)", "missing": "not seen in GPS",
    "extra": "in GPS, not declared", "nogps": "cannot verify: no usable GPS",
}


def _assign(decl: pl.DataFrame, gps: pl.DataFrame, mc: dict) -> dict[int, int]:
    if decl.is_empty() or gps.is_empty():
        return {}
    A = decl["dep"].to_numpy()[:, None].astype(float)
    B = decl["arr"].to_numpy()[:, None].astype(float)
    short = (B - A) <= mc["short_trip_min"] * 60
    gd = gps["dep"].to_numpy()[None, :].astype(float)
    ga = gps["arr"].to_numpy()[None, :].astype(float)
    C = np.where(short, 2 * np.abs(A - gd), np.abs(A - gd) + np.abs(B - ga))
    C = np.where(C <= mc["max_cost_min"] * 60, C, 1e9)
    rows, cols = linear_sum_assignment(C)
    return {int(r): int(c) for r, c in zip(rows, cols) if C[r, c] < 1e9}


def verify(papers: pl.DataFrame, trips: pl.DataFrame, identity: pl.DataFrame, terminals: dict, cfg: dict) -> pl.DataFrame:
    mc = cfg["match"]
    decl_all = papers.drop_nulls("dep")
    res = identity.select("bus", "resolved_device_id", "resolved_line")
    gps_all = trips.filter(pl.col("kind") == "trip")
    out = []
    for (bus,), decl in decl_all.join(res, on="bus", how="left").group_by(["bus"], maintain_order=True):
        decl = decl.sort("dep").with_row_index("j")
        dev = decl["resolved_device_id"][0]
        gps = (gps_all.filter(pl.col("device_id") == dev) if dev is not None else gps_all.clear()).sort("dep").with_row_index("g")
        pairs = _assign(decl, gps, mc)
        pair_df = pl.DataFrame({"j": list(pairs.keys()), "g": list(pairs.values())}, schema={"j": pl.UInt32, "g": pl.UInt32})
        m = (decl.join(pair_df, on="j", how="left")
                 .join(gps.select("g", pl.col("dep").alias("gps_dep"), pl.col("arr").alias("gps_arr"), "frm", "to"), on="g", how="left"))
        near = mc["near_dep_min"] * 60
        gdeps = gps["dep"].to_numpy() if not gps.is_empty() else np.array([])
        m = m.with_columns(
            ((pl.col("arr") - pl.col("dep")) <= mc["short_trip_min"] * 60).alias("short"),
            ((pl.col("dep") - pl.col("gps_dep")) / 60).round(0).cast(pl.Int64).alias("delta_dep_min"),
            ((pl.col("arr") - pl.col("gps_arr")) / 60).round(0).cast(pl.Int64).alias("delta_arr_min"),
            pl.col("dep").map_elements(lambda d: bool(len(gdeps)) and bool((np.abs(gdeps - d) <= near).any()),
                                       return_dtype=pl.Boolean).alias("gps_nearby"))
        dd, da = pl.col("delta_dep_min").abs(), pl.col("delta_arr_min").abs()
        m = m.with_columns(
            pl.when(pl.lit(dev is None)).then(pl.lit("nogps"))
              .when(pl.col("g").is_null() & pl.col("gps_nearby")).then(pl.lit("dup"))
              .when(pl.col("g").is_null()).then(pl.lit("missing"))
              .when(pl.col("short") & (dd <= mc["dep_ok_min"])).then(pl.lit("ok_dep"))
              .when(~pl.col("short") & (dd <= mc["dep_ok_min"]) & (da <= mc["arr_ok_min"])).then(pl.lit("ok"))
              .otherwise(pl.lit("diff")).alias("status"))
        out.append(m.select("bus", "paper_id", "shift", "trip_number", "direction", "dep_time", "arr_time", "tickets",
                            "gps_dep", "gps_arr", "frm", "to", "delta_dep_min", "delta_arr_min", "short", "status",
                            pl.lit(dev).cast(pl.Int64).alias("device_id"), "resolved_line"))
        # GPS trips of this device during the bus's declared hours that no declared trip took
        if not gps.is_empty():
            lo, hi = decl["dep"].min() - 1800, decl["arr"].max() + 1800
            extra = gps.filter(~pl.col("g").is_in(list(pairs.values())) & pl.col("dep").is_between(lo, hi))
            if not extra.is_empty():
                out.append(extra.select(pl.lit(bus).alias("bus"), pl.lit(None, pl.Int64).alias("paper_id"), pl.lit(None, pl.Utf8).alias("shift"),
                                        pl.lit(None, pl.Int64).alias("trip_number"), pl.lit(None, pl.Utf8).alias("direction"),
                                        pl.lit(None, pl.Utf8).alias("dep_time"), pl.lit(None, pl.Utf8).alias("arr_time"),
                                        pl.lit(None, pl.Int64).alias("tickets"), pl.col("dep").alias("gps_dep"), pl.col("arr").alias("gps_arr"),
                                        "frm", "to", pl.lit(None, pl.Int64).alias("delta_dep_min"), pl.lit(None, pl.Int64).alias("delta_arr_min"),
                                        pl.lit(False).alias("short"), pl.lit("extra").alias("status"),
                                        pl.col("device_id").cast(pl.Int64), pl.lit(decl["resolved_line"][0]).alias("resolved_line")))
    v = pl.concat(out, how="vertical_relaxed")
    fmt = lambda c: pl.format("{}:{}", (pl.col(c) // 3600).cast(pl.Utf8).str.zfill(2), (pl.col(c) % 3600 // 60).cast(pl.Utf8).str.zfill(2))
    tname = pl.struct("resolved_line", "frm", "to").map_elements(
        lambda s: f"{terminals.get(s['resolved_line'], {}).get(s['frm'], s['frm'])} → {terminals.get(s['resolved_line'], {}).get(s['to'], s['to'])}"
        if s["frm"] else None, return_dtype=pl.Utf8)
    v = v.with_columns(fmt("gps_dep").alias("gps_dep_time"), fmt("gps_arr").alias("gps_arr_time"), tname.alias("actual_direction"))
    # Which written direction means "leaves from S" on each line: the majority among matched trips.
    # Papers write aller/retour inconsistently, so a mismatch is a note, never a verdict.
    lab = (v.filter(pl.col("direction").is_in(["aller", "retour"]) & pl.col("frm").is_not_null())
            .with_columns(((pl.col("direction") == "aller") == (pl.col("frm") == "S")).alias("aller_is_S"))
            .group_by("resolved_line").agg(pl.col("aller_is_S").mean().alias("direction_agreement")))
    v = v.join(lab, on="resolved_line", how="left").with_columns(
        (pl.col("direction").is_in(["aller", "retour"]) & pl.col("frm").is_not_null()
         & (((pl.col("direction") == "aller") == (pl.col("frm") == "S")) != (pl.col("direction_agreement") >= 0.5))).alias("direction_differs"),
        pl.col("status").replace_strict(STATUS).alias("verdict"))
    return v.select("bus", "paper_id", "shift", "trip_number", "direction", "dep_time", "arr_time", "tickets",
                    "device_id", "resolved_line", "gps_dep_time", "gps_arr_time", "actual_direction",
                    "delta_dep_min", "delta_arr_min", "status", "verdict", "direction_differs").sort(
        ["bus", pl.coalesce("dep_time", "gps_dep_time")])
