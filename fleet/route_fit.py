"""Which line is each device actually driving? Compare its moving points with
every route: precision = share of its points near the route, recall = share of
the route it covers (100 m bins), F = harmonic mean."""
from __future__ import annotations

import numpy as np
import polars as pl

from .geo import project
from .lines import Line


def route_fit(pos: pl.DataFrame, lines: dict[str, Line], cfg: dict) -> pl.DataFrame:
    rf = cfg["route_fit"]
    h0, h1 = cfg["service_hours"]
    moving = (
        pos.filter((pl.col("kmh") > rf["moving_kmh"]) & pl.col("t").dt.hour().is_between(h0, h1))
        .with_columns((pl.col("t").dt.epoch("s") // rf["thin_s"]).alias("slot"))
        .unique(["device_id", "slot"], keep="first", maintain_order=True)
        .sort(["device_id", "t"])
    )
    rows = []
    for (dev,), g in moving.group_by(["device_id"], maintain_order=True):
        q = np.column_stack([g["x"].to_numpy(), g["y"].to_numpy()])
        for code, ln in lines.items():
            off, along = project(q, ln.pts, ln.cum)
            near = off <= rf["near_m"]
            nbins = int(np.ceil(ln.length / rf["bin_m"]))
            covered = len(np.unique((along[near] // rf["bin_m"]).astype(int)))
            prec, rec = float(near.mean()), covered / nbins
            f = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
            rows.append(dict(device_id=dev, line=code, points=len(q), precision=prec, recall=rec, f=f))
    fit = pl.DataFrame(rows)
    return fit.with_columns(
        pl.col("f").rank("ordinal", descending=True).over("device_id").alias("rank")
    ).sort(["device_id", "rank"])


def best_line(fit: pl.DataFrame, min_f: float) -> pl.DataFrame:
    """One row per device: its best line, F, and the margin over the second best."""
    second = fit.filter(pl.col("rank") == 2).select("device_id", pl.col("line").alias("second_line"), pl.col("f").alias("second_f"))
    return (fit.filter(pl.col("rank") == 1)
            .join(second, on="device_id", how="left")
            .select("device_id", "line", "f", "precision", "recall", "second_line",
                    (pl.col("f") - pl.col("second_f")).alias("margin"),
                    (pl.col("f") >= min_f).alias("on_network")))
