"""What the public pages may show, shared by the pipeline page (viz/server.py) and the Streamlit dashboard
(dashboard/app.py), so both say the same thing:

  - bus labels: "Bus A", "Bus B", ... from the fleet registry (sorted bus numbers), the same letter for a bus
    on both pages, while VIZ_ANONYMIZE=1 (the default: the operator's consent for real numbers is pending);
  - the day's result numbers (declared, confirmed %, time differs, not found, km, commercial %).
"""
from __future__ import annotations

import os
from pathlib import Path

import polars as pl

ANON = os.environ.get("VIZ_ANONYMIZE", "1") == "1"


def _letters(n: int) -> str:
    s = ""
    n += 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def bus_alias() -> dict[str, str]:
    try:
        from sources import cloudroutes
        buses = sorted({str(b["bus"]) for b in cloudroutes.registry()})
    except Exception:
        buses = []
    return {b: f"Bus {_letters(i)}" for i, b in enumerate(buses)}


ALIAS = bus_alias()


def show_bus(b) -> str | None:
    """The label a viewer sees for a bus number (None stays None)."""
    if b is None:
        return None
    return ALIAS.get(str(b), "Bus ?") if ANON else f"Bus {b}"


def label_order(label: str) -> tuple:
    """Bus A, B, ... Z, AA, AB: by length, then by letters."""
    s = str(label or "")
    return (len(s), s)


def day_results(wh: Path, day: str) -> dict | None:
    """The day's result, from the warehouse the batch writes; None before the batch has run."""
    f = wh / "trip_verification" / f"{day}.parquet"
    if not f.exists():
        return None
    v = pl.read_parquet(f)
    c = {k: int(x) for k, x in v.group_by("status").len().iter_rows()}
    km_f = wh / "km_report" / f"{day}.parquet"
    km = pl.read_parquet(km_f) if km_f.exists() else None
    ident_f = wh / "device_identity" / f"{day}.parquet"
    ident = pl.read_parquet(ident_f) if ident_f.exists() else None
    declared = int(v["paper_id"].is_not_null().sum())
    with_gps = declared - c.get("nogps", 0)
    confirmed = c.get("ok", 0) + c.get("ok_dep", 0)
    return {
        "declared": declared,
        "status": c,
        "confirmed_pct": round(100 * confirmed / with_gps) if with_gps else None,   # of declared trips with GPS
        "total_km": round(float(km["total_km"].sum()), 1) if km is not None else None,
        "commercial_pct": round(100 * float(km["commercial_km"].sum()) / float(km["total_km"].sum())) if km is not None else None,
        "identity": dict(ident.group_by("verdict").len().iter_rows()) if ident is not None else {},
    }
