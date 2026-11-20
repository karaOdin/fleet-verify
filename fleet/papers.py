"""Road papers (the receiver's work sheet per shift), flattened to one row per declared trip."""
from __future__ import annotations

import polars as pl

from .lines import line_code

SCHEMA = {"paper_id": pl.Int64, "bus": pl.Utf8, "line": pl.Utf8, "shift": pl.Utf8, "status": pl.Utf8,
          "trip_number": pl.Int64, "direction": pl.Utf8, "dep_time": pl.Utf8, "arr_time": pl.Utf8,
          "dep": pl.Int64, "arr": pl.Int64, "tickets": pl.Int64, "notes": pl.Utf8}


def _sec(hhmm):
    if not hhmm:
        return None
    h, m = hhmm.split(":")[:2]
    return int(h) * 3600 + int(m) * 60


def flatten(papers: list[dict]) -> pl.DataFrame:
    """papers: CloudRoutes road papers, each with its trips. A paper with no trips
    keeps one row with empty trip fields, so it still counts as a paper."""
    rows = []
    for p in papers:
        base = dict(paper_id=int(p["id"]), bus=str(p["bus"]), line=line_code(p["line"]), shift=p.get("shift"),
                    status=p.get("status"), notes="; ".join(p["notes"]) if isinstance(p.get("notes"), list) else p.get("notes"))
        trips = p.get("trips") or []
        if not trips:
            rows.append({**base, "trip_number": None, "direction": None, "dep_time": None, "arr_time": None,
                         "dep": None, "arr": None, "tickets": None})
        for t in trips:
            rows.append({**base, "trip_number": t.get("trip_number"), "direction": t.get("direction"),
                         "dep_time": t.get("departure_time"), "arr_time": t.get("arrival_time"),
                         "dep": _sec(t.get("departure_time")), "arr": _sec(t.get("arrival_time")),
                         "tickets": t.get("total_tickets")})
    return pl.DataFrame(rows, schema=SCHEMA).sort(["bus", "dep"], nulls_last=True)
