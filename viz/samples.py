"""Raw data samples for the source nodes ("this is what the data really looks like").

Read from the files in data/inbox only: never from the live APIs, so no token is used and nothing
changes during the talk. Public (no admin token), so everything that leaves here is filtered:
  - fields are kept by allow-list: a new field in the source (a receiver, a note) is never shown
  - the bus is its anonymised label (Bus A, ...); receiver_id, bus_id and free-text notes are dropped
  - device ids only, never device names; place names by name_en; no Arabic text at all

The road paper is chosen for its quirk: paper 6477 (line 10) writes every direction the opposite
way round to where the GPS shows the bus going (aller/retour are written inconsistently: 52-65 %
agreement over the day). The GPS rows are the same bus at the start of that paper's first trip.
"""
from __future__ import annotations

import csv
import json
import math
import os
import re
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INBOX = Path(os.environ.get("FLEET_INBOX", ROOT / "data" / "inbox"))
DAY = os.environ.get("VIZ_DAY", "2026-11-19")
PAPER_ID = 6477                     # the paper shown (see above)
PAPER_TRIPS = 3                     # trips kept in the sample
GPS_ROWS = 8
GPS_FROM_LOCAL = "06:24"            # the first trip of that paper leaves at 06:26 (GPS)

PAPER_FIELDS = ["id", "work_date", "shift", "status", "bus", "line", "total_tickets", "total_amount", "trips_count",
                "submitted_at", "approved_at", "rejected_at", "created_at", "updated_at", "changed_at", "trips"]
TRIP_FIELDS = ["id", "trip_number", "direction", "departure_time", "arrival_time", "tickets", "amount", "updated_at"]
GPS_FIELDS = ["device_id", "fix_time_utc", "lat", "lon", "speed_kn"]
ARABIC = re.compile(r"[؀-ۿ]")
csv.field_size_limit(10**9)


def _clean(v):
    """Defence in depth: no Arabic free text leaves this module."""
    return None if isinstance(v, str) and ARABIC.search(v) else v


def _registry() -> list[dict]:
    return json.loads((INBOX / "fleet_registry.json").read_text(encoding="utf-8"))["buses"]


def _paper(show_bus) -> tuple[dict, dict]:
    papers = json.loads((INBOX / f"road_papers_{DAY}.json").read_text(encoding="utf-8"))
    papers = papers.get("data") or papers.get("road_papers") or []
    p = next((x for x in papers if x.get("id") == PAPER_ID), papers[0])
    out = {k: _clean(p.get(k)) for k in PAPER_FIELDS if k in p and k != "trips"}
    out["bus"] = show_bus(p.get("bus"))
    out["trips"] = [{k: _clean(t.get(k)) for k in TRIP_FIELDS if k in t}
                    for t in sorted(p.get("trips", []), key=lambda t: t.get("trip_number") or 0)[:PAPER_TRIPS]]
    out["trips_count"] = p.get("trips_count")       # the real count; only the first trips are shown
    return p, out


def _gps(bus: str) -> tuple[int | None, list[list[str]]]:
    from stream.common import from_local_time
    import polars as pl
    dev = next((b.get("traccar_device_id") for b in _registry() if str(b.get("bus")) == str(bus)), None)
    df = pl.read_csv(INBOX / f"positions_{DAY}.csv.gz", infer_schema=False)      # every value as stored (text)
    if dev is None:
        dev = int(df["device_id"][0])
    rows = df.filter(pl.col("device_id") == str(dev)).sort("fix_time_utc")
    rows = from_local_time(rows, GPS_FROM_LOCAL).head(GPS_ROWS).select(GPS_FIELDS)
    return dev, [list(r) for r in rows.iter_rows()]


def _line(code: str) -> dict:
    from fleet.lines import _json, line_code
    from fleet.settings import plane, settings, terminals
    rows = list(csv.DictReader(open(INBOX / "lines.csv", encoding="utf-8")))
    r = next(x for x in rows if line_code(x["name"]) == code and x.get("deleted_at") in (None, "", "NULL"))
    pl_ = plane()
    names = terminals().get(code, {}).get("name_en", {})

    def terminal(end: str, col: str) -> dict:
        ring = _json(r[col])
        lat, lon = sum(p[0] for p in ring) / len(ring), sum(p[1] for p in ring) / len(ring)
        c = pl_.xy([lat], [lon])[0]
        v = pl_.xy([p[0] for p in ring], [p[1] for p in ring])
        radius = max(math.hypot(x - c[0], y - c[1]) for x, y in v)
        return {"end": end, "name_en": names.get(end), "centre": [round(lat, 5), round(lon, 5)],
                "geofence_radius_m": round(radius), "geofence_points": len(ring)}

    poly, wps = _json(r.get("polyline")), _json(r.get("waypoints"))
    return {"code": code, "name": r["name"], "terminals": [terminal("S", "start_geofence"), terminal("E", "end_geofence")],
            "rule_terminal_radius_m": settings().get("terminal_radius_m"),
            "polyline_points": len(poly) if isinstance(poly, list) else None,
            "waypoints_points": len(wps) if isinstance(wps, list) else None,
            "updated_at": r.get("updated_at")}


def _compact(obj) -> str:
    """JSON with every innermost object or list on one line (a trip per line): readable on a projector."""
    def leafy(v):
        return isinstance(v, (dict, list)) and not any(isinstance(x, (dict, list)) for x in (v.values() if isinstance(v, dict) else v))

    def dump(v, ind):
        if leafy(v) or not isinstance(v, (dict, list)):
            return json.dumps(v, ensure_ascii=False, separators=(", ", ": "))
        pad, inner = " " * ind, " " * (ind + 2)
        if isinstance(v, dict):
            body = ",\n".join(f"{inner}{json.dumps(k)}: {dump(x, ind + 2)}" for k, x in v.items())
            return "{\n" + body + "\n" + pad + "}"
        return "[\n" + ",\n".join(inner + dump(x, ind + 2) for x in v) + "\n" + pad + "]"
    return dump(obj, 0)


def _gps_note(rows: list[list[str]]) -> str:
    from datetime import datetime
    ts = [datetime.fromisoformat(r[1]) for r in rows]
    gaps = sorted((b - a).total_seconds() for a, b in zip(ts, ts[1:]) if b != a)
    repeats = len(rows) - len({tuple(r) for r in rows})
    note = f" One fix about every {round(gaps[len(gaps) // 2])} s." if gaps else ""
    if repeats:
        note += (f" {repeats} row{'s' if repeats > 1 else ''} here {'are' if repeats > 1 else 'is'} an exact repeat "
                 f"of the row above: the source file repeats 1.2 % of its rows, the batch drops them.")
    return note


@lru_cache
def build(show_bus) -> dict[str, dict]:
    raw_paper, paper = _paper(show_bus)
    dev, gps = _gps(raw_paper.get("bus"))
    code = "".join(c for c in str(raw_paper.get("line", "")) if c.isdigit()).zfill(2)
    header = ",".join(GPS_FIELDS)
    return {
        "gps": {"id": "gps", "title": "Raw GPS", "format": "csv",
                "text": "\n".join([header] + [",".join(r) for r in gps]),
                "caption": f"data/inbox/positions_{DAY}.csv.gz · {GPS_ROWS} consecutive fixes of device {dev}, "
                           f"exactly as stored (UTC, speed in knots). The same bus as the road paper, as it leaves "
                           f"on its first trip." + _gps_note(gps)},
        "paper": {"id": "paper", "title": "Raw road paper", "format": "json",
                  "text": _compact(paper),
                  "caption": f"data/inbox/road_papers_{DAY}.json · paper {paper.get('id')}, first {PAPER_TRIPS} of "
                             f"{paper.get('trips_count')} trips; staff ids and the bus id removed. Its quirk: every "
                             f"direction is written the other way round to where the GPS shows the bus going."},
        "line": {"id": "line", "title": "Raw line", "format": "json",
                 "text": _compact(_line(code)),
                 "caption": "data/inbox/lines.csv · the line of that paper: its two terminal geofences (centre and "
                            "radius measured from the polygon) and how many points its route has."},
    }


NODE_SAMPLES = {"replay": ["gps"], "raw_positions": ["gps"], "cloudroutes": ["paper", "line"],
                "road_papers": ["paper"], "route_fit": ["line"]}
