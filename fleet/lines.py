"""Bus lines as drawn in CloudRoutes: the route polyline and the two terminal
geofences (S = start, E = end)."""
from __future__ import annotations

import json
from dataclasses import dataclass

import numpy as np
import polars as pl

from .geo import Plane


@dataclass
class Line:
    code: str            # "04", "10", ...
    name: str
    pts: np.ndarray      # route vertices, metres
    cum: np.ndarray      # distance along the route at each vertex
    S: np.ndarray        # start terminal centre, metres
    E: np.ndarray        # end terminal centre, metres

    @property
    def length(self) -> float:
        return float(self.cum[-1])


def line_code(name: str) -> str:
    digits = "".join(c for c in str(name) if c.isdigit())
    return digits.zfill(2) if digits else str(name)


def _json(v):
    """CloudRoutes exports some geometry columns as JSON inside a JSON string."""
    if v is None:
        return None
    v = json.loads(v) if isinstance(v, str) else v
    return json.loads(v) if isinstance(v, str) else v


def lines_frame(rows: list[dict]) -> pl.DataFrame:
    """Normalise raw CloudRoutes line rows (CSV export or API) to one row per line
    with plain lat/lon lists. This is what the pipeline stores."""
    out = []
    for r in rows:
        if r.get("deleted_at") not in (None, "", "NULL"):
            continue
        wp = _json(r["waypoints"])
        clean = [wp[0]]
        for p in wp[1:]:
            if p != clean[-1]:
                clean.append(p)
        sg = _json(r["start_geofence"])
        eg = _json(r["end_geofence"])
        out.append(dict(
            code=line_code(r["name"]), name=r["name"], line_id=str(r["id"]),
            route_lat=[float(p[0]) for p in clean], route_lon=[float(p[1]) for p in clean],
            s_lat=float(np.mean([p[0] for p in sg])), s_lon=float(np.mean([p[1] for p in sg])),
            e_lat=float(np.mean([p[0] for p in eg])), e_lon=float(np.mean([p[1] for p in eg])),
            updated_at=r.get("updated_at"),
        ))
    return pl.DataFrame(out)


def build_lines(frame: pl.DataFrame, plane: Plane) -> dict[str, Line]:
    out = {}
    for r in frame.iter_rows(named=True):
        pts = plane.xy(r["route_lat"], r["route_lon"])
        seg = np.diff(pts, axis=0)
        cum = np.concatenate([[0.0], np.cumsum(np.hypot(seg[:, 0], seg[:, 1]))])
        out[r["code"]] = Line(r["code"], r["name"], pts, cum,
                              plane.xy([r["s_lat"]], [r["s_lon"]])[0],
                              plane.xy([r["e_lat"]], [r["e_lon"]])[0])
    return out
