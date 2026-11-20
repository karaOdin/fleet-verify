"""Planar geometry for one city.

Points are projected to metres on a local equirectangular plane. Over a city
(a few tens of km) the error is far below GPS noise, and every distance and
projection below is plain numpy arithmetic.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class Plane:
    lat0: float
    lon0: float

    @property
    def kx(self) -> float:
        return 111_320 * math.cos(math.radians(self.lat0))

    @property
    def ky(self) -> float:
        return 110_540.0

    def xy(self, lat, lon) -> np.ndarray:
        lat = np.asarray(lat, dtype=float)
        lon = np.asarray(lon, dtype=float)
        return np.column_stack([(lon - self.lon0) * self.kx, (lat - self.lat0) * self.ky])

    def latlon(self, xy) -> tuple[float, float]:
        return self.lat0 + xy[1] / self.ky, self.lon0 + xy[0] / self.kx


def project(points: np.ndarray, line_pts: np.ndarray, cum: np.ndarray, chunk: int = 64):
    """Distance from each point to a polyline, and how far along the polyline
    the nearest point lies (metres from the start). Vectorised over points,
    chunked over segments to bound memory."""
    a = line_pts[:-1]
    d = line_pts[1:] - a
    l2 = (d ** 2).sum(1)
    l2[l2 == 0] = 1e-9
    n = len(points)
    best = np.full(n, 1e12)
    along = np.zeros(n)
    rows = np.arange(n)
    for k in range(0, len(a), chunk):
        aa, dd, ll = a[k:k + chunk], d[k:k + chunk], l2[k:k + chunk]
        t = ((points[:, None, 0] - aa[None, :, 0]) * dd[None, :, 0]
             + (points[:, None, 1] - aa[None, :, 1]) * dd[None, :, 1]) / ll[None, :]
        t = np.clip(t, 0, 1)
        px = aa[None, :, 0] + t * dd[None, :, 0] - points[:, None, 0]
        py = aa[None, :, 1] + t * dd[None, :, 1] - points[:, None, 1]
        dist = px * px + py * py
        i = dist.argmin(1)
        m = dist[rows, i]
        upd = m < best
        best[upd] = m[upd]
        along[upd] = (cum[k + i] + t[rows, i] * np.sqrt(ll[i]))[upd]
    return np.sqrt(best), along


class PointProjector:
    """project() for one point at a time, for the stream (called on every position).

    The route's segments are bucketed in a grid of `cell` metres. A point is compared only with the
    segments of the cells within `cell` of it, in plain Python (cheaper than numpy for a handful of
    segments). That is exact: if the nearest of those lies within `cell`, no segment outside the
    searched cells can be nearer. Otherwise every segment is compared. Each distance uses the same
    arithmetic as project(), and ties go to the lowest segment index as with argmin, so the result
    is identical to project(points, line_pts, cum, chunk=len(line_pts)).
    """

    def __init__(self, line_pts: np.ndarray, cum: np.ndarray, cell: float = 100.0):
        a = line_pts[:-1]
        d = line_pts[1:] - a
        l2 = (d ** 2).sum(1)
        l2[l2 == 0] = 1e-9
        self.seg = [(float(a[i, 0]), float(a[i, 1]), float(d[i, 0]), float(d[i, 1]), float(l2[i]),
                     float(np.sqrt(l2[i])), float(cum[i])) for i in range(len(a))]
        self.cell = cell
        self.grid: dict[tuple[int, int], list[int]] = {}
        for i, (ax, ay, dx, dy, *_rest) in enumerate(self.seg):
            x0, x1 = sorted((ax, ax + dx))
            y0, y1 = sorted((ay, ay + dy))
            for gx in range(int(x0 // cell), int(x1 // cell) + 1):
                for gy in range(int(y0 // cell), int(y1 // cell) + 1):
                    self.grid.setdefault((gx, gy), []).append(i)
        # every cell next to a route segment: the segments of its 3 x 3 neighbourhood, sorted by index
        near: dict[tuple[int, int], set] = {}
        for (gx, gy), ids in self.grid.items():
            for kx in (gx - 1, gx, gx + 1):
                for ky in (gy - 1, gy, gy + 1):
                    near.setdefault((kx, ky), set()).update(ids)
        self.near = {k: sorted(v) for k, v in near.items()}
        self.a, self.d, self.l2 = a, d, l2
        self.sl2, self.cum = np.sqrt(l2), cum

    def _full(self, x: float, y: float) -> tuple[float, float]:
        """Every segment at once (numpy), for points far from the route."""
        a, d = self.a, self.d
        t = ((x - a[:, 0]) * d[:, 0] + (y - a[:, 1]) * d[:, 1]) / self.l2
        np.clip(t, 0, 1, out=t)
        px = a[:, 0] + t * d[:, 0] - x
        py = a[:, 1] + t * d[:, 1] - y
        dist = px * px + py * py
        i = int(dist.argmin())
        return float(np.sqrt(dist[i])), float(self.cum[i] + t[i] * self.sl2[i])

    def _best(self, x: float, y: float, idx) -> tuple[float, int, float]:
        best, bi, bt = None, -1, 0.0
        seg = self.seg
        for i in idx:
            ax, ay, dx, dy, l2, _, _ = seg[i]
            t = ((x - ax) * dx + (y - ay) * dy) / l2
            t = 0.0 if t < 0 else (1.0 if t > 1 else t)
            px = ax + t * dx - x
            py = ay + t * dy - y
            dist = px * px + py * py
            if best is None or dist < best:
                best, bi, bt = dist, i, t
        return best, bi, bt

    def __call__(self, x: float, y: float) -> tuple[float, float]:
        c = self.cell
        near = self.near.get((int(x // c), int(y // c)))
        best = None
        if near:
            best, i, t = self._best(x, y, near)
        if best is None or best > c * c:            # nothing within one cell: compare every segment
            return self._full(x, y)
        seg = self.seg[i]
        return float(np.sqrt(best)), seg[6] + t * seg[5]
