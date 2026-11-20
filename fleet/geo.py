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
