"""The same trip rules as fleet/trips.py, one GPS point at a time.

Used by the Kafka stream processor: state is a small JSON-able dict per device,
so it fits a Quix Streams state store, and every rule matches the batch version
exactly (tests/test_live_matches_batch.py replays a day and compares).

process(state, point) returns the list of events this point completes:
  {"event": "trip" | "excursion" | "loop", device_id, line, frm, to, dep, arr, off_share, span}
"""
from __future__ import annotations

import numpy as np

from .geo import project
from .lines import Line


def init_state() -> dict:
    return {"ep": None, "acc": None, "i": 0}


def _zone(off: float, along: float, dS: float, dE: float, L: float, cfg: dict) -> int:
    p0, p1 = cfg["terminal_progress"]
    on = off <= cfg["on_route_m"]
    if dS <= cfg["terminal_radius_m"] or (on and along <= p0 * L):
        return 1
    if dE <= cfg["terminal_radius_m"] or (on and along >= p1 * L):
        return 2
    return 0


def _acc_start(on: bool, along: float) -> dict:
    return {"n": 1, "n_on": int(on), "lo": along if on else None, "hi": along if on else None}


def _acc_add(acc: dict, on: bool, along: float) -> None:
    acc["n"] += 1
    if on:
        acc["n_on"] += 1
        acc["lo"] = along if acc["lo"] is None else min(acc["lo"], along)
        acc["hi"] = along if acc["hi"] is None else max(acc["hi"], along)


def process(state: dict, point: dict, line: Line, xy: np.ndarray, cfg: dict, proj: tuple | None = None) -> list[dict]:
    """point: {"device_id", "s" (seconds since local midnight)}; xy: the point in metres.
    proj: (distance to the route, distance along it) if the caller already projected the point."""
    q = np.asarray(xy, dtype=float).reshape(1, 2)
    if proj is None:
        off, along = project(q, line.pts, line.cum, chunk=len(line.pts))   # one point: one pass over the whole route
        off, along = float(off[0]), float(along[0])
    else:
        off, along = proj
    on = off <= cfg["on_route_m"]
    z = _zone(off, along, float(np.hypot(*(q[0] - line.S))), float(np.hypot(*(q[0] - line.E))), line.length, cfg)
    s = int(point["s"])
    ep, acc = state["ep"], state["acc"]
    events = []
    if z == 0:
        if acc is not None:
            _acc_add(acc, on, along)
    elif ep is not None and ep["z"] == z and s - ep["s1"] < cfg["episode_gap_s"]:
        ep["s1"] = s                       # still in the same terminal: the segment restarts here
        state["acc"] = _acc_start(on, along)
    else:
        if ep is not None and acc is not None:
            _acc_add(acc, on, along)       # the first point of the new terminal closes the segment
            off_share = 1 - acc["n_on"] / acc["n"]
            span = (acc["hi"] - acc["lo"]) / line.length if acc["lo"] is not None else 0.0
            if ep["z"] != z and span >= cfg["trip_min_span"] and off_share < cfg["excursion_off_share"]:
                kind = "trip"
            elif off_share >= cfg["excursion_off_share"]:
                kind = "excursion"
            else:
                kind = "loop"
            events.append({"event": kind, "device_id": point["device_id"], "line": line.code,
                           "frm": "SE"[ep["z"] - 1], "to": "SE"[z - 1], "dep": ep["s1"], "arr": s,
                           "off_share": off_share, "span": span})
        state["ep"] = {"z": z, "s0": s, "s1": s}
        state["acc"] = _acc_start(on, along)
    state["i"] += 1
    return events
