"""Live alerts: situations a dispatcher should look at now, which the daily batch only sees tomorrow.

An alert is a recommendation for a human reviewer, never a sanction.

Per device (same device state as the trip rules, one point at a time):
  off_route              farther than `off_route_m` from the line, and driving there (faster than `idle_kmh`)
                         for more than `off_route_min` minutes of that episode. Standing time does not count:
                         on 19 Nov every bus of lines 04 and 05 waits ~600 m off the route near their shared
                         terminal between trips (the batch calls it an excursion), which is a layover, not a
                         detour. A long standstill is the idle rule's business.
  idle_outside_terminal  slower than `idle_kmh` for more than `idle_min` minutes, outside the terminal zones
Both start counting only once the device has reached a terminal of its line today, so the drive from
the depot and the wait there before the first trip never raise them. Points at the device's overnight
parking place (`home`, its first position of the day) are ignored too.

Across devices (driven by the stream's own clock, the latest fix time seen on any device, so a replay
behaves like real time; over Kafka there is one clock per partition, since order holds only within one):
  silent                 no position for more than `silent_min` minutes from a device that was in service:
                         it had moved on its line during the `active_window_min` minutes before it went
                         quiet, and its last position was not a parked bus (see `_in_service_when_quiet`).

If the time of a position is more than `rewind_min` minutes before the stream clock, the stream was
rewound (a day replayed again): the clock starts over instead of declaring every device silent.

Every alert is a pair of events with the same `alert_id`: state "opened" (when the rule is met; `start` is
when the situation began) and later "closed" (with `duration_min`). An open alert is never opened twice.
Times: `ts` = epoch seconds (durations), `s` = seconds since local midnight (display).
"""
from __future__ import annotations

import math

import numpy as np

from .geo import project
from .lines import Line
from .live import _zone

TYPES = ("off_route", "idle_outside_terminal", "silent")


# ---------------- per device ----------------

def init_state() -> dict:
    return {"started": False, "home": None, "off": None, "idle": None, "open": {}}


def _event(kind: str, state: str, device_id: int, line: str, start_ts: float, start_s: int,
           at_ts: float, at_s: int, lat=None, lon=None) -> dict:
    e = {"alert": kind, "state": state, "alert_id": f"{device_id}:{kind}:{int(start_ts)}",
         "device_id": int(device_id), "line": line, "start": int(start_s), "at": int(at_s),
         "start_ts": int(start_ts), "at_ts": int(at_ts)}
    if lat is not None:
        e.update(lat=float(lat), lon=float(lon))
    if state == "closed":
        e["duration_min"] = round((at_ts - start_ts) / 60, 1)
    return e


def _rule(state: dict, kind: str, cond: bool, min_s: float, p: dict, line: str, out: list, counts: bool = True) -> None:
    """A condition that holds continuously; the alert opens once the time that `counts` within the
    episode reaches `min_s`, and closes when the condition stops."""
    key = "off" if kind == "off_route" else "idle"
    since = state[key]
    if cond:
        if since is None:
            state[key] = since = {"ts": p["ts"], "s": p["s"], "last": p["ts"], "n": 0.0}
        if counts:
            since["n"] += p["ts"] - since["last"]
        since["last"] = p["ts"]
        if kind not in state["open"] and since["n"] >= min_s:
            state["open"][kind] = True
            out.append(_event(kind, "opened", p["device_id"], line, since["ts"], since["s"], p["ts"], p["s"],
                              p.get("lat"), p.get("lon")))
    elif since is not None:
        if kind in state["open"]:
            del state["open"][kind]
            out.append(_event(kind, "closed", p["device_id"], line, since["ts"], since["s"], p["ts"], p["s"]))
        state[key] = None


def process(state: dict, point: dict, line: Line, xy, cfg: dict, proj: tuple | None = None) -> list[dict]:
    """point: {"device_id", "s", "ts" (defaults to s), "kmh", optional "lat"/"lon"}; xy: metres.
    proj: (distance to the route, distance along it) if the caller already projected the point."""
    ac = cfg["alerts"]
    p = {**point, "ts": point.get("ts", point["s"])}
    q = np.asarray(xy, dtype=float).reshape(1, 2)
    if state["home"] is None:
        state["home"] = [float(q[0, 0]), float(q[0, 1])]
    if proj is None:
        off, along = project(q, line.pts, line.cum, chunk=len(line.pts))
        off, along = float(off[0]), float(along[0])
    else:
        off, along = proj
    dS, dE = float(np.hypot(*(q[0] - line.S))), float(np.hypot(*(q[0] - line.E)))
    if _zone(off, along, dS, dE, line.length, cfg):
        state["started"] = True                        # reached a terminal: the service day has begun
    at_home = math.hypot(q[0, 0] - state["home"][0], q[0, 1] - state["home"][1]) <= ac["home_m"]
    active = state["started"] and not at_home
    in_terminal = min(dS, dE) <= ac["terminal_zone_m"] or _zone(off, along, dS, dE, line.length, cfg) > 0
    state["last_on_line"] = off <= cfg["on_route_m"]
    state["last_in_terminal"] = bool(in_terminal)
    out: list[dict] = []
    _rule(state, "off_route", active and off > ac["off_route_m"], ac["off_route_min"] * 60, p, line.code, out,
          counts=p["kmh"] >= ac["idle_kmh"])
    _rule(state, "idle_outside_terminal", active and p["kmh"] < ac["idle_kmh"] and not in_terminal,
          ac["idle_min"] * 60, p, line.code, out)
    return out


# ---------------- across devices: the stream clock and `silent` ----------------

def clock_init() -> dict:
    return {"now_ts": None, "checked_ts": None, "devices": {}}


def _in_service_when_quiet(d: dict, ac: dict) -> bool:
    """Was the device in service at its last position? It moved on its line shortly before, and its
    last position was not a parked bus: standing still at a terminal or off the line (a tracker with the
    engine off reports only every 15 min or so). A bus standing at a stop between terminals still counts."""
    moved = d["moved_ts"] is not None and d["last_ts"] - d["moved_ts"] <= ac["active_window_min"] * 60
    parked = d["last_kmh"] < ac["idle_kmh"] and (d["last_in_terminal"] or not d["last_on_line"])
    return moved and not parked


def observe(clock: dict, point: dict, line_code: str, dev_state: dict, cfg: dict) -> list[dict]:
    """Record a position (any device), after process() ran on it with `dev_state`.
    Returns the `closed` event if this device was silent."""
    on_line, in_terminal = dev_state.get("last_on_line", False), dev_state.get("last_in_terminal", False)
    ac = cfg["alerts"]
    ts = point.get("ts", point["s"])
    if clock["now_ts"] is not None and ts < clock["now_ts"] - ac["rewind_min"] * 60:
        clock.update(clock_init())        # rewound (a replay started again): a fresh clock, no silent flood
    clock["now_ts"] = ts if clock["now_ts"] is None else max(clock["now_ts"], ts)
    dev = int(point["device_id"])
    d = clock["devices"].setdefault(dev, {"moved_ts": None, "open": None})
    out = []
    if d["open"] is not None:
        o = d["open"]
        out.append(_event("silent", "closed", dev, d["line"], o["ts"], o["s"], ts, point["s"]))
        d["open"] = None
    moving_on_line = on_line and point["kmh"] >= ac["moving_kmh"]
    d.update(line=line_code, last_ts=ts, last_s=int(point["s"]), last_kmh=float(point["kmh"]), last_on_line=on_line,
             last_in_terminal=in_terminal, lat=point.get("lat"), lon=point.get("lon"))
    if moving_on_line:
        d["moved_ts"] = ts
    return out


def check_silent(clock: dict, now_s: int, cfg: dict) -> list[dict]:
    """Open `silent` for every device quiet for too long. Call on every message, after observe()."""
    ac = cfg["alerts"]
    h0, h1 = cfg["service_hours"]
    now_ts = clock["now_ts"]
    if now_ts is None or now_ts == clock["checked_ts"] or not h0 * 3600 <= now_s < (h1 + 1) * 3600:
        return []
    clock["checked_ts"] = now_ts
    out = []
    for dev, d in clock["devices"].items():
        if d["open"] is None and now_ts - d["last_ts"] > ac["silent_min"] * 60 and _in_service_when_quiet(d, ac):
            d["open"] = {"ts": d["last_ts"], "s": d["last_s"]}
            out.append(_event("silent", "opened", dev, d["line"], d["last_ts"], d["last_s"], now_ts, now_s,
                              d.get("lat"), d.get("lon")))
    return out
