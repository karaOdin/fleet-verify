"""Traccar REST client. Credentials come from the environment only:
TRACCAR_URL and TRACCAR_TOKEN (a user that sees only this company's devices,
not an administrator token)."""
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import polars as pl

RAW_SCHEMA = {"device_id": pl.Int64, "fix_time_utc": pl.Utf8, "lat": pl.Float64, "lon": pl.Float64, "speed_kn": pl.Float64}


def configured() -> bool:
    return bool(os.environ.get("TRACCAR_URL") and os.environ.get("TRACCAR_TOKEN"))


def _get(path: str, params: dict | None = None, timeout: int = 120):
    base = os.environ["TRACCAR_URL"].rstrip("/")
    url = base + path + ("?" + urllib.parse.urlencode(params, doseq=True) if params else "")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {os.environ['TRACCAR_TOKEN']}",
                                               "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def devices() -> list[dict]:
    return [{"id": d["id"], "name": d.get("name"), "status": d.get("status"), "lastUpdate": d.get("lastUpdate")}
            for d in _get("/api/devices")]


def _rows(points: list[dict]) -> list[dict]:
    return [{"device_id": p["deviceId"], "fix_time_utc": p["fixTime"], "lat": p["latitude"],
             "lon": p["longitude"], "speed_kn": p.get("speed", 0.0), "position_id": p.get("id")} for p in points]


def positions_day(device_ids: list[int], day: date, tz: str, retries: int = 3) -> tuple[pl.DataFrame, list[int]]:
    """All positions of one local day. Returns the table and the devices that failed."""
    z = ZoneInfo(tz)
    start = datetime.combine(day, time(0), z).astimezone(ZoneInfo("UTC"))
    end = start + timedelta(days=1)
    fmt = lambda d: d.strftime("%Y-%m-%dT%H:%M:%SZ")
    rows, failed = [], []
    for dev in device_ids:
        for attempt in range(retries):
            try:
                rows += _rows(_get("/api/positions", {"deviceId": dev, "from": fmt(start), "to": fmt(end)}))
                break
            except Exception:
                if attempt == retries - 1:
                    failed.append(dev)
    df = pl.DataFrame(rows, schema={**RAW_SCHEMA, "position_id": pl.Int64}) if rows else pl.DataFrame(schema=RAW_SCHEMA)
    return df.select(list(RAW_SCHEMA)), failed


def latest() -> list[dict]:
    """Latest position of every device (for live polling)."""
    return _rows(_get("/api/positions", timeout=30))
