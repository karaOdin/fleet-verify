"""Shared setup for the stream apps: Kafka address, topics, lines, and which line each device drives."""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from fleet.lines import build_lines, lines_frame
from fleet.settings import ROOT, plane, settings
from sources import cloudroutes

BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
POSITIONS = os.environ.get("TOPIC_POSITIONS", "fleet.positions")
TRIP_EVENTS = os.environ.get("TOPIC_TRIP_EVENTS", "fleet.trip-events")
ALERTS = os.environ.get("TOPIC_ALERTS", "fleet.alerts")
DATA = Path(os.environ.get("FLEET_DATA", ROOT / "data"))
CFG = settings()
TZ = ZoneInfo(CFG["timezone"])


def load_lines():
    return build_lines(lines_frame(cloudroutes.lines()), plane())


def device_assignments() -> dict[int, dict]:
    """device_id -> {"bus", "line"}. Taken from the most recent daily identity
    result (what the device really drove); falls back to the registry."""
    wh = DATA / "warehouse" / "device_identity"
    files = sorted(wh.glob("*.parquet")) if wh.exists() else []
    if files:
        df = pl.read_parquet(files[-1]).drop_nulls(["resolved_device_id", "resolved_line"])
        return {int(r["resolved_device_id"]): {"bus": r["bus"], "line": r["resolved_line"]} for r in df.iter_rows(named=True)}
    return {int(r["traccar_device_id"]): {"bus": r["bus"], "line": r["line"]}
            for r in cloudroutes.registry() if r.get("traccar_device_id")}


def local_time(fix_time_utc: str) -> datetime:
    t = datetime.fromisoformat(fix_time_utc.replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=ZoneInfo("UTC"))
    return t.astimezone(TZ)


def hhmm(s: int) -> str:
    return f"{s // 3600:02d}:{s % 3600 // 60:02d}"


def message(device_id: int, fix_time_utc: str, lat: float, lon: float, speed_kn: float) -> tuple[str, bytes]:
    """Kafka key and value for one position (the same format from every producer)."""
    return str(device_id), json.dumps({"device_id": int(device_id), "fix_time_utc": fix_time_utc,
                                       "lat": float(lat), "lon": float(lon), "speed_kn": float(speed_kn or 0)}).encode()


def event_ms(fix_time_utc: str) -> int:
    """Kafka message timestamp = the GPS fix time (event time), so consumers that read several
    partitions process positions in data-time order, even when a replay dumps a day at once."""
    t = datetime.fromisoformat(fix_time_utc.replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=ZoneInfo("UTC"))
    return int(t.timestamp() * 1000)


def _local_minute():
    local = pl.col("fix_time_utc").str.to_datetime(time_zone="UTC").dt.convert_time_zone(CFG["timezone"])
    return local.dt.hour().cast(pl.Int32) * 60 + local.dt.minute().cast(pl.Int32)


def from_local_time(df, hhmm_start: str):
    """Keep positions at or after a local wall-clock time (for demos that skip the night)."""
    h, m = map(int, hhmm_start.split(":"))
    return df.filter(_local_minute() >= h * 60 + m)


def before_local_time(df, hhmm_end: str):
    """Keep positions before a local wall-clock time."""
    h, m = map(int, hhmm_end.split(":"))
    return df.filter(_local_minute() < h * 60 + m)
