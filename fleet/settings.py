"""Thresholds, loaded once from config/settings.json."""
from __future__ import annotations

import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path

from .geo import Plane

ROOT = Path(__file__).resolve().parent.parent


@lru_cache
def settings(path: str | None = None) -> dict:
    p = Path(path or os.environ.get("FLEET_SETTINGS", ROOT / "config" / "settings.json"))
    return json.loads(p.read_text(encoding="utf-8"))


def settings_hash() -> str:
    raw = json.dumps(settings(), sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()[:12]


def plane() -> Plane:
    p = settings()["plane"]
    return Plane(p["lat0"], p["lon0"])


@lru_cache
def terminals() -> dict:
    p = ROOT / "config" / "terminals.json"
    data = json.loads(p.read_text(encoding="utf-8"))
    return {k: v for k, v in data.items() if not k.startswith("_")}
