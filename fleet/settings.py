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


DISPLAY_ONLY = {"name_en"}          # display names: they change no result, so they stay out of settings_hash


def _without_display(x):
    if isinstance(x, dict):
        return {k: _without_display(v) for k, v in x.items() if k not in DISPLAY_ONLY}
    if isinstance(x, list):
        return [_without_display(v) for v in x]
    return x


def settings_hash() -> str:
    raw = json.dumps(_without_display(settings()), sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()[:12]


def plane() -> Plane:
    p = settings()["plane"]
    return Plane(p["lat0"], p["lon0"])


@lru_cache
def terminals() -> dict:
    p = ROOT / "config" / "terminals.json"
    data = json.loads(p.read_text(encoding="utf-8"))
    return {k: v for k, v in data.items() if not k.startswith("_")}


@lru_cache
def place_names_en() -> dict[str, str]:
    """Data name of a terminal or layover place -> its English display name (name_en)."""
    out = {}
    for t in terminals().values():
        for end in ("S", "E"):
            if t.get("name_en", {}).get(end):
                out[t[end]] = t["name_en"][end]
    for places in settings().get("layovers", {}).values():
        for p in places if isinstance(places, list) else []:
            if p.get("name_en"):
                out[p["name"]] = p["name_en"]
    return out


def place_en(name):
    """The English display name of a place; "A → B" is translated on each side; unknown names pass through."""
    if not isinstance(name, str):
        return name
    names = place_names_en()
    return " → ".join(names.get(part, part) for part in name.split(" → "))
