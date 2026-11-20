"""CloudRoutes data: lines, the fleet registry and road papers.

Road papers come from the tenant's data API (CLOUDROUTES_TENANT_URL, CLOUDROUTES_DATA_TOKEN).
Lines and the fleet registry come from CLOUDROUTES_URL/TOKEN when those endpoints exist.
Anything not configured is read from data/inbox (same shapes).
Endpoint paths can be changed with CLOUDROUTES_*_PATH without touching code.
"""
from __future__ import annotations

import csv
import json
import os
import urllib.parse
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INBOX = Path(os.environ.get("FLEET_INBOX", ROOT / "data" / "inbox"))
csv.field_size_limit(10**9)


def configured() -> bool:
    """Reference data (lines, buses) API: CLOUDROUTES_URL + CLOUDROUTES_TOKEN."""
    return bool(os.environ.get("CLOUDROUTES_URL") and os.environ.get("CLOUDROUTES_TOKEN"))


def data_api_configured() -> bool:
    """Road-paper data API on the tenant domain: CLOUDROUTES_TENANT_URL + CLOUDROUTES_DATA_TOKEN
    (a read-only data token from `php artisan data-api:token issue`, never an agent token)."""
    return bool(os.environ.get("CLOUDROUTES_TENANT_URL") and os.environ.get("CLOUDROUTES_DATA_TOKEN"))


def _get(path: str, params: dict | None = None, base_env: str = "CLOUDROUTES_URL", token_env: str = "CLOUDROUTES_TOKEN"):
    base = os.environ[base_env].rstrip("/")
    url = base + path + ("?" + urllib.parse.urlencode(params) if params else "")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {os.environ[token_env]}",
                                               "Accept": "application/json"})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 4:          # 60 requests/min: wait and retry
                time.sleep(int(e.headers.get("Retry-After", 5 * (attempt + 1))))
                continue
            if e.code in (401, 403):
                raise PermissionError(f"CloudRoutes refused the data token ({e.code}): revoked, expired or missing ability") from e
            if e.code == 422:
                raise ValueError(f"CloudRoutes rejected the parameters: {e.read().decode(errors='replace')}") from e
            raise


def _data(payload, key):
    """Accept {key: [...]}, {"data": [...]} or a bare list."""
    if isinstance(payload, list):
        return payload
    return payload.get(key) or payload.get("data") or []


def _papers_pages(params: dict) -> list[dict]:
    """All pages of /api/data/v1/road-papers for the given filters."""
    path = os.environ.get("CLOUDROUTES_PAPERS_PATH", "/api/data/v1/road-papers")
    out, page = [], 1
    while page:
        payload = _get(path, {**params, "page": page, "per_page": 500},
                       base_env="CLOUDROUTES_TENANT_URL", token_env="CLOUDROUTES_DATA_TOKEN")
        out += payload.get("data", [])
        page = (payload.get("meta") or {}).get("next_page")
    return out


def _normalise_paper(p: dict) -> dict:
    """Data API shape -> the shape fleet.papers.flatten expects. Receiver names are never requested."""
    trips = sorted(p.get("trips") or [], key=lambda t: t.get("trip_number") or 0)
    if p.get("shift") == "night" and trips:
        # a night shift's times earlier than its first trip belong to the next day
        first = trips[0].get("departure_time") or "00:00"
        roll = lambda hm: None if not hm else (f"{int(hm[:2]) + 24:02d}{hm[2:]}" if hm < first else hm)
        trips = [{**t, "departure_time": roll(t.get("departure_time")), "arrival_time": roll(t.get("arrival_time"))} for t in trips]
    return {"id": p["id"], "bus": str(p.get("bus")), "line": p.get("line"), "shift": p.get("shift"),
            "status": p.get("status"), "work_date": p.get("work_date"), "receiver_id": p.get("receiver_id"),
            "changed_at": p.get("changed_at"),
            "trips": [{"trip_number": t.get("trip_number"), "direction": t.get("direction"),
                       "departure_time": t.get("departure_time"), "arrival_time": t.get("arrival_time"),
                       "total_tickets": t.get("tickets", t.get("total_tickets"))} for t in trips]}


def papers_changed_since(since_iso: str) -> list[dict]:
    """Papers whose paper or trips changed at or after `since_iso` (inclusive: repeats are expected)."""
    return [_normalise_paper(p) for p in _papers_pages({"updated_since": since_iso})]


def lines() -> list[dict]:
    if configured():
        return _data(_get(os.environ.get("CLOUDROUTES_LINES_PATH", "/api/v1/lines")), "lines")
    return list(csv.DictReader(open(INBOX / "lines.csv", encoding="utf-8")))


def registry() -> list[dict]:
    """[{bus, line, traccar_device_id}]"""
    if configured():
        rows = _data(_get(os.environ.get("CLOUDROUTES_BUSES_PATH", "/api/v1/buses")), "buses")
        return [{"bus": str(r.get("bus") or r.get("number") or r.get("name")), "line": r.get("line"),
                 "traccar_device_id": r.get("traccar_device_id")} for r in rows]
    return json.loads((INBOX / "fleet_registry.json").read_text(encoding="utf-8"))["buses"]


def road_papers(day: str) -> list[dict]:
    """All road papers of one work date, each with its trips:
    {id, bus, line, shift, status, trips: [{trip_number, direction, departure_time, arrival_time, total_tickets}]}"""
    if data_api_configured():
        return [_normalise_paper(p) for p in _papers_pages({"date": day})]
    f = INBOX / f"road_papers_{day}.json"
    if not f.exists():
        raise FileNotFoundError(f"No road papers for {day}: set CLOUDROUTES_URL/TOKEN or put {f.name} in {INBOX}")
    payload = json.loads(f.read_text(encoding="utf-8"))
    if "data" in payload:                      # a saved response of the data API
        return [_normalise_paper(p) for p in payload["data"]]
    return payload["road_papers"]
