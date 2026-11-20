"""The visual pipeline: one web page that shows every node of the system and what it
is doing right now, read from the real system (heartbeats of the stream apps,
Kafka offsets and lag, Dagster outputs in the warehouse, PostgreSQL).

    uvicorn viz.server:app --host 0.0.0.0 --port 8000

Controls (start a replay, stop the stream processor to show lag, run the daily
batch) need the header X-Admin-Token = VIZ_ADMIN_TOKEN. Without that variable every
control request is refused. The client address is never used: behind nginx every
request comes from 127.0.0.1. The page shows the control panel only after
/api/auth accepts the token (open the page once as /#admin=<token>).
"""
from __future__ import annotations

import ast
import asyncio
import json
import os
import re
import secrets
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path

import polars as pl
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from stream.common import ALERTS, DATA, POSITIONS, TRIP_EVENTS  # noqa: E402
from viz import kafka_metrics  # noqa: E402
from viz.graph import NODES, EDGES  # noqa: E402
from fleet.settings import place_en  # noqa: E402
from viz import samples as SAMPLES  # noqa: E402

STATE = DATA / "state"
WH = DATA / "warehouse"
DAY = os.environ.get("VIZ_DAY", "2026-11-19")
DEMO_FILE = os.environ.get("VIZ_DEMO_FILE", str(DATA / "inbox" / f"positions_{DAY}.csv.gz"))
DEMO_FROM = os.environ.get("VIZ_DEMO_FROM", "05:30")
ANON = os.environ.get("VIZ_ANONYMIZE", "1") == "1"
ADMIN = os.environ.get("VIZ_ADMIN_TOKEN")
PY = sys.executable

app = FastAPI(title="fleet-verify pipeline")


# ---------------- anonymisation ----------------

def _letters(n: int) -> str:
    s = ""
    n += 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _bus_alias() -> dict[str, str]:
    try:
        from sources import cloudroutes
        buses = sorted({str(b["bus"]) for b in cloudroutes.registry()})
    except Exception:
        buses = []
    return {b: f"Bus {_letters(i)}" for i, b in enumerate(buses)}


ALIAS = _bus_alias()
show_bus = (lambda b: ALIAS.get(str(b), "Bus ?")) if ANON else (lambda b: f"Bus {b}")


# ---------------- processes the page can start and stop ----------------

class Procs:
    def __init__(self):
        self.p: dict[str, subprocess.Popen] = {}
        self.group = os.environ.get("LIVE_GROUP", "live-trips")

    def running(self, name: str) -> bool:
        p = self.p.get(name)
        return p is not None and p.poll() is None

    def start(self, name: str, args: list[str], env: dict | None = None):
        if self.running(name):
            return
        log = (STATE / f"{name}.log").open("ab")
        self.p[name] = subprocess.Popen([PY, "-m", *args], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                        env={**os.environ, **(env or {})})

    def stop(self, name: str):
        p = self.p.get(name)
        if p is not None and p.poll() is None:
            p.terminate()
            try:
                p.wait(5)
            except subprocess.TimeoutExpired:
                p.kill()


PROCS = Procs()


# ---------------- the daily batch, run as a Dagster job ----------------

class Batch:
    STEP = re.compile(r" - (\w+) - (STEP_START|STEP_SUCCESS|STEP_FAILURE) - ")
    CHECK = re.compile(r"Asset check '(\w+)' on '(\w+)' (passed|failed)")

    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.running, self.day, self.started, self.finished = False, None, None, None
        self.steps, self.checks, self.ok, self.log = {}, {}, None, deque(maxlen=40)

    def run(self, day: str):
        with self.lock:
            if self.running:
                return
            self.reset()
            self.running, self.day, self.started = True, day, time.time()
        threading.Thread(target=self._run, args=(day,), daemon=True).start()

    def _exec(self, args: list[str]) -> int:
        env = {**os.environ, "DAGSTER_HOME": os.environ.get("DAGSTER_HOME", str(ROOT / ".dagster"))}
        Path(env["DAGSTER_HOME"]).mkdir(parents=True, exist_ok=True)
        p = subprocess.Popen([PY, "-m", "dagster", *args, "-m", "pipeline.definitions"], cwd=ROOT, env=env,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in p.stdout:
            if m := self.STEP.search(line):
                self.steps[m.group(1)] = {"STEP_START": "running", "STEP_SUCCESS": "done", "STEP_FAILURE": "failed"}[m.group(2)]
            if m := self.CHECK.search(line):
                self.checks[m.group(1)] = {"asset": m.group(2), "passed": m.group(3) == "passed"}
            if "ERROR" in line or "RUN_" in line:
                self.log.append(line.strip()[-300:])
        return p.wait()

    def _run(self, day: str):
        try:
            if not (WH / "lines" / "all.parquet").exists():
                self._exec(["asset", "materialize", "--select", "lines,fleet_registry,traccar_devices"])
            code = self._exec(["job", "execute", "-j", "daily_verification", "--tags", json.dumps({"dagster/partition": day})])
            self.ok = code == 0
        finally:
            self.running, self.finished = False, time.time()


BATCH = Batch()


# ---------------- reading the system ----------------

def _hb(name: str) -> dict | None:
    f = STATE / f"{name}.json"
    try:
        return json.loads(f.read_text())
    except Exception:
        return None


def _alive(hb: dict | None, within: float = 5) -> bool:
    return bool(hb) and not hb.get("stopped") and time.time() - hb.get("updated", 0) < within


def _asset(name: str, day: str) -> dict:
    f = WH / name / (f"{day}.parquet" if name not in ("lines", "fleet_registry", "traccar_devices") else "all.parquet")
    if not f.exists():
        return {"exists": False}
    try:
        rows = pl.scan_parquet(f).select(pl.len()).collect().item()
    except Exception:
        rows = None
    return {"exists": True, "rows": rows, "mtime": f.stat().st_mtime}


def _lake(day: str) -> dict:
    d = DATA / "lake" / "positions" / f"date={day}"
    files = list(d.glob("*.parquet")) if d.exists() else []
    rows = pl.scan_parquet([str(x) for x in files]).select(pl.len()).collect().item() if files else 0
    return {"files": len(files), "rows": rows}


_results_cache: dict = {}


def _results(day: str) -> dict | None:
    f = WH / "trip_verification" / f"{day}.parquet"
    if not f.exists():
        return None
    key = (day, f.stat().st_mtime)
    if _results_cache.get("key") == key:
        return _results_cache["val"]
    v = pl.read_parquet(f)
    c = dict(v.group_by("status").len().iter_rows())
    km_f = WH / "km_report" / f"{day}.parquet"
    km = pl.read_parquet(km_f) if km_f.exists() else None
    ident_f = WH / "device_identity" / f"{day}.parquet"
    ident = pl.read_parquet(ident_f) if ident_f.exists() else None
    val = {
        "declared": int(v["paper_id"].is_not_null().sum()),
        "status": {k: int(x) for k, x in c.items()},
        "total_km": round(float(km["total_km"].sum()), 1) if km is not None else None,
        "commercial_pct": round(100 * float(km["commercial_km"].sum()) / float(km["total_km"].sum())) if km is not None else None,
        "identity": dict(ident.group_by("verdict").len().iter_rows()) if ident is not None else {},
    }
    _results_cache.update(key=key, val=val)
    return val


_pg_cache = {"t": 0, "val": None}


def _postgres(day: str):
    url = os.environ.get("POSTGRES_URL")
    if not url:
        return {"configured": False}
    if time.time() - _pg_cache["t"] < 5:
        return _pg_cache["val"]
    try:
        import psycopg2
        with psycopg2.connect(url, connect_timeout=2) as conn, conn.cursor() as cur:
            cur.execute("select count(*) from trip_verification where work_date = %s", (day,))
            val = {"configured": True, "ok": True, "rows": cur.fetchone()[0]}
    except Exception as e:
        val = {"configured": True, "ok": False, "error": str(e)[:120]}
    _pg_cache.update(t=time.time(), val=val)
    return val


class Rates:
    """messages per second from successive counter readings"""
    def __init__(self):
        self.prev: dict[str, tuple[float, float]] = {}

    def rate(self, key: str, value) -> float:
        now = time.time()
        if value is None:
            return 0.0
        old = self.prev.get(key)
        self.prev[key] = (now, value)
        if not old or now - old[0] <= 0:
            return 0.0
        return max(0.0, (value - old[1]) / (now - old[0]))


RATES = Rates()


def snapshot() -> dict:
    day = BATCH.day or DAY
    k = kafka_metrics.read([POSITIONS, TRIP_EVENTS, ALERTS], [PROCS.group, "lake-sink"])
    rp, lt, ls = _hb("replay"), _hb("live_trips"), _hb("lake_sink")
    tp, pp = _hb("traccar_poll"), _hb("papers_poll")
    pos_end = k["topics"].get(POSITIONS) if k["ok"] else None
    ev_end = k["topics"].get(TRIP_EVENTS) if k["ok"] else None
    n = {}
    n["replay"] = {"active": _alive(rp), "sent": (rp or {}).get("sent"), "total": (rp or {}).get("total"),
                   "data_time": (rp or {}).get("data_time"), "rate": RATES.rate("replay", (rp or {}).get("sent"))}
    n["traccar"] = {"active": _alive(tp, 30), "total": (tp or {}).get("total")}
    n["positions"] = {"kafka": k["ok"], "offset": pos_end, "rate": RATES.rate("positions", pos_end)}
    n["live_trips"] = {"active": _alive(lt), "mode": (lt or {}).get("mode"), "processed": (lt or {}).get("processed"),
                       "trips": (lt or {}).get("trips"), "data_time": (lt or {}).get("data_time"),
                       "lag": k["lag"].get(PROCS.group) if k["ok"] else None,
                       "rate": RATES.rate("live", (lt or {}).get("processed"))}
    n["trip_events"] = {"kafka": k["ok"], "offset": ev_end, "trips": (lt or {}).get("trips")}
    open_alerts = [_public_alert(a) for a in (lt or {}).get("open_alerts", [])] if _alive(lt, 30) else []
    n["live_trips"]["alerts_open"] = len(open_alerts)
    n["alerts"] = {"kafka": k["ok"], "offset": k["topics"].get(ALERTS) if k["ok"] else None,
                   "total": (lt or {}).get("alerts"), "open": len(open_alerts)}
    n["live_board"] = {"trips": (lt or {}).get("trips")}
    n["lake_sink"] = {"active": _alive(ls), "written": (ls or {}).get("written"),
                      "lag": k["lag"].get("lake-sink") if k["ok"] else None,
                      "rate": RATES.rate("lake", (ls or {}).get("written"))}
    n["lake"] = _lake(day)
    n["cloudroutes"] = {"api": bool(os.environ.get("CLOUDROUTES_TENANT_URL") and os.environ.get("CLOUDROUTES_DATA_TOKEN")),
                        "poll_active": _alive(pp, 300), "total": (pp or {}).get("total")}
    for a in ["raw_positions", "road_papers", "route_fit", "gps_trips", "device_identity", "trip_verification",
              "km_report", "bus_day_report"]:
        n[a] = {**_asset(a, day), "step": BATCH.steps.get(a)}
    n["postgres"] = {**_postgres(day), "step": BATCH.steps.get("postgres_export")}
    return {
        "t": time.time(), "day": day, "kafka": {"ok": k["ok"], "error": k.get("error")},
        "nodes": n,
        "procs": {x: PROCS.running(x) for x in ["replay", "live_trips", "live_fallback", "lake_sink"]},
        "batch": {"running": BATCH.running, "day": BATCH.day, "ok": BATCH.ok, "steps": BATCH.steps,
                  "checks": BATCH.checks, "started": BATCH.started, "finished": BATCH.finished, "log": list(BATCH.log)[-5:]},
        "results": _results(day),
        "open_alerts": open_alerts,
        "anonymized": ANON,
    }


ALERT_LABEL = {"off_route": "Off route", "idle_outside_terminal": "Long stop outside a terminal", "silent": "Device silent"}


def _public_trip(e: dict) -> dict:
    """What a public viewer sees of a trip event: no device id, the bus anonymised."""
    return {"bus": show_bus(e.get("bus")), "event": e["event"], "line": e.get("line"),
            "dep": e.get("dep_time"), "arr": e.get("arr_time"), "from": e.get("from_name_en") or place_en(e.get("from_name")),
            "to": e.get("to_name_en") or place_en(e.get("to_name")), "minutes": e.get("minutes")}


def _public_alert(e: dict) -> dict:
    return {"id": _alert_key(e.get("alert_id", "")), "bus": show_bus(e.get("bus")), "alert": e.get("alert"),
            "label": ALERT_LABEL.get(e.get("alert"), e.get("alert")), "state": e.get("state", "opened"),
            "line": e.get("line"), "since": e.get("start_time"), "at": e.get("at_time"),
            "minutes": e.get("duration_min")}


def _alert_key(alert_id: str) -> str:
    """alert_id contains the device id; the page gets a short opaque key instead."""
    import hashlib
    return hashlib.sha256(alert_id.encode()).hexdigest()[:10]


class Tail:
    """Reads the lines appended to a JSONL file since the last call (and notices a reset)."""
    def __init__(self, path: Path, public):
        self.path, self.public = path, public

    def since(self, pos: int, keep: int = 60) -> tuple[list[dict], int]:
        f = self.path
        if not f.exists():
            return [], 0
        size = f.stat().st_size
        if pos > size:          # file was reset
            pos = 0
        if pos == size:
            return [], pos
        with f.open("rb") as fh:
            fh.seek(pos)
            chunk = fh.read(size - pos)
        lines = chunk.split(b"\n")
        tail = lines.pop()               # an incomplete last line waits for the next read
        out = []
        for raw in lines:
            try:
                out.append(self.public(json.loads(raw)))
            except Exception:
                continue
        return out[-keep:], size - len(tail)


TRIPS_TAIL = Tail(STATE / "trip-events.jsonl", _public_trip)
ALERTS_TAIL = Tail(STATE / "alerts.jsonl", _public_alert)


# ---------------- HTTP ----------------

def _token_ok(request: Request) -> bool:
    """True only for the right X-Admin-Token. No token configured = no controls for anyone,
    and the client address (127.0.0.1 behind the proxy, or an X-Forwarded-For header) counts for nothing."""
    if not ADMIN:
        return False
    given = request.headers.get("x-admin-token", "")
    return secrets.compare_digest(given.encode("utf-8"), ADMIN.encode("utf-8"))


def _admin(request: Request):
    if not ADMIN:
        raise HTTPException(403, "controls are off: VIZ_ADMIN_TOKEN is not set")
    if not _token_ok(request):
        raise HTTPException(403, "admin token required")


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/graph")
def graph(request: Request):
    return {"nodes": NODES, "edges": EDGES, "day": DAY, "controls": _token_ok(request), "raw": SAMPLES.NODE_SAMPLES}


@app.get("/api/sample/{node}")
def sample(node: str):
    """Real raw rows behind a source node, for every viewer (no token): read from data/inbox only,
    filtered by viz/samples.py (no receiver, no device name, bus anonymised, no Arabic text)."""
    ids = SAMPLES.NODE_SAMPLES.get(node)
    if not ids:
        raise HTTPException(404, "no raw sample for this node")
    built = SAMPLES.build(show_bus)
    return [built[i] for i in ids]


@app.get("/api/auth")
def auth(request: Request):
    """The page asks this before it shows the control panel."""
    _admin(request)
    return {"ok": True}


@app.get("/api/state")
def state():
    return snapshot()


@app.get("/api/stream")
async def stream(request: Request):
    async def gen():
        _, pos = TRIPS_TAIL.since(0)
        _, apos = ALERTS_TAIL.since(0)
        first = True
        while not await request.is_disconnected():
            snap = await asyncio.to_thread(snapshot)
            events, pos = await asyncio.to_thread(TRIPS_TAIL.since, pos)
            found, apos = await asyncio.to_thread(ALERTS_TAIL.since, apos)
            if first:                    # a new viewer gets the recent history
                events = TRIPS_TAIL.since(0)[0][-15:]
                found = ALERTS_TAIL.since(0, keep=400)[0]
                first = False
            snap["events"] = events
            snap["alerts"] = found
            yield f"data: {json.dumps(snap, ensure_ascii=False, default=str)}\n\n"
            await asyncio.sleep(1)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/code/{node}")
def code(node: str):
    meta = next((x for x in NODES if x["id"] == node), None)
    if not meta or not meta.get("code"):
        raise HTTPException(404, "no code for this node")
    out = []
    for ref in meta["code"]:
        path, _, func = ref.partition(":")
        src = (ROOT / path).read_text(encoding="utf-8")
        if func:
            tree = ast.parse(src)
            fn = next((n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name == func), None)
            seg = ast.get_source_segment(src, fn) if fn else None
            start = fn.lineno if fn else 1
            out.append({"file": path, "function": func, "line": start, "source": seg or src})
        else:
            out.append({"file": path, "function": None, "line": 1, "source": src})
    return out


@app.post("/api/control/{action}")
def control(action: str, request: Request, speed: float = 120, day: str | None = None):
    _admin(request)
    STATE.mkdir(parents=True, exist_ok=True)
    if action == "replay":
        PROCS.start("replay", ["stream.replay", DEMO_FILE, "--speed", str(speed), "--from", DEMO_FROM])
    elif action == "stop-replay":
        PROCS.stop("replay")
    elif action == "start-live":
        PROCS.start("live_trips", ["stream.live_trips"], {"LIVE_GROUP": PROCS.group, "LIVE_OFFSET_RESET": "latest"})
    elif action == "stop-live":                     # the lag demo: positions keep arriving, nobody reads them
        PROCS.stop("live_trips")
    elif action == "start-lake":
        PROCS.start("lake_sink", ["stream.lake_sink"])
    elif action == "stop-lake":
        PROCS.stop("lake_sink")
    elif action == "fallback":                      # no Kafka: same logic straight from the file
        PROCS.start("live_fallback", ["stream.live_trips", "--file", DEMO_FILE, "--speed", str(speed), "--from", DEMO_FROM])
    elif action == "stop-fallback":
        PROCS.stop("live_fallback")
    elif action == "reset":                         # a clean live demo: new consumer group, empty event feed
        for name in ["replay", "live_trips", "live_fallback"]:
            PROCS.stop(name)
        PROCS.group = f"live-trips-{int(time.time())}"
        for f in ["trip-events.jsonl", "alerts.jsonl", "replay.json", "live_trips.json"]:
            (STATE / f).unlink(missing_ok=True)
    elif action == "batch":
        BATCH.run(day or DAY)
    else:
        raise HTTPException(404, "unknown action")
    return {"ok": True, "procs": {x: PROCS.running(x) for x in ["replay", "live_trips", "live_fallback", "lake_sink"]},
            "group": PROCS.group}
