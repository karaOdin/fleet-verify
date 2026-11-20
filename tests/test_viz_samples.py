"""Raw data samples on the source nodes: public (no admin token), read from data/inbox only,
and nothing personal, secret or Arabic in them."""
import json
import re

import pytest
from fastapi.testclient import TestClient

from viz import samples, server

ARABIC = re.compile(r"[؀-ۿ]")
PERSON = re.compile(r"receiver|driver|agent|controller|person|user|name\"\s*:\s*\"(?!Line )|note|comment|phone|email",
                    re.IGNORECASE)


def _env_secrets() -> list[str]:
    """Every token-like value the app could know: from .env and from the process environment."""
    import os
    from pathlib import Path
    vals = [v for k, v in os.environ.items() if any(w in k for w in ("TOKEN", "PASSWORD", "HASH", "SECRET"))]
    env = Path(server.ROOT) / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            k, _, v = line.partition("=")
            if any(w in k for w in ("TOKEN", "PASSWORD", "HASH", "SECRET")):
                vals.append(v.split("#")[0].strip().strip("'\""))
    return [v for v in vals if len(v) >= 6]


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(server, "ADMIN", None)          # controls off: the samples must still work
    return TestClient(server.app, client=("127.0.0.1", 50000))


@pytest.mark.parametrize("node,ids", sorted(samples.NODE_SAMPLES.items()))
def test_samples_are_public_and_clean(client, node, ids, monkeypatch):
    # never the live APIs: any HTTP call from the sample code fails the test
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: pytest.fail("a sample called a live API"))
    samples.build.cache_clear()
    r = client.get(f"/api/sample/{node}")
    assert r.status_code == 200
    got = r.json()
    assert [x["id"] for x in got] == ids
    text = json.dumps(got, ensure_ascii=False)
    assert not ARABIC.search(text)
    assert not PERSON.search(text), PERSON.search(text)
    for s in _env_secrets():
        assert s not in text
    for x in got:
        if x["format"] == "json":
            json.loads(x["text"])                        # still valid JSON


def test_paper_sample_fields(client):
    paper = json.loads(next(x for x in client.get("/api/sample/cloudroutes").json() if x["id"] == "paper")["text"])
    assert set(paper) <= set(samples.PAPER_FIELDS)
    assert "receiver_id" not in paper and "bus_id" not in paper
    assert paper["bus"].startswith("Bus ")
    assert 1 <= len(paper["trips"]) <= samples.PAPER_TRIPS
    assert all(set(t) <= set(samples.TRIP_FIELDS) for t in paper["trips"])


def test_gps_sample_is_raw_rows_of_one_device(client):
    gps = client.get("/api/sample/replay").json()[0]["text"].splitlines()
    assert gps[0] == "device_id,fix_time_utc,lat,lon,speed_kn"
    rows = [r.split(",") for r in gps[1:]]
    assert len(rows) == samples.GPS_ROWS and len({r[0] for r in rows}) == 1
    assert [r[1] for r in rows] == sorted(r[1] for r in rows)


def test_line_sample(client):
    line = json.loads(client.get("/api/sample/route_fit").json()[0]["text"])
    assert [t["end"] for t in line["terminals"]] == ["S", "E"]
    assert all(t["name_en"] and t["geofence_radius_m"] > 0 for t in line["terminals"])
    assert line["polyline_points"] > 1 and "polyline" not in line


def test_no_sample_for_other_nodes(client):
    assert client.get("/api/sample/postgres").status_code == 404
    assert client.get("/api/graph").json()["raw"] == samples.NODE_SAMPLES
