"""Clean start on the page empties the live feed AND the day's batch results, so Run the batch fills the page
and the dashboard again on stage. The latest resolved identity is kept for live."""
import json

import polars as pl
import pytest
from fastapi.testclient import TestClient

import tools.reset_demo as reset_demo
from viz import server

TOKEN = "clean-start-test-token"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "ADMIN", TOKEN)
    monkeypatch.setattr(server, "STATE", tmp_path / "state")
    monkeypatch.setattr(reset_demo, "DATA", tmp_path)
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "trip-events.jsonl").write_text("{}\n")
    for a in reset_demo.DAILY_ASSETS:
        (tmp_path / "warehouse" / a).mkdir(parents=True)
        (tmp_path / "warehouse" / a / f"{server.DAY}.parquet").write_bytes(b"x")
    (tmp_path / "warehouse" / "lines").mkdir()
    (tmp_path / "warehouse" / "lines" / "all.parquet").write_bytes(b"x")          # not a daily result: kept
    pl.DataFrame({"bus": ["22620"], "resolved_device_id": [146], "resolved_line": ["11"]}).write_parquet(
        tmp_path / "warehouse" / "device_identity" / f"{server.DAY}.parquet")
    return tmp_path


def _post(c, action):
    return c.post(f"/api/control/{action}", headers={"X-Admin-Token": TOKEN})


def test_clean_start_clears_the_day_results(env):
    server.BATCH.reset()
    server.BATCH.ok, server.BATCH.checks = True, {"x": {"passed": True}}
    r = _post(TestClient(server.app), "reset")
    assert r.status_code == 200
    assert not (env / "state" / "trip-events.jsonl").exists()
    for a in reset_demo.DAILY_ASSETS:
        assert not (env / "warehouse" / a / f"{server.DAY}.parquet").exists(), a
    assert (env / "warehouse" / "lines" / "all.parquet").exists()
    saved = json.loads((env / "identity" / "latest.json").read_text())
    assert saved["assignments"]["146"] == {"bus": "22620", "line": "11"}
    assert server.BATCH.ok is None and server.BATCH.checks == {}


def test_clean_start_waits_for_a_running_batch(env):
    server.BATCH.reset()
    server.BATCH.running = True
    try:
        assert _post(TestClient(server.app), "reset").status_code == 409
        assert (env / "warehouse" / "trip_verification" / f"{server.DAY}.parquet").exists()
    finally:
        server.BATCH.reset()
