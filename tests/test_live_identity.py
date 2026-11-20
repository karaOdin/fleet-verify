"""Live uses the latest resolved identity from the last batch run, and a demo reset keeps it.

After tools.reset_demo deletes 19 Nov's results, live_trips must not fall back to the registry: the registry
puts device 146 on bus 22620, line 12, but it drove line 11 all day, and measured against line 12 it raised
false off_route alerts on stage."""
import polars as pl
import pytest

import stream.common as common
import tools.reset_demo as reset_demo

DAY = "2026-11-19"


@pytest.fixture
def data(tmp_path, monkeypatch, day):
    monkeypatch.setattr(common, "DATA", tmp_path)
    monkeypatch.setattr(reset_demo, "DATA", tmp_path)
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    wh = tmp_path / "warehouse" / "device_identity"
    wh.mkdir(parents=True)
    day["ident"].write_parquet(wh / f"{DAY}.parquet")
    return tmp_path


def test_identity_beats_registry(data, day):
    live = common.device_assignments()
    assert live.get(146, {}).get("line") != "12"                 # not the registry's (wrong) line
    assert live == common.identity_assignments(day["ident"])


def test_reset_keeps_the_latest_identity(data, day):
    before = common.device_assignments()
    done = reset_demo.reset(DAY, kafka=False)
    assert not (data / "warehouse" / "device_identity" / f"{DAY}.parquet").exists()     # the day is gone
    assert any("latest resolved identity kept" in d for d in done)
    assert common.device_assignments() == before                                      # live is unchanged
    assert common.device_assignments().get(146, {}).get("line") != "12"


def test_registry_only_without_any_batch_run(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "DATA", tmp_path)
    reg = common.device_assignments()
    assert reg[146] == {"bus": "22620", "line": "12"}                                 # what the registry says

