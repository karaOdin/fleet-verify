"""Regression: the numbers reported for 19 Nov 2026. Bus 22494 is the table that was
checked by hand (difference = written time minus GPS time)."""
import polars as pl


def test_trip_detection(day):
    t = day["trips"]
    assert t["device_id"].n_unique() == 50
    assert t.filter(pl.col("kind") == "trip").height == 578


def test_kilometres(day):
    k = day["km"]
    assert round(k["total_km"].sum(), 1) == 6907.5
    assert round(k["commercial_km"].sum(), 1) == 5208.5
    cats = ["commercial_km", "depot_to_first_km", "waiting_off_line_km", "at_terminals_km", "last_to_depot_km"]
    diff = (k["total_km"] - sum(k[c] for c in cats)).abs().max()
    assert diff <= 0.3            # categories add up to the total (per-category rounding)


def test_identity(day):
    i = {r["bus"]: r for r in day["ident"].iter_rows(named=True)}
    names = {r["id"]: r["name"] for r in day["devices"].iter_rows(named=True)}
    assert names[i["H030"]["resolved_device_id"]] == "620/11"     # paper matches another device
    assert names[i["22618"]["resolved_device_id"]] == "623/17"
    assert i["H031"]["verdict"] == "off_network"                   # same name as a Bou Saada bus
    assert i["H031"]["resolved_device_id"] is None
    assert i["22222"]["verdict"] == "no_device"
    assert i["H021"]["verdict"] == "confirmed"                     # device named line 17, drives line 10
    assert i["H021"]["resolved_line"] == "10"
    resolved = day["ident"].drop_nulls("resolved_device_id")
    assert resolved["resolved_device_id"].n_unique() == resolved.height   # one device per bus


def test_verification(day):
    v = day["ver"]
    assert v.filter(pl.col("paper_id").is_not_null()).height == 668
    counts = dict(v.group_by("status").len().iter_rows())
    assert counts == {"ok": 366, "ok_dep": 50, "diff": 116, "dup": 42, "missing": 14, "extra": 30, "nogps": 80}


def test_bus_22494_against_hand_check(day):
    v = day["ver"].filter((pl.col("bus") == "22494") & pl.col("paper_id").is_not_null()).sort("dep_time")
    assert v["gps_dep_time"].to_list() == ["08:27", "09:08", "10:22", "11:13", "12:31", "13:29", "14:55", "15:45", "16:59", "18:00"]
    assert v["delta_dep_min"].to_list() == [-10, 0, -15, 0, -6, 1, -5, 15, 0, 9]
    assert v["delta_arr_min"].to_list() == [0, 8, 1, 9, -4, 8, 3, 9, 10, 22]
