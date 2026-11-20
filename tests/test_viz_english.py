"""The UI is English only; Arabic stays in the data (terminal and place names), shown through name_en."""
import json
import re

from fastapi.testclient import TestClient

from fleet import settings as S
from viz import server

ARABIC = re.compile(r"[؀-ۿ]")


def test_page_and_graph_have_no_arabic():
    c = TestClient(server.app)
    html = c.get("/").text
    assert not ARABIC.search(html)
    assert '<html lang="en" dir="ltr">' in html
    assert not ARABIC.search(json.dumps(c.get("/api/graph").json(), ensure_ascii=False))


def test_public_events_are_english():
    trip = server._public_trip({"bus": "22494", "event": "trip", "line": "05", "dep_time": "08:18", "arr_time": "08:51",
                                "from_name": "لاروكاد", "to_name": "القطب الجامعي", "minutes": 33})
    assert (trip["from"], trip["to"]) == ("La Rocade", "University Pole")
    assert trip["bus"].startswith("Bus ")
    for kind in ["off_route", "idle_outside_terminal", "silent"]:
        a = server._public_alert({"alert_id": "x", "bus": "22494", "alert": kind, "state": "opened"})
        assert not ARABIC.search(json.dumps(a, ensure_ascii=False)), a


def test_every_place_has_an_english_name():
    for line, t in S.terminals().items():
        for end in ("S", "E"):
            assert t["name_en"][end] and not ARABIC.search(t["name_en"][end]), (line, end)
    for line, places in S.settings()["layovers"].items():
        for p in places if isinstance(places, list) else []:
            assert p["name"] and p["name_en"] and not ARABIC.search(p["name_en"]), line


def test_display_names_stay_out_of_the_settings_hash():
    cfg = S.settings()
    stripped = S._without_display(cfg)
    assert "name_en" not in json.dumps(stripped)
    assert stripped["layovers"]["04"][0]["name"] == cfg["layovers"]["04"][0]["name"]   # the data name stays
    assert S.settings_hash() == "8853213ce5ca"         # the hash of the rules before name_en was added
