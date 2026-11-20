"""Live alerts on 19 Nov 2026, replayed point by point in time order (as Kafka delivers them).

Checked against what is known about that day:
  - 22606's paper says it went to school transport after 14:10: off_route from 14:12 for the rest of the day.
  - every bus of lines 04 and 05 waits ~600 m off the route near their shared terminal between trips;
    that is a layover (standing still) and must not raise off_route.
"""
from collections import Counter

import pytest

from alerts_replay import replay_alerts


@pytest.fixture(scope="module")
def found(day):
    bus = {r["resolved_device_id"]: r["bus"] for r in day["ident"].drop_nulls("resolved_device_id").iter_rows(named=True)}
    ev = replay_alerts(day)
    for e in ev:
        e["bus"] = bus.get(e["device_id"])
    return ev


def test_counts_per_type(found):
    opened = Counter(e["alert"] for e in found if e["state"] == "opened")
    assert dict(opened) == {"off_route": 27, "silent": 8}
    assert opened["idle_outside_terminal"] == 0


def test_school_transport_22606(found):
    mine = [e for e in found if e["bus"] == "22606" and e["alert"] == "off_route" and e["state"] == "opened"]
    after = [e for e in mine if e["start"] >= 14 * 3600 + 10 * 60]
    assert len(after) == 1
    closed = next(e for e in found if e["alert_id"] == after[0]["alert_id"] and e["state"] == "closed")
    assert closed["duration_min"] > 240                 # gone for the rest of the afternoon


def test_layovers_are_not_off_route(found):
    lines = Counter(e["line"] for e in found if e["alert"] == "off_route" and e["state"] == "opened")
    assert lines.get("05", 0) == 0
    buses_04 = {e["bus"] for e in found if e["alert"] == "off_route" and e["line"] == "04"}
    assert buses_04 <= {"22494", "H015"}                # 22494 drives (not parks) another road near the terminal


def test_opened_once_closed_once(found):
    ids = Counter((e["alert_id"], e["state"]) for e in found)
    assert max(ids.values()) == 1                      # deduplicated
    opened = {e["alert_id"] for e in found if e["state"] == "opened"}
    closed = {e["alert_id"]: e for e in found if e["state"] == "closed"}
    assert set(closed) <= opened                       # nothing closes that never opened
    assert all(c["duration_min"] > 0 for c in closed.values())
    assert len(opened - set(closed)) <= 3              # only alerts still open when service ends


def test_rewind_restarts_the_clock(day):
    """A day replayed again (time jumps back) must not make every device `silent`."""
    from fleet import alerts
    cfg = day["cfg"]
    clock, st, out = alerts.clock_init(), {"last_on_line": True, "last_in_terminal": False}, []
    for s in range(8 * 3600, 9 * 3600, 10):                     # two buses driving on their line, 08:00-09:00
        for dev in (1, 2):
            out += alerts.observe(clock, {"device_id": dev, "s": s, "kmh": 30.0}, "10", st, cfg)
            out += alerts.check_silent(clock, s, cfg)
    assert out == []
    for s in range(5 * 3600, 5 * 3600 + 1200, 10):              # the replay starts again at 05:00, bus 1 only
        out += alerts.observe(clock, {"device_id": 1, "s": s, "kmh": 30.0}, "10", st, cfg)
        out += alerts.check_silent(clock, s, cfg)
    assert out == []                                            # bus 2 is not "silent since 09:00"
