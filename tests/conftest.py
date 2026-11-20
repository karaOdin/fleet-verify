import csv
import json
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
csv.field_size_limit(10**9)
INBOX = ROOT / "data" / "inbox"
DAY = "2026-11-19"


@pytest.fixture(scope="session")
def day():
    """Everything for 19 Nov 2026, computed once with the library functions."""
    from fleet.settings import settings, plane, terminals
    from fleet.lines import lines_frame, build_lines
    from fleet.positions import normalise, with_xy
    from fleet.route_fit import route_fit, best_line
    from fleet.trips import zone_points, detect
    from fleet.papers import flatten
    from fleet.identity import resolve
    from fleet.match import verify
    from fleet.km import km_by_category

    cfg = settings()
    lines = build_lines(lines_frame(list(csv.DictReader(open(INBOX / "lines.csv", encoding="utf-8")))), plane())
    pos = with_xy(normalise(pl.read_csv(INBOX / f"positions_{DAY}.csv.gz"), cfg["timezone"]), plane())
    best = best_line(route_fit(pos, lines, cfg), cfg["route_fit"]["min_f"])
    trips = detect(zone_points(pos, lines, best.filter("on_network").select("device_id", "line"), cfg), cfg)
    from sources.cloudroutes import road_papers as load_papers
    papers = flatten(load_papers(DAY))
    reg = pl.DataFrame(json.loads((INBOX / "fleet_registry.json").read_text())["buses"],
                       schema={"bus": pl.Utf8, "line": pl.Utf8, "traccar_device_id": pl.Int64})
    devices = pl.DataFrame(json.loads((INBOX / "devices.json").read_text())).select("id", "name", "status")
    ident = resolve(reg, devices, best, trips, papers, cfg)
    ver = verify(papers, trips, ident, terminals(), cfg)
    km = km_by_category(pos, trips, cfg["max_speed_kmh"])
    return dict(cfg=cfg, lines=lines, pos=pos, best=best, trips=trips, papers=papers,
                ident=ident, ver=ver, km=km, devices=devices)
