"""The workshop run, clicked in a real browser (headless Chromium):
Clean start → live_trips ▶ → Replay the day → trips and an alert in the feed → ⏸ (lag rises) → ▶ (lag falls)
→ Run the batch → the four checks. A screenshot of each step goes to docs/screens/.

Skipped unless the page answers at VIZ_URL (default http://localhost:8000) with Kafka up.
It drives the real demo: it starts with a clean start and leaves the day's batch results in place,
so run `python -m tools.reset_demo --kafka` before the talk.

    uvicorn viz.server:app --port 8000 &   pytest -q tests/test_viz_browser.py   (VIZ_ADMIN_TOKEN set in both)
"""
import json
import os
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

URL = os.environ.get("VIZ_URL", "http://localhost:8000").rstrip("/")
TOKEN = os.environ.get("VIZ_ADMIN_TOKEN", "")
SHOTS = Path(__file__).resolve().parent.parent / "docs" / "screens"


def _state() -> dict | None:
    try:
        with urllib.request.urlopen(URL + "/api/state", timeout=5) as r:
            return json.load(r)
    except Exception:
        return None


def _ready() -> bool:
    s = _state()
    return bool(s and s["kafka"]["ok"])


pytestmark = [pytest.mark.skipif(not _ready(), reason=f"viz page not running at {URL} (or Kafka down)"),
              pytest.mark.skipif(not TOKEN, reason="VIZ_ADMIN_TOKEN not set: the page shows no controls")]


def _wait(cond, timeout: float, every: float = 1.0, what: str = ""):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        v = cond()
        if v:
            return v
        time.sleep(every)
    raise AssertionError(f"timed out after {timeout:.0f} s waiting for {what}")


def _lag_on_node(page) -> int:
    """The lag number written on the live_trips node, as the audience reads it."""
    nodes = page.locator("#nodes g.node")
    for i in range(nodes.count()):
        if nodes.nth(i).get_attribute("aria-label") == "live_trips":
            text = nodes.nth(i).locator("text.metric").text_content() or ""
            m = re.search(r"lag ([\d,]+)", text)
            return int(m.group(1).replace(",", "")) if m else 0
    raise AssertionError("live_trips node not found")


def test_workshop_run_in_browser():
    from playwright.sync_api import expect, sync_playwright

    SHOTS.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pw:
        try:                                       # the full Chromium build, headless
            browser = pw.chromium.launch(channel="chromium")
        except Exception:                          # or Playwright's headless shell
            browser = pw.chromium.launch()
        # reduced motion: no moving dots, so software rendering in headless Chromium does not compete
        # with live_trips for the CPU (the dots respect the viewer's motion setting anyway)
        page = browser.new_page(viewport={"width": 1366, "height": 900}, locale="ar", reduced_motion="reduce")
        page.goto(URL + "/", wait_until="domcontentloaded")
        expect(page.locator("#k-txt")).to_have_text("Kafka connected", timeout=20_000)
        # a viewer sees no controls; a wrong token does not show them either
        expect(page.locator("#ctl")).to_be_hidden()
        page.goto(URL + "/#admin=not-the-token", wait_until="domcontentloaded")
        page.wait_for_timeout(1_500)
        expect(page.locator("#ctl")).to_be_hidden()
        # the presenter opens the page once with the token in the fragment; it leaves the address bar
        page.goto(URL + "/#admin=" + urllib.parse.quote(TOKEN, safe=""), wait_until="domcontentloaded")
        expect(page.locator("#ctl")).to_be_visible(timeout=10_000)
        assert "#admin" not in page.url
        # x60: the lag must fall because the consumer is faster than the input, whatever else runs on the
        # machine. At x120 live_trips is close to its limit on a busy laptop (see WORKSHOP.md).
        expect(page.locator("#speed")).to_have_value("120")      # the page's default
        page.select_option("#speed", "60")
        msg = page.locator("#msg")

        def click(action: str):
            page.click(f'button[data-a="{action}"]')
            expect(msg).to_contain_text("✓", timeout=15_000)

        page.screenshot(path=str(SHOTS / "01-page.png"), full_page=True)

        click("reset")
        click("start-live")
        _wait(lambda: (_state() or {}).get("procs", {}).get("live_trips"), 20, what="live_trips to run")
        page.wait_for_timeout(8_000)               # the new consumer group joins before the first message
        click("replay")

        # trips and at least one alert reach the page
        expect(page.locator("#feed li:not(.empty)").first).to_be_visible(timeout=120_000)
        expect(page.locator("#alerts li:not(.empty)").first).to_be_visible(timeout=120_000)
        _wait(lambda: page.locator("#feed li:not(.empty)").count() >= 3, 60, what="three trips in the feed")
        assert "For the dispatcher to review" in page.locator(".panels").inner_text()
        assert "violation" not in page.locator("#alerts").inner_text().lower()
        assert not re.search(r"[\u0600-\u06FF]", page.locator("body").inner_text()), "Arabic text on the page"
        page.screenshot(path=str(SHOTS / "02-trips-and-alerts.png"), full_page=True)

        # pause: positions keep arriving, nobody reads them, the lag on the node rises
        click("stop-live")
        page.wait_for_timeout(6_000)
        lag1 = _lag_on_node(page)
        page.wait_for_timeout(8_000)
        lag2 = _lag_on_node(page)
        assert lag2 > lag1 > 0, (lag1, lag2)
        page.screenshot(path=str(SHOTS / "03-paused-lag-rises.png"), full_page=True)

        # resume: it catches up without losing anything
        click("start-live")
        _wait(lambda: _lag_on_node(page) < lag2 / 2, 300, what="the lag to fall")
        page.screenshot(path=str(SHOTS / "04-resumed-lag-falls.png"), full_page=True)

        # the daily batch: asset nodes light up, then the four checks
        click("stop-replay")
        click("batch")
        _wait(lambda: (_state() or {}).get("batch", {}).get("running"), 20, what="the batch to start")
        batch = _wait(lambda: (lambda b: b if not b.get("running") else None)((_state() or {}).get("batch", {})),
                      600, 2, what="the batch to finish")
        assert batch["ok"], batch["log"]
        expect(page.locator("#checks li")).to_have_count(4, timeout=10_000)
        page.wait_for_timeout(2_000)
        assert page.locator("#checks .dot.on").count() == 4
        assert page.locator("#kpis .kpi").count() == 6
        page.screenshot(path=str(SHOTS / "05-batch-checks.png"), full_page=True)

        click("stop-live")
        browser.close()
