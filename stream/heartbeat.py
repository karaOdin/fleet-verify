"""Each stream app reports what it is doing to data/state/<name>.json (at most once a
second). The visual pipeline reads these files, so it shows real activity even when
it cannot reach Kafka, and every app stays independent of the viewer."""
from __future__ import annotations

import json
import os
import time

from stream.common import DATA

STATE = DATA / "state"


class Heartbeat:
    def __init__(self, name: str, every: float = 1.0):
        self.name, self.every, self.last = name, every, 0.0
        self.started = time.time()
        STATE.mkdir(parents=True, exist_ok=True)
        self.path = STATE / f"{name}.json"

    def beat(self, force: bool = False, **metrics) -> None:
        now = time.time()
        if not force and now - self.last < self.every:
            return
        self.last = now
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"name": self.name, "pid": os.getpid(), "started": self.started,
                                   "updated": now, **metrics}, ensure_ascii=False, default=str))
        tmp.replace(self.path)

    def stop(self, **metrics) -> None:
        self.beat(force=True, stopped=True, **metrics)


EVENTS = STATE / "trip-events.jsonl"


def append_events(events: list[dict]) -> None:
    """Trip events also go to a local file, so the viewer works with or without Kafka."""
    if not events:
        return
    STATE.mkdir(parents=True, exist_ok=True)
    with EVENTS.open("a", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
