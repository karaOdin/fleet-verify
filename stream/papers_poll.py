"""Road papers as they change: poll the CloudRoutes data API every 120 s with
updated_since = the latest changed_at seen, and publish each paper to Kafka
(topic fleet.road-papers, key = paper id, so a compacted topic keeps the latest
version of every paper).

    python -m stream.papers_poll

The cursor is saved in data/state/papers_cursor.txt so a restart resumes where it
stopped. The bound is inclusive, so the same paper may be sent again: consumers
upsert by id. Deleted papers are not streamed; the daily Dagster run replaces the
whole day and removes them. Verdicts on draft/submitted papers are provisional.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta

from confluent_kafka import Producer

from sources import cloudroutes
from stream.heartbeat import Heartbeat
from stream.common import BROKER, DATA, TZ

TOPIC = os.environ.get("TOPIC_ROAD_PAPERS", "fleet.road-papers")
CURSOR = DATA / "state" / "papers_cursor.txt"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--every", type=float, default=120)
    ap.add_argument("--since", help="ISO-8601 start (default: saved cursor, else today 00:00 local)")
    args = ap.parse_args()
    if not cloudroutes.data_api_configured():
        raise SystemExit("Set CLOUDROUTES_TENANT_URL and CLOUDROUTES_DATA_TOKEN in the environment.")
    CURSOR.parent.mkdir(parents=True, exist_ok=True)
    since = args.since or (CURSOR.read_text().strip() if CURSOR.exists() else
                           datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0).isoformat())
    p = Producer({"bootstrap.servers": BROKER})
    hb, total = Heartbeat("papers_poll", every=0), 0
    while True:
        try:
            papers = cloudroutes.papers_changed_since(since)
            for paper in papers:
                p.produce(TOPIC, key=str(paper["id"]), value=json.dumps(paper, ensure_ascii=False).encode())
            p.flush(10)
            if papers:
                since = max(x["changed_at"] for x in papers if x.get("changed_at"))
                CURSOR.write_text(since)
            total += len(papers)
            hb.beat(last_batch=len(papers), total=total)
            print(f"{time.strftime('%H:%M:%S')} {len(papers)} papers changed (cursor {since})", flush=True)
        except Exception as e:      # network or API errors: keep the cursor, try again next round
            print(f"poll failed: {e}", flush=True)
        time.sleep(args.every)


if __name__ == "__main__":
    main()
