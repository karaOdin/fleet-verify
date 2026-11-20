"""Kafka numbers for the visual pipeline: total messages per topic (sum of end offsets)
and consumer-group lag. Short timeouts, cached for one second; never raises."""
from __future__ import annotations

import time

from stream.common import BROKER

_cache = {"t": 0.0, "key": None, "val": None}
_clients: dict = {}                 # one admin client and one consumer, reused (new connections every second are costly)


def _get_clients():
    from confluent_kafka import Consumer
    from confluent_kafka.admin import AdminClient
    if not _clients:
        _clients["admin"] = AdminClient({"bootstrap.servers": BROKER, "socket.timeout.ms": 1500})
        _clients["consumer"] = Consumer({"bootstrap.servers": BROKER, "group.id": "viz-metrics", "enable.auto.commit": False})
    return _clients["admin"], _clients["consumer"]


def _drop_clients():
    c = _clients.pop("consumer", None)
    _clients.clear()
    try:
        c and c.close()
    except Exception:
        pass


def read(topics: list[str], groups: list[str]) -> dict:
    key = (tuple(topics), tuple(groups))
    if _cache["key"] == key and time.time() - _cache["t"] < 1:
        return _cache["val"]
    val = _read(topics, groups)
    _cache.update(t=time.time(), key=key, val=val)
    return val


def _read(topics: list[str], groups: list[str]) -> dict:
    try:
        from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    except Exception as e:
        return {"ok": False, "error": f"confluent-kafka missing: {e}", "topics": {}, "lag": {}}
    try:
        admin, c = _get_clients()
        md = admin.list_topics(timeout=1.5)
        ends: dict[str, dict[int, int]] = {}
        for t in topics:
            if t not in md.topics:
                ends[t] = {}
                continue
            ends[t] = {p: c.get_watermark_offsets(TopicPartition(t, p), timeout=1.5)[1] for p in md.topics[t].partitions}
        lag = {}
        for g in groups:
            parts = [TopicPartition(t, p) for t in topics for p in ends.get(t, {})]
            if not parts:
                continue
            try:
                fut = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(g, parts)], request_timeout=1.5)
                res = list(fut.values())[0].result()
                committed = [tp for tp in res.topic_partitions if tp.offset >= 0]
                if committed:
                    lag[g] = sum(max(0, ends[tp.topic][tp.partition] - tp.offset) for tp in committed)
            except Exception:
                pass
        return {"ok": True, "topics": {t: sum(v.values()) for t, v in ends.items()}, "lag": lag}
    except Exception as e:
        _drop_clients()                 # reconnect next time (broker restarted, topics recreated, ...)
        return {"ok": False, "error": str(e)[:160], "topics": {}, "lag": {}}
