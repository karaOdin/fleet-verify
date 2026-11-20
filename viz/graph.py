"""The nodes and edges drawn on the page. x/y are in a 1260 x 580 drawing.
`code` points at the real source the node runs (file:function)."""

NODES = [
    # ---- live lane
    {"id": "replay", "label": "Replay / Traccar", "sub": "مصدر المواقع", "kind": "source", "x": 112, "y": 110,
     "code": ["stream/replay.py:main", "stream/traccar_poll.py:main"]},
    {"id": "positions", "label": "fleet.positions", "sub": "Kafka topic", "kind": "topic", "x": 318, "y": 110,
     "code": ["stream/common.py:message"]},
    {"id": "live_trips", "label": "live_trips", "sub": "Quix Streams · حالة لكل حافلة", "kind": "processor", "x": 524, "y": 110,
     "code": ["fleet/live.py:process", "stream/live_trips.py:step"]},
    {"id": "trip_events", "label": "fleet.trip-events", "sub": "Kafka topic", "kind": "topic", "x": 730, "y": 110,
     "code": ["stream/live_trips.py:run_kafka"]},
    {"id": "live_board", "label": "Live board", "sub": "رحلات لحظة حدوثها", "kind": "sink", "x": 936, "y": 110,
     "code": ["dashboard/app.py"]},
    {"id": "lake_sink", "label": "lake_sink", "sub": "Kafka ← Parquet", "kind": "processor", "x": 524, "y": 225,
     "code": ["stream/lake_sink.py:main"]},
    {"id": "lake", "label": "Lake", "sub": "Parquet · يوم لكل مجلد", "kind": "store", "x": 730, "y": 225,
     "code": ["stream/lake_sink.py:main"]},
    # ---- daily lane (Dagster)
    {"id": "cloudroutes", "label": "CloudRoutes API", "sub": "أوراق الطريق", "kind": "source", "x": 112, "y": 470,
     "code": ["sources/cloudroutes.py:road_papers", "sources/cloudroutes.py:_normalise_paper"]},
    {"id": "raw_positions", "label": "raw_positions", "sub": "Dagster · مواقع اليوم", "kind": "asset", "x": 112, "y": 360,
     "code": ["pipeline/assets.py:raw_positions"]},
    {"id": "road_papers", "label": "road_papers", "sub": "Dagster · رحلات مصرّح بها", "kind": "asset", "x": 318, "y": 470,
     "code": ["pipeline/assets.py:road_papers", "fleet/papers.py:flatten"]},
    {"id": "route_fit", "label": "route_fit", "sub": "أي خط يسير عليه الجهاز", "kind": "asset", "x": 318, "y": 360,
     "code": ["fleet/route_fit.py:route_fit"]},
    {"id": "gps_trips", "label": "gps_trips", "sub": "Polars · أنصاف الرحلات", "kind": "asset", "x": 524, "y": 360,
     "code": ["fleet/trips.py:zone_points", "fleet/trips.py:detect"]},
    {"id": "device_identity", "label": "device_identity", "sub": "ER · أي جهاز لأي حافلة", "kind": "asset", "x": 730, "y": 400,
     "code": ["fleet/identity.py:trip_scores", "fleet/identity.py:resolve"]},
    {"id": "trip_verification", "label": "trip_verification", "sub": "الورقة ↔ GPS رحلة برحلة", "kind": "asset", "x": 936, "y": 360,
     "code": ["fleet/match.py:verify", "fleet/match.py:_assign"]},
    {"id": "km_report", "label": "km_report", "sub": "كم تجاري / غير تجاري", "kind": "asset", "x": 936, "y": 470,
     "code": ["fleet/km.py:km_by_category"]},
    {"id": "bus_day_report", "label": "bus_day_report", "sub": "سطر لكل حافلة", "kind": "asset", "x": 1142, "y": 400,
     "code": ["pipeline/assets.py:bus_day_report"]},
    {"id": "postgres", "label": "PostgreSQL", "sub": "التقارير", "kind": "store", "x": 1142, "y": 510,
     "code": ["pipeline/pg.py:replace_day"]},
]

EDGES = [
    ["replay", "positions", "live"], ["positions", "live_trips", "live"], ["live_trips", "trip_events", "live"],
    ["trip_events", "live_board", "live"], ["positions", "lake_sink", "live"], ["lake_sink", "lake", "live"],
    ["lake", "raw_positions", "batch"], ["raw_positions", "route_fit", "batch"], ["route_fit", "gps_trips", "batch"],
    ["raw_positions", "gps_trips", "batch"], ["gps_trips", "device_identity", "batch"],
    ["cloudroutes", "road_papers", "batch"], ["road_papers", "device_identity", "batch"],
    ["road_papers", "trip_verification", "batch"], ["device_identity", "trip_verification", "batch"],
    ["gps_trips", "trip_verification", "batch"], ["gps_trips", "km_report", "batch"], ["device_identity", "km_report", "batch"],
    ["trip_verification", "bus_day_report", "batch"], ["km_report", "bus_day_report", "batch"],
    ["bus_day_report", "postgres", "batch"],
]
