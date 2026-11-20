# fleet-verify

Checks every bus's road paper against its GPS, every day, and follows trips live.

```
                 ┌──────────────── live (seconds) ────────────────┐
Traccar ─poll─▶ Kafka fleet.positions ─▶ stream.live_trips ─▶ Kafka fleet.trip-events ─▶ dashboard (Live)
                 │                       (Quix Streams, state per device)
                 └─▶ stream.lake_sink ─▶ data/lake/positions/date=…/*.parquet
                                                   │
                 ┌──────────── daily (02:30) ──────┼───────────────────────────────┐
CloudRoutes ─▶ road_papers, lines, registry        ▼                                │
                 Dagster: raw_positions ─▶ route_fit ─▶ gps_trips ─▶ device_identity (ER)
                          ─▶ trip_verification, km_report ─▶ bus_day_report ─▶ PostgreSQL
                                                                     └─▶ dashboard (Daily)
```

| Part | Tool | What it does |
|---|---|---|
| `fleet/` | Polars + numpy | All the rules, as pure functions. No I/O. |
| `fleet/trips.py` | Polars | Terminal episodes → half-trips, excursions, loops |
| `fleet/identity.py` | Polars | **Entity resolution**: which device really drove each bus today |
| `fleet/match.py` | scipy | Declared trip ↔ GPS trip, one to one (Hungarian method) |
| `fleet/km.py` | Polars (`join_asof`) | Commercial / non-commercial km |
| `fleet/live.py` | plain Python | The same trip rules, one point at a time, for the stream |
| `stream/` | Kafka, Quix Streams | Live trips, raw positions to the lake |
| `pipeline/` | Dagster | Daily partitions, asset checks, schedule, PostgreSQL export |
| `dashboard/` | Streamlit | Daily report and live events |

## Run

```bash
pip install -r requirements.txt
cp .env.example .env            # fill in, then: set -a; source .env; set +a
docker compose up -d            # Kafka :9092, Kafka console :8080, PostgreSQL :5432
```

**Daily verification (no Kafka needed)**

```bash
dagster dev -m pipeline.definitions          # http://localhost:3000
# Materialize partition 2026-11-19, or from the shell:
dagster job execute -m pipeline.definitions -j daily_verification --tags '{"dagster/partition":"2026-11-19"}'
streamlit run dashboard/app.py
```

Positions for a day are read from the Kafka lake if the stream wrote it, else from
`data/inbox/positions_<day>.csv.gz`, else fetched from the Traccar API.
Road papers come from the CloudRoutes API when `CLOUDROUTES_URL` is set,
else from `data/inbox/road_papers_<day>.json`.

**Live**

```bash
python -m stream.live_trips          # window 1: trips as they happen
python -m stream.lake_sink           # window 2: keeps every position for the daily run
python -m stream.papers_poll         # road papers as receivers submit them
python -m stream.traccar_poll        # window 3: real buses ...
python -m stream.replay data/inbox/positions_2026-11-19.csv.gz --speed 120   # ... or replay a day
```

Without Kafka (same logic): `python -m stream.live_trips --file data/inbox/positions_2026-11-19.csv.gz`

## Tests

```bash
pytest -q
```

- `test_day_2026_11_19.py`: the numbers of 19 Nov 2026 (668 declared trips, 6,907.5 km, 75 % commercial,
  the ER cases H030, 22618, H031, H021) and bus 22494 trip by trip as checked by hand.
- `test_live_matches_batch.py`: replays the day point by point through the stream logic and requires
  exactly the trips the batch finds.

## Inputs CloudRoutes must provide

| Endpoint | Status | Returns |
|---|---|---|
| `GET {tenant}/api/data/v1/road-papers` | **built** (`feat/data-corrections`) | `{data: [{id, work_date, shift, status, bus, line, receiver_id, changed_at, trips: [{trip_number, direction, departure_time, arrival_time, tickets}]}], meta: {next_page, ...}}` |
| `GET /api/v1/buses` | to build | `{buses: [{bus, line, traccar_device_id}]}` (the device **id**, never matched by name) |
| `GET /api/v1/lines` | to build | `{lines: [{id, name, waypoints, start_geofence, end_geofence}]}` |

Road papers: the daily run fetches the whole `date` (all pages) and replaces the day, which also drops
papers deleted in CloudRoutes. `python -m stream.papers_poll` follows changes every 120 s with
`updated_since` and publishes them to `fleet.road-papers`. Night-shift times earlier than the first
trip are moved to the next day (24:xx).

`data/inbox/*.json` shows each format with the real data of 19 Nov.

## Settings

`config/settings.json` holds every threshold (terminal radius, gap between episodes, match tolerances,
ER margins). Each output row carries `settings_hash`, so a report always says which rules produced it.
`config/terminals.json` names each line's two terminals.

## Reading the results

- Difference = written time minus GPS time. `+` the paper is later than the GPS, `−` earlier.
- `direction_differs` is a note, not a verdict: papers write aller/retour inconsistently (52–65 % agreement).
- ER verdicts other than `confirmed` go to a person. Results are recommendations for review,
  never automatic sanctions (personal data of workers, law 18-07).

## The visual pipeline

```bash
uvicorn viz.server:app --port 8000        # http://localhost:8000
```

One page with every node of the system. Each node reads its state from the real system:
heartbeats of the stream apps (`data/state/*.json`), Kafka end offsets and consumer lag,
the Dagster outputs in `data/warehouse`, PostgreSQL. Click a node to see the code it runs
(read from the source files, so it is always the code that actually runs).

Buttons (header `X-Admin-Token: $VIZ_ADMIN_TOKEN`; without the variable only from the same machine):

| Button | What happens |
|---|---|
| إعادة اليوم في Kafka | `stream.replay` sends the day into `fleet.positions` from `VIZ_DEMO_FROM` at the chosen speed |
| live_trips ▶ / ⏸ | start or stop the stream processor; stopped, the lag on its node grows; restarted, it catches up |
| lake_sink ▶ | positions to the Parquet lake |
| تشغيل الـbatch | runs the Dagster job for the day; asset nodes light up as each step runs, then the four checks |
| بداية نظيفة | new consumer group, empty feed |
| بدون Kafka | the same live logic straight from the file, paced: the fallback if Kafka fails on stage |

Bus numbers are shown as "حافلة A, B, …" while `VIZ_ANONYMIZE=1`.

## Deploy on a VPS

```bash
cp .env.example .env      # DOMAIN, ADMIN_USER, ADMIN_HASH, POSTGRES_PASSWORD, VIZ_ADMIN_TOKEN, tokens
docker compose -f docker-compose.vps.yml up -d --build
```

DNS: `pipeline.`, `dagster.` and `kafka.` records of `$DOMAIN` pointing at the VPS. Caddy gets the
certificates. The pipeline page is public (controls need the token); Dagster and the Kafka console sit
behind basic auth. Kafka and PostgreSQL publish no ports.
