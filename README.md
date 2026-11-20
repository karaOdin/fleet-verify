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
| `fleet/alerts.py` | plain Python | Live alerts (off route, long stop outside a terminal, silent device), one point at a time |
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

## Live alerts

`stream.live_trips` also runs `fleet/alerts.py` on every position, with the same device state, and publishes
`opened` / `closed` events to `fleet.alerts` (and `data/state/alerts.jsonl`). Thresholds: `config/settings.json` → `alerts`.

| Alert | Rule |
|---|---|
| `off_route` | farther than 150 m from the line for more than 10 minutes in a row, outside the line's known places |
| `idle_outside_terminal` | under 3 km/h for more than 30 minutes in a row, outside the line's known places (on the route or off it) |
| `silent` | no position for more than 10 minutes from a device that was in service (moved on its line in the 30 minutes before, last seen not parked) |

Known places of a line: its terminal zones and its layover places (`config/settings.json` → `layovers`: centre and radius
of where buses wait between rounds, seeded from 19 Nov).

Nothing counts before the device first reaches a terminal of its line, nor at its overnight parking place.
Which bus and line each device stands for: live uses the latest resolved identity from the last batch run
(`device_identity`, what each device really drove), not the registry, as production would. `tools.reset_demo`
keeps a copy (`data/identity/latest.json`) before it deletes the day's results. The registry alone is wrong for
some devices: on 19 Nov device 146 is registered on bus 22620, line 12, but drove line 11 all day as H030.
The overnight parking place is the first position a device sends: a replay from 05:30 learns it later than
the whole day does, so the stage shows 32 off_route alerts where the whole day has 31 (device 145, 18:48).
`python -m tools.bench_live --mode file|kafka` measures how many positions per second `live_trips` handles.
On the VPS (4 vCPU, inside `fleet-viz`): 4,820 positions/s through Kafka; ×120 needs about 740/s at the morning peak.
`silent` runs on the stream's own clock (latest fix time), one clock per Kafka partition, so a replay behaves
like real time. Alerts are for the dispatcher to look at ("For the dispatcher to review"), never a sanction.
`docs/alerts-2026-11-19.md` lists every alert of 19 Nov and explains the rules against that day.

## Tests

```bash
pytest -q
```

- `test_day_2026_11_19.py`: the numbers of 19 Nov 2026 (668 declared trips, 6,907.5 km, 75 % commercial,
  the ER cases H030, 22618, H031, H021) and bus 22494 trip by trip as checked by hand.
- `test_live_matches_batch.py`: replays the day point by point through the stream logic and requires
  exactly the trips the batch finds.
- `test_alerts.py`: alert counts of 19 Nov, 22606's school transport after 14:10, layovers are not off route.
- `test_viz_security.py`, `test_viz_english.py`, `test_viz_samples.py`: controls need the token, the UI is English,
  the raw samples are public and carry no person field, token or Arabic text.
- `test_kafka_e2e.py` (needs a broker): two hours of 19 Nov through Kafka; trips equal the batch's, alerts equal the pure rules.
- `test_viz_browser.py` (needs the page running): the workshop run clicked in headless Chromium, screenshots in `docs/screens/`.
  `pip install playwright && python -m playwright install chromium`.

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

One page (English, LTR) with every node of the system. Terminal and place names are shown by their `name_en` (`config/terminals.json`, `layovers` in `config/settings.json`); `name_en` is display only and stays out of `settings_hash`. Each node reads its state from the real system:
heartbeats of the stream apps (`data/state/*.json`), Kafka end offsets and consumer lag,
the Dagster outputs in `data/warehouse`, PostgreSQL. Click a node to see the code it runs
(read from the source files, so it is always the code that actually runs). The source nodes (Replay / Traccar, CloudRoutes API,
raw_positions, road_papers, route_fit) open first on their raw data: 8 GPS rows of one device as stored, one road
paper (paper 6477: its directions are written the other way round to the GPS), and its line's terminals; read from
`data/inbox` only (`viz/samples.py`), public, filtered by allow-list.

Buttons need the header `X-Admin-Token: $VIZ_ADMIN_TOKEN`. Without the variable every control request is refused,
from any address: behind a reverse proxy every request comes from 127.0.0.1, so the client address never counts.
Viewers do not see the panel at all; the presenter opens the page once as `/#admin=<token>` (the fragment never
reaches the server or its logs, the page keeps the token in the browser), `/#lock` forgets it.

| Button | What happens |
|---|---|
| Replay the day into Kafka | `stream.replay` sends the day into `fleet.positions` from `VIZ_DEMO_FROM` at the chosen speed |
| live_trips ▶ / ⏸ | start or stop the stream processor; stopped, the lag on its node grows; restarted, it catches up |
| lake_sink ▶ | positions to the Parquet lake |
| Run the batch | runs the Dagster job for the day; asset nodes light up as each step runs, then the four checks |
| Clean start | new consumer group, empty feed |
| No Kafka | the same live logic straight from the file, paced: the fallback if Kafka fails on stage |

Bus numbers are shown as "Bus A, B, …" while `VIZ_ANONYMIZE=1`.

## Deploy on a VPS

The shared VPS (Ploi, nginx on 80/443 for other sites) runs `docker-compose.vps.yml`, project `fleet`, containers `fleet-*`:

```bash
cp .env.example .env      # POSTGRES_PASSWORD, VIZ_ADMIN_TOKEN, tokens
./run-vps.sh up           # kafka, postgres, viz (127.0.0.1:8000), lake-sink
./run-vps.sh stop         # stop everything, data kept
./run-vps.sh reset        # back to the start of the demo (tools.reset_demo --kafka)
./run-vps.sh dagster start|stop   # Dagster UI on 127.0.0.1:3001, only while needed
./run-vps.sh status       # containers, ports, memory
```

Every published port binds to 127.0.0.1 (Docker-published ports bypass ufw); Kafka and PostgreSQL publish none.
The host's nginx proxies `pipeline.devcloud.dz` to 127.0.0.1:8000; Dagster is reached through an SSH tunnel
(`deploy/DEPLOY.md`: nginx blocks, DNS, unlock, morning checklist). No kafka-ui, dagster-daemon or Superset on this server: each container has a `mem_limit`,
and the stack measured 473 MiB idle, 818 MiB during a ×120 replay, 1,011 MiB during the batch (limit 2.2 GiB).
