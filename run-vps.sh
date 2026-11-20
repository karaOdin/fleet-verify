#!/usr/bin/env bash
# The fleet stack on the VPS (docker-compose.vps.yml, project "fleet", containers fleet-*).
#
#   ./run-vps.sh up                 build if needed and start (kafka, postgres, viz, lake-sink, dashboard)
#   ./run-vps.sh prepare            before the talk: up, Dagster web UI on, demo reset, status
#   ./run-vps.sh stop               stop every fleet container (volumes and data kept)
#   ./run-vps.sh reset              back to the start of the demo: stops what the page started, clears the
#                                   live state, day 19's results, the Kafka topics and every consumer group's
#                                   offsets (tools/reset_demo --kafka)
#   ./run-vps.sh dagster start|stop the Dagster web UI on 127.0.0.1:3001, only while you need it
#   ./run-vps.sh status             containers, published ports, memory (docker stats)
#   ./run-vps.sh logs [service]     follow the logs
#   ./run-vps.sh build              rebuild the app image after a code change, then restart the app containers
#
# Never touches anything outside the "fleet" project: no nginx, no other site, no host service.
set -euo pipefail
cd "$(dirname "$0")"
DC=(docker compose -f docker-compose.vps.yml)

[ -f .env ] || { echo ".env missing (cp .env.example .env and fill it in)" >&2; exit 1; }
grep -qE '^VIZ_ADMIN_TOKEN=.+' .env || echo "warning: VIZ_ADMIN_TOKEN is empty: the page's controls are off" >&2

wait_viz() {
  for _ in $(seq 1 60); do
    curl -fsS -o /dev/null http://127.0.0.1:8000/api/state 2>/dev/null && { echo "viz: http://127.0.0.1:8000"; return; }
    sleep 2
  done
  echo "viz did not answer on 127.0.0.1:8000 (./run-vps.sh logs viz)" >&2; return 1
}

wait_dashboard() {
  for _ in $(seq 1 60); do
    curl -fsS -o /dev/null http://127.0.0.1:8501/_stcore/health 2>/dev/null && { echo "dashboard: http://127.0.0.1:8501"; return; }
    sleep 2
  done
  echo "dashboard did not answer on 127.0.0.1:8501 (./run-vps.sh logs dashboard)" >&2; return 1
}

case "${1:-}" in
  up)
    "${DC[@]}" up -d --build
    wait_viz
    wait_dashboard ;;
  prepare)                          # before the talk
    "$0" up
    "$0" dagster start                # dag-work.devcloud.dz (password) during the talk; ~310 MiB more
    "$0" reset
    "$0" status ;;
  stop|down)
    "${DC[@]}" --profile dagster stop ;;
  reset)
    "${DC[@]}" stop lake-sink         # a group with a running consumer cannot be deleted
    "${DC[@]}" restart viz            # ends replay / live_trips / fallback started from the page
    "${DC[@]}" exec -T viz python -m tools.reset_demo --kafka    # topics recreated, every consumer group deleted
    "${DC[@]}" start lake-sink
    wait_viz ;;
  dagster)
    case "${2:-}" in
      start) "${DC[@]}" --profile dagster up -d dagster-web
             echo "dagster: http://127.0.0.1:3001 (from your machine: ssh -L 3001:127.0.0.1:3001 ploi@95.111.253.252)" ;;
      stop)  "${DC[@]}" --profile dagster stop dagster-web ;;
      *) echo "usage: $0 dagster start|stop" >&2; exit 2 ;;
    esac ;;
  status)
    "${DC[@]}" --profile dagster ps --format 'table {{.Name}}\t{{.Status}}\t{{.Ports}}'
    docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}' \
      $("${DC[@]}" --profile dagster ps -q)
    echo "page http://127.0.0.1:8000 · dashboard http://127.0.0.1:8501 · dagster http://127.0.0.1:3001 (when started)" ;;
  logs)
    shift; "${DC[@]}" logs -f --tail 100 "$@" ;;
  build)
    "${DC[@]}" build
    "${DC[@]}" up -d
    wait_viz ;;
  *)
    sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
