# Deploy on the shared VPS

One Contabo VPS managed by Ploi (4 vCPU, 3.8 GiB RAM), shared with other sites. nginx (Ploi) owns 80/443;
MySQL, Redis and memcached run on the host. The fleet stack never touches them, nginx's files, or any other site.
The old Traccar instance on this host stays stopped.

```
browser ──https──▶ nginx (Ploi) ──▶ 127.0.0.1:8000  fleet-viz          pipeline.devcloud.dz (public, controls need the token)
                                └─▶ 127.0.0.1:3001  fleet-dagster-web  dag-work.devcloud.dz (password)
                   docker network "fleet": fleet-kafka, fleet-postgres, fleet-lake-sink  (no published ports)
```

Every published port binds to 127.0.0.1: Docker-published ports bypass ufw, so nothing binds 0.0.0.0.
Kafka and PostgreSQL publish no ports at all.

## DNS

| Record | Type | Value |
|---|---|---|
| `pipeline.devcloud.dz` | A | `95.111.253.252` |
| `dag-work.devcloud.dz` | A | `95.111.253.252` |
| (optional) both names | AAAA | `2a02:c207:2245:6966::1` (nginx already listens on `[::]:443`) |

Then in Ploi: one site per name, a Let's Encrypt certificate each.

## nginx (paste into each Ploi site's nginx config, replacing its `location /`)

Both blocks were checked with a throwaway nginx on 127.0.0.1:18080 (`nginx -t` passes; the results are in the comments).

### pipeline.devcloud.dz → 127.0.0.1:8000

```nginx
    # pipeline.devcloud.dz -> the visual pipeline (127.0.0.1:8000), public; /api/stream is server-sent events
    location / {
        proxy_pass         http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header   Connection        "";
        proxy_set_header   Host              $host;
        proxy_set_header   X-Real-IP         $remote_addr;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
        proxy_buffering    off;              # SSE: every update goes out at once
        proxy_cache        off;
        gzip               off;
        proxy_read_timeout 1h;               # an open page keeps one stream for the whole talk
        proxy_send_timeout 1h;
    }
```

Checked: page 200; one SSE event per second through the proxy (also with `gzip on` at http level); a control
request without the token 403. The app never trusts the client address: behind nginx every request comes from
127.0.0.1, so only `X-Admin-Token` opens the controls, and none at all when `VIZ_ADMIN_TOKEN` is unset.

### dag-work.devcloud.dz → 127.0.0.1:3001 (password)

```nginx
    # dag-work.devcloud.dz -> Dagster web UI (127.0.0.1:3001), behind a password
    location / {
        auth_basic           "Dagster";
        auth_basic_user_file /home/ploi/fleet-secrets/dagster.htpasswd;

        proxy_pass         http://127.0.0.1:3001;
        proxy_http_version 1.1;
        proxy_set_header   Upgrade           $http_upgrade;      # Dagster's UI uses a websocket (/graphql)
        proxy_set_header   Connection        "upgrade";
        proxy_set_header   Host              $host;
        proxy_set_header   X-Real-IP         $remote_addr;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
        proxy_buffering    off;
        proxy_read_timeout 1h;
        proxy_send_timeout 1h;
        client_max_body_size 20m;
    }
```

Checked: 401 without a password and with a wrong one; 200 with it (page and `/graphql`); 101 on the websocket upgrade.

Password files, outside the repository (nginx workers run as `ploi`):

| File | Mode | Content |
|---|---|---|
| `/home/ploi/fleet-secrets/dagster.htpasswd` | 640 ploi:ploi | `fleet:$apr1$…` (`openssl passwd -apr1`) |
| `/home/ploi/fleet-secrets/dagster-password.txt` | 600 ploi:ploi | user and password, for you |

New password: `P=$(openssl rand -base64 18 | tr -d '/+=' | cut -c1-20); printf 'fleet:%s\n' "$(openssl passwd -apr1 "$P")" > /home/ploi/fleet-secrets/dagster.htpasswd`
(and update the 600 file). nginx reads the file on each request: no reload needed.

Without the dagster site, an SSH tunnel works too: `ssh -L 3001:127.0.0.1:3001 ploi@95.111.253.252`, then
http://localhost:3001. (The original plan also tunnelled Kafka UI on 8081: it was dropped from this server.)

## Run, reset, stop

```bash
cd ~/fleet-verify
./run-vps.sh up                # build if needed, start kafka, postgres, viz, lake-sink
./run-vps.sh prepare           # before the talk: up, Dagster UI on, demo reset, status
./run-vps.sh reset             # back to the start of the demo (about 50 s)
./run-vps.sh dagster start     # / stop: the Dagster UI only when needed (~270-310 MiB)
./run-vps.sh status            # containers, ports, memory
./run-vps.sh logs viz          # follow one service
./run-vps.sh build             # after a code change (git pull): rebuild and restart the app containers
./run-vps.sh stop              # stop everything; volumes and data stay
```

`reset` stops lake-sink, restarts viz (ends the replay and live_trips the page started), then
`tools.reset_demo --kafka`: live state, day 19's lake and results (warehouse and PostgreSQL) are cleared, the Kafka
topics are recreated empty and every consumer group's offsets are deleted. It keeps `data/inbox`, Dagster history,
and the latest resolved identity: live uses the latest resolved identity from the last batch run
(`data/identity/latest.json`), as production would. Run the batch once after a fresh install so that file exists.

Write each value in `.env` on its own line with no comment after it: Docker Compose passes `VAR=   # text` into the
container as the value `# text`, not as empty (`VIZ_ADMIN_TOKEN` was set that way until 20 Nov). After changing
`.env`: `docker compose -f docker-compose.vps.yml up -d --force-recreate viz`.
Secrets: `.env` (mode 600, never printed, never committed) holds `POSTGRES_PASSWORD`, `VIZ_ADMIN_TOKEN` and the API
tokens. `CLOUDROUTES_URL` stays empty on purpose: lines and the fleet registry come from `data/inbox`; with
`FLEET_*_PREFER=inbox` no live API is called during the talk.

## Unlock the controls on the presenting laptop

The token is in `/home/ploi/fleet-secrets/viz-admin-token.txt` (mode 600; the same value as `VIZ_ADMIN_TOKEN` in
`.env`). Open `https://pipeline.devcloud.dz/#admin=<token>` once. The fragment never reaches the server or its logs;
the page checks the token with `/api/auth`, keeps it in the browser and removes it from the address bar. The control
panel then shows. Viewers never see it. `https://pipeline.devcloud.dz/#lock` forgets the token.

## Memory (limit for the whole stack: 2.2 GiB)

Each container has a `mem_limit` (= `memswap_limit`): kafka 900 MiB (heap 384 MiB; the rest is page cache and off-heap), postgres 192 MiB
(shared_buffers 64MB, max_connections 20), viz 1052 MiB (the replay, live_trips and the batch run inside it),
lake-sink 256 MiB, dagster-web 512 MiB (only on demand). Measured with `docker stats`:

| Moment | Total |
|---|---|
| idle | 473 MiB |
| idle, Dagster UI on, after a few hours (Kafka 512 MiB) | 915 MiB |
| replay ×120 | 818 MiB |
| batch | 1,011 MiB |
| the whole browser test (replay, pause, resume, batch) | 1,137 MiB |

With the Dagster UI on during the talk, expect about 1.45 GiB at the peak.

## Morning checklist

1. `ssh ploi@95.111.253.252`, `cd ~/fleet-verify && git pull` (if anything changed: `./run-vps.sh build`).
2. `./run-vps.sh prepare`: five containers up, `127.0.0.1:8000` and `127.0.0.1:3001` only, total memory under 1 GiB.
3. `curl -sS -o /dev/null -w "%{http_code}\n" https://pipeline.devcloud.dz/` → 200, and
   `https://dag-work.devcloud.dz/` → 401 without the password.
4. On the laptop: open `/#admin=<token>`; the control panel shows; the header reads "Kafka connected".
5. Rehearse once: Clean start → live_trips → Replay ×120: the first alert (Bus R, since 06:24) appears at ~0:35,
   Bus R's 14:12 alert at ~4:30, the replay ends at ~9:17. Pause / resume, then Run the batch (~49 s to four green
   checks).
6. `./run-vps.sh reset`: the top lane idle, the bottom lane grey for 19 Nov.
7. If Kafka shows close to its limit in `status` (900 MiB), `docker restart fleet-kafka` before the talk.
8. Fallback if Kafka fails on stage: the "No Kafka: same logic from the file" button.
