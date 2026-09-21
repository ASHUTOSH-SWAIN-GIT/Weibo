# Prod-like local stack

Phase 1 of `plans/production-validation.md`: every backing service real, controller
reachable only over TLS with a token. Runs on one Mac.

## Start

```sh
cp deploy/compose/.env.example deploy/compose/.env   # fill in WEIBO_AUTH_TOKEN
set -a && source deploy/compose/.env && set +a
make prod-like-up
```

Wait for `minio-init` to exit 0 (creates the `weibo-savepoints` bucket) and
`postgres`/`minio` healthchecks to pass: `docker compose -f deploy/compose/docker-compose.prod-like.yml ps`.

Then start the controller **on the host** (needs the Docker daemon socket —
not containerized, see `docs/self-hosting.md`):

```sh
cd control && go run ./cmd/weibo dashboard \
  -addr 127.0.0.1:9000 -image weibo-runner:dev \
  -db /tmp/weibo-prod-like/control.db -no-open
```

Caddy proxies `https://weibo.localhost` → `127.0.0.1:9000` with an internally
trusted TLS cert. On macOS `*.localhost` already resolves to `127.0.0.1`; no
`/etc/hosts` edit needed. On first request your browser/curl needs to trust
Caddy's local CA — `docker compose -f deploy/compose/docker-compose.prod-like.yml exec caddy caddy trust`
or pass `curl -k` for scripts.

## Gate (from plans/production-validation.md Phase 1)

```sh
make test-integration-live          # WEIBO_LIVE=1, needs the env above
KAFKA_BROKERS=localhost:39092 ./scripts/test-kafka.sh   # against the real broker
curl -sS https://weibo.localhost/jobs -H "Authorization: Bearer $WEIBO_TOKEN"   # 200
curl -sS https://weibo.localhost/jobs -H "Authorization: Bearer wrong"          # 401
```

## Ports

| Service | Host port(s) | Notes |
|---|---|---|
| Kafka | 39092 (host/tools), 39093 (job containers via `host.docker.internal`) | non-standard ports to avoid clashing with a local native broker on 9092 and other projects on 29092; dual listener — see comment in the compose file |
| Postgres | 54320 | not 5432 — collides with a natively running local Postgres on this machine |
| MinIO | 9002 (S3 API), 9001 (console) | 9000 stays free for the controller |
| Prometheus | 9090 | still points at the old hardcoded target; Phase 4 retargets it to `/targets` |
| Grafana | 3000 | anon admin |
| Caddy | 80, 443 | TLS terminator in front of the host controller |

## Why job containers need a second Kafka listener

`control/backend/docker.go`'s `ContainerCreate` passes a nil `NetworkingConfig`,
so job containers attach to Docker's default bridge network, not this compose
project's network. They can't reach `kafka:9092` by service name. They *can*
reach the host via `host.docker.internal`, which Docker Desktop resolves
automatically — hence the `INTERNAL://host.docker.internal:39093` listener.

## Stop

```sh
make prod-like-down    # also drops the named volumes
```
