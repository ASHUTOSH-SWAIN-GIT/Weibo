# Self-hosting Mailer

How to run the Mailer control plane on a single VM (EC2, a droplet, bare
metal) so developers can `mailer deploy` jobs to it, watch them in the UI, and
manage their lifecycle — safely enough for a small team.

This guide covers a **Docker** deployment (the default, one VM). For a cluster,
see [Kubernetes backend](#backend-selection-docker-vs-kubernetes).

- [Architecture in one paragraph](#architecture-in-one-paragraph)
- [Operator quickstart](#operator-quickstart)
- [Registry authentication](#registry-authentication)
- [Security checklist](#security-checklist)
- [Backend selection: Docker vs Kubernetes](#backend-selection-docker-vs-kubernetes)
- [systemd unit (optional)](#systemd-unit-optional)

---

## Architecture in one paragraph

One long-running **controller** process (`mailer dashboard`) serves the REST
API and the web UI, and drives **one container per job** on the local Docker
daemon. Jobs come in two shapes: a **YAML workflow** (runs the generic
`mailer-runner` image with the workflow document injected) or an **SDK job** (a
prebuilt Go pipeline image you build and push). The controller persists job
state to SQLite and a reconciler keeps every job converging on its desired
state. Developers talk to it entirely over REST — `mailer deploy`, `mailer
jobs`, `mailer logs`, etc. — so nothing but the controller needs Docker.

```
developer ──(REST + bearer token)──▶ controller (mailer dashboard) ──▶ Docker ──▶ job containers
   mailer deploy / jobs / logs           API + UI on :9000                 one per job
```

---

## Operator quickstart

On a fresh VM. Assumes Ubuntu-like; adjust for your distro.

### 1. Install Docker

```sh
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "$USER"     # log out/in so `docker` works without sudo
docker version                      # confirm the daemon is reachable
```

### 2. Authenticate to your image registry

The controller must be able to **pull** the images your jobs run. Log in on the
VM once (see [Registry authentication](#registry-authentication) for ECR and
private registries):

```sh
docker login <registry>            # e.g. docker login ghcr.io
```

Public images need no login.

### 3. Build and push the runner image

The `mailer-runner` image runs any YAML workflow. Build it from the repo root
and push it to a registry the VM can pull from:

```sh
docker build -f Dockerfile.runner -t <registry>/mailer-runner:1.0 .
docker push <registry>/mailer-runner:1.0
```

> On the VM itself you can skip the push and build locally as
> `mailer-runner:dev`; YAML jobs never trigger a pull for a locally-present
> runner image. A registry image is only required when the runner image is not
> already on the host (or on a Kubernetes cluster).

### 4. Start the controller

Generate a strong bearer token and start the dashboard headless, bound to the
runner image and protected by the token:

```sh
export MAILER_AUTH_TOKEN="$(openssl rand -hex 32)"
mailer dashboard \
  -addr 127.0.0.1:9000 \
  -image <registry>/mailer-runner:1.0 \
  -db /var/lib/mailer/control.db \
  -no-open
```

- `-addr 127.0.0.1:9000` binds to loopback only — reach it over an SSH tunnel
  or front it with a TLS proxy (see [Security](#security-checklist)). Use
  `-addr :9000` only behind such a proxy.
- `-auth-token` is read from `MAILER_AUTH_TOKEN`; the log prints
  `API auth ENABLED` when a token is set.
- `-no-open` keeps it from trying to open a browser on the server.

Keep it running under [systemd](#systemd-unit-optional) in production.

### 5. Deploy from a developer machine

Developers point the CLI at the controller and pass the same token. For a
loopback-bound controller, open a tunnel first:

```sh
ssh -N -L 9000:127.0.0.1:9000 user@vm        # in one terminal
export MAILER_CONTROLLER=http://localhost:9000
export MAILER_TOKEN="<the shared token>"
```

**A YAML workflow** — nothing to build, just submit:

```sh
mailer deploy -file examples/workflows/wordcount.yaml
mailer jobs
mailer logs <job-id>
```

**An SDK job** — `mailer deploy` builds the image from the manifest's `image:`,
pushes it, then submits. A minimal `mailer.yaml`:

```yaml
kind: sdk
name: orders
image: <registry>/orders-sdk:1.0
env:
  LOG_LEVEL: info
resources:
  cpu: "500m"      # optional CPU cap (Kubernetes quantity)
  memory: "512Mi"  # optional memory cap
```

```sh
mailer deploy -file mailer.yaml -dockerfile Dockerfile -context .
# build → push → submit; then:
mailer jobs
mailer status <job-id>
mailer cancel <job-id>
```

> **SDK build caveat:** if your SDK job's `go.mod` uses a
> `replace => ../mailer` local path, it will not resolve inside a Docker build
> context. Either depend on a tagged module version, or build the job binary on
> the host and `COPY` it into the image. Then `mailer deploy -no-build -no-push`
> skips the docker steps and just submits.

### CLI command reference

All management commands are pure REST — they work against a controller on any
host and honor `MAILER_CONTROLLER` / `MAILER_TOKEN`.

| Command | Purpose |
| --- | --- |
| `mailer dashboard`            | Start the controller + UI (the server process). |
| `mailer deploy`               | Build, push, and submit a job manifest. |
| `mailer jobs`                 | List jobs. |
| `mailer status <id>`          | One job's detail, latest run, and history. |
| `mailer logs <id> [-tail N]`  | Print a job's container logs. |
| `mailer cancel <id>`          | Gracefully stop a job. |
| `mailer restart <id> [-savepoint L]` | Resume a job (optionally from a savepoint). |
| `mailer savepoint <id> -label L` | Stop a job with a named savepoint. |

---

## Registry authentication

The controller pulls a job's `image` before launch using the **same credentials
the Docker CLI uses** — it reads `~/.docker/config.json` for the user running
`mailer dashboard`. So whatever `docker pull <image>` can fetch, the controller
can too.

- **Docker Hub / GHCR / generic registries:** `docker login <registry>` on the
  VM writes the credential (or a `credsStore` reference). The controller resolves
  it per-image, mapping the bare `docker.io` domain to
  `https://index.docker.io/v1/` the way the Docker CLI does.
- **Amazon ECR:** install the credential helper and configure it, so tokens
  refresh automatically without a login cron:

  ```sh
  # ~/.docker/config.json
  { "credHelpers": { "<acct>.dkr.ecr.<region>.amazonaws.com": "ecr-login" } }
  ```

  Provide credentials via the instance's IAM role (an EC2 node role with
  `AmazonEC2ContainerRegistryReadOnly` is enough to pull) or `aws configure`.
- **Verify** the controller can pull before relying on it:

  ```sh
  docker pull <registry>/your-image:tag    # as the same user that runs the dashboard
  ```

  If that works, job launches will too. Public images work with no credentials
  at all (anonymous pull).

Pull policy: images are pulled only when **absent** locally; a local build (e.g.
`mailer-runner:dev`) is never pulled over. A pull failure falls back to a
locally-present image so an air-gapped host still launches.

---

## Security checklist

The API and UI are **plain HTTP**, and the bearer token is a shared secret. Do
not let it cross an untrusted network in cleartext.

- [ ] **Terminate TLS in front of the controller.** Put Caddy / nginx / a cloud
      load balancer in front and proxy to `127.0.0.1:9000`. The token then only
      travels over HTTPS. Without this, anyone on-path can read the token and
      every request.
- [ ] **Bind to a private interface.** Start with `-addr 127.0.0.1:9000` and
      reach it via an SSH tunnel or the TLS proxy on the same host. Use a public
      `-addr :9000` **only** when a TLS proxy sits in front.
- [ ] **Set a strong token and rotate it.** `openssl rand -hex 32`. To rotate:
      restart the dashboard with a new `MAILER_AUTH_TOKEN` and redistribute it;
      clients update `MAILER_TOKEN`. Browsers re-prompt automatically on the
      next 401.
- [ ] **Job control port is already loopback-only.** The Docker backend
      publishes each job's control surface on `127.0.0.1` only — it is not
      reachable off-host. Nothing to configure.
- [ ] **Cap job resources as a runaway guardrail.** Set `resources.cpu` /
      `resources.memory` in SDK manifests so a buggy job can't starve the VM.
- [ ] **Restrict who can reach the daemon.** The controller has full Docker
      access (it launches containers). Treat the VM and the `MAILER_AUTH_TOKEN`
      as production secrets; limit SSH access.
- [ ] **Persist the SQLite DB on durable storage.** Point `-db` at a path on a
      persistent volume (e.g. `/var/lib/mailer/control.db`) so job records
      survive a redeploy of the controller.

> **No token = open API.** Running `mailer dashboard` without `-auth-token`
> leaves the API fully open (the pre-auth behavior). That is fine for a laptop
> or a fully private network, but never for anything reachable by others.

---

## Backend selection: Docker vs Kubernetes

| | Docker (default) | Kubernetes |
| --- | --- | --- |
| Best for | a single VM | an existing cluster |
| Unit of work | one container per job | one `batch/v1` Job per job |
| State | per-job named volume | per-job PVC |
| Select with | `-backend docker` (default) | `-backend kubernetes` |

**Docker** — nothing extra to configure:

```sh
mailer dashboard -image <registry>/mailer-runner:1.0
```

**Kubernetes** — the runner image must be pullable *by the cluster* (push it to
a registry the nodes can reach). Relevant flags:

```sh
mailer dashboard -backend kubernetes \
  -namespace mailer \
  -image <registry>/mailer-runner:1.0 \
  -pvc-size 2Gi \
  -storage-class gp3 \
  -image-pull-secrets regcred
```

- `-pvc-size` / `-storage-class` size and place each job's state volume.
- `-image-pull-secrets` names pre-created `dockerconfigjson` Secrets in the
  namespace for private registries (Mailer references them; it does not create
  them).
- **Live-metrics note:** the dashboard proxies a job's `/state` and `/metrics`
  through the ClusterIP Service, which needs in-cluster networking. Job
  *lifecycle* (submit/status/logs/cancel/restart) works from anywhere via the
  API; run the controller in-cluster (or port-forward) for live metrics.

---

## systemd unit (optional)

Run the controller as a managed service so it restarts on crash and boot. Put
the token in an environment file readable only by root:

```sh
# /etc/mailer/mailer.env   (chmod 600)
MAILER_AUTH_TOKEN=<your-strong-token>
```

```ini
# /etc/systemd/system/mailer.service
[Unit]
Description=Mailer control plane
After=docker.service
Requires=docker.service

[Service]
User=mailer
EnvironmentFile=/etc/mailer/mailer.env
ExecStart=/usr/local/bin/mailer dashboard \
  -addr 127.0.0.1:9000 \
  -image <registry>/mailer-runner:1.0 \
  -db /var/lib/mailer/control.db \
  -no-open
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
```

```sh
sudo useradd -r -G docker mailer            # service account with Docker access
sudo install -d -o mailer /var/lib/mailer
sudo systemctl daemon-reload
sudo systemctl enable --now mailer
journalctl -u mailer -f                     # watch it start
```

The `mailer` service account needs membership in the `docker` group to reach the
daemon. Front the loopback address with your TLS proxy for external access.
