# Mailer — Self-Hosted Deployment System

Status: PROPOSAL.

## Context

Make Mailer a **self-hosted stream-processing platform**: a user writes a pipeline
with the Mailer SDK, builds it into a Docker image, pushes it to any OCI registry,
and deploys it *by image reference* through the Mailer controller, which runs inside
the user's own infrastructure (primarily a single VM/EC2 with Docker) and pulls &
runs the container there. Data never leaves the user's network; Mailer only
orchestrates.

**Most of this already exists.** The controller already speaks only to a
`ContainerBackend` interface (`control/backend/backend.go:57-71`), SDK jobs already
deploy by image reference (`kind: sdk` + `image`; `control/controller.go`
`parseSDKManifest`/`submitSDK`/`launch`), and monitoring/restart/checkpoints/isolation
all work. This plan closes **four concrete gaps** between "works with a local image
via curl" and "push to a registry, one-command deploy on your own box":

1. The Docker backend never pulls the image (assumes it's already local).
2. No resource limits anywhere (`LaunchSpec`, Docker `HostConfig`, K8s container).
3. No deploy CLI — "deploy" today is raw `curl`.
4. No API authentication — anyone who reaches `:9000` controls every job.

### Scope decisions
- **Backend:** Docker-first; add Kubernetes parity only where cheap.
- **CLI:** full `mailer deploy` = `docker build` → `docker push` → submit, plus
  management commands.
- **Registry auth:** assume a pre-authenticated host (operator ran `docker login`
  or uses an ECR credential helper + node IAM). No credential storage in the
  controller — but `docker pull` from the controller must actually work.
- **API auth:** shared bearer-token layer (controller + CLI + UI), opt-in (empty
  token = today's fully-open behavior).

All work is in the **`control/` Go module** (its own `go.mod`, wired via `go.work`).

Ordering: **1 → 2 → 3 → 4 → 5.** Phase 1 must precede Phase 2's end-to-end value
(deploy pushes to a registry the controller must pull from); Phase 4 edits the
Phase-2 client. Auth is opt-in throughout, so Phases 1–3 stay usable on a private
network before Phase 4 lands.

---

## Phase 1 — Registry pull (Docker) + cheap K8s parity

**Goal:** the Docker backend pulls `spec.Image` before creating the container,
honoring a `docker login`'d host (incl. ECR credential helpers), with graceful
fallback to a locally-present image. K8s gets pull-policy + pull-secret passthrough.

**Changes**
- `control/backend/backend.go`: add `PullPolicy string` to `LaunchSpec` (`:26-46`).
  Empty = `ifnotpresent` (default), plus `always` / `never`.
- `control/backend/docker.go`: new `pullImage(ctx, ref)` called at the top of
  `Launch` (`:68`, before `VolumeCreate`). Logic: skip when policy is
  `ifnotpresent` and `HasImage` (`:48`) is true; else `cli.ImagePull` with resolved
  auth; on pull failure fall back to a locally-present image (warn + continue) or
  return the error if absent. Reuse the already-imported
  `github.com/docker/docker/api/types/image` (`PullOptions`).
- **Registry credentials (the crux):** helper `resolveRegistryAuth(ref) string` —
  parse the registry domain with the in-tree `github.com/distribution/reference`
  (`ParseNormalizedNamed` → `Domain`; map `docker.io` → `https://index.docker.io/v1/`),
  load `~/.docker/config.json` via `github.com/docker/cli/cli/config`
  (`LoadDefaultConfigFile`), call `cfg.GetAuthConfig(domain)` (this invokes any
  `credsStore`/`credHelpers`, e.g. `docker-credential-ecr-login`), then
  `registry.EncodeAuthConfig(...)` for the `RegistryAuth` string. Empty when no
  creds found → anonymous pull (public images still work). **This is the one new
  dependency: `github.com/docker/cli`** (`go get` in `control/`).
- `control/backend/kubernetes.go`: in `buildJob` (`:214-223`) set container
  `ImagePullPolicy` from `spec.PullPolicy`; add `ImagePullSecrets []string` to
  `KubernetesOptions` (`:39-45`) → `PodSpec.ImagePullSecrets` (`:203`).
- `control/controller.go`: `launch` (`:376`) sets `LaunchSpec.PullPolicy`
  (default `ifnotpresent` so YAML jobs on the local `mailer-runner:dev` never
  attempt a bogus pull).

**Backward compat:** YAML jobs (local runner image) → `ifnotpresent` + local
fast-path → unchanged. SDK image jobs gain real pull.

**Verify:** `go build ./...` in `control/`; extend the `backend` fake/tests to
assert `pullImage` runs and that a pull error with a present local image does not
fail `Launch`. Manual: push a private SDK image, submit its manifest on a host
where it's absent, confirm the controller pulls it.

---

## Phase 2 — `mailer deploy` full CLI + management commands

**Goal:** one command from source to running job, plus thin REST management commands.

**Changes** (all in `control/cmd/mailer/`)
- `main.go`: extend the subcommand switch (`:35-45`) and `usage()` with
  `deploy`, `jobs`, `logs`, `cancel`, `restart`, `savepoint`, `status`. Keep
  `dashboard` unchanged.
- New `client.go`: a REST client `{base, token, http}` with one method per
  existing route (`api/api.go:33-49`): `submit`, `listJobs`, `getJob`, `cancel`,
  `restart(savepoint)`, `savepoint`, `logs(tail)`. Management subcommands are thin
  wrappers that print a compact table (or raw text for logs). Pure REST → works
  against a controller on another host.
- New `deploy.go`: `runDeploy` — flags `-file` (default `mailer.yaml`),
  `-dockerfile`, `-context`, `-controller` (env `MAILER_CONTROLLER`, default
  `http://localhost:9000`), `-no-build`, `-no-push`, `-env KEY=VAL` (repeatable).
  Shell out via `os/exec` (already imported): `docker build -t <image> …` then
  `docker push <image>` (stream output), then POST the **raw manifest** to
  `/jobs` as `{"workflow": <manifest>, "env": {...}}` — matches
  `readWorkflow`/`submitRequest` (`api.go:53-84`). Deploy is schema-agnostic: it
  forwards the doc, so Phase 3's new manifest fields need no deploy changes.

**Key decisions:** shell out to the docker CLI for build/push (reuses the user's
buildkit/auth; keeps the CLI thin) — distinct from Phase 1's server-side SDK pull.
Only `dashboard` and `deploy` touch local docker; everything else is REST.

**Verify:** `go build ./control/cmd/mailer`. Manual: in `mailer-test/sdk-job/`,
`mailer deploy -controller http://host:9000`; confirm build→push→job appears via
`mailer jobs`, `mailer logs <id>` streams, `cancel`/`restart` work.

---

## Phase 3 — Resource limits + manifest `env`/`resources` + PVC flags

**Goal:** CPU/memory limits flow manifest → controller → `LaunchSpec` → Docker
`HostConfig.Resources` and K8s container `Resources`; manifest gains `env` and
`resources`; PVC size/class become CLI flags.

**Changes**
- `control/controller.go`: extend `sdkManifest` (`:135-139`) with
  `Env map[string]string` and `Resources *ResourceSpec{CPU, Memory string}` using
  **Kubernetes quantity strings** (`"500m"`, `"512Mi"`). Validate them in
  `submitSDK` (`:154`) with `resource.ParseQuantity` and reject bad values with a
  400 (mirrors YAML validation at `:103-106`) — this prevents a K8s
  `resource.MustParse` panic at launch.
- **Zero-migration persistence:** keep storing the full manifest in `store.Job.Spec`
  (already done, `:168`). In `launch` (`:348-405`), when `Kind==KindSDK`,
  **re-parse `job.Spec`** to recover `Env`/`Resources` and build the `LaunchSpec`.
  No `store.Job` field additions, no SQLite migration; fields survive controller
  restart because `Spec` is durable. Merge manifest `Env` into the container env
  (API-supplied secret env wins; stays in-memory only).
- `control/backend/backend.go`: add `Resources *ResourceLimits{CPU, Memory string}`
  to `LaunchSpec` (nil = unlimited = today's behavior).
- `control/backend/docker.go`: in `Launch` populate `host.Resources`
  (`container.Resources` on `HostConfig`, `:113`): CPU string → `NanoCPUs`
  (`"500m"`→5e8; strip `m`/÷1000 else parse float ×1e9); Memory →
  `units.RAMInBytes("512Mi")` (promote `github.com/docker/go-units` to direct).
- `control/backend/kubernetes.go`: set container `Resources`
  (`corev1.ResourceRequirements`, requests=limits) via `resource.MustParse`
  (`resource` already imported, `:13`). Wire `-pvc-size` / `-storage-class`
  (already on `KubernetesOptions` + `ensurePVC` `:243-268`, just unwired) and the
  Phase-1 `-image-pull-secrets` flag through `makeBackend` (`cmd/mailer/main.go:133-136`).

**Verify:** unit-test the two Docker parsers (`"500m"`→5e8, `"1Gi"`→1073741824).
Manual: deploy with `resources: {cpu: "500m", memory: "256Mi"}`; `docker inspect`
shows `NanoCpus`/`Memory`; on K8s `kubectl get pod -o yaml` shows requests/limits.

---

## Phase 4 — API bearer-token auth (controller + CLI + UI)

**Goal:** a shared bearer token protects the API; CLI and embedded UI send it;
empty token = today's fully-open behavior.

**Changes**
- `control/api/api.go`: `NewServer(ctrl, token)` (`:28`); in `Handler()` (`:33-49`)
  wrap the mux in an `auth` middleware. Empty token → pass through. Else require
  `Authorization: Bearer <token>` (compare with `crypto/subtle.ConstantTimeCompare`)
  on all routes **except** `GET /healthz` and `GET /{$}` (the HTML shell must load
  so the browser can prompt). Optional `POST /auth` returns 200 iff the token is
  valid (nicer UI verify).
- `control/cmd/mailer/{main.go,client.go}`: `-auth-token` flag (env
  `MAILER_AUTH_TOKEN`) on `runDashboard` → `api.NewServer(ctrl, token)` (`:91`);
  `-token` flag (env `MAILER_TOKEN`) on the client → sets the `Authorization`
  header on every request (no header when empty).
- `control/ui/index.html`: token in `localStorage['mailer_token']`; add an
  `authFetch(url, opts)` wrapper that injects the header and route the `api.*`
  methods (`:116-126`) through it. On a `401`, clear the token and render a small
  inline token-entry form into `#app` (`:110`); on save, store and re-run the
  current view's `poll` (`:131`). If the first `/jobs` returns 200 (no-token mode),
  no prompt ever appears — UX identical to today.

**Rationale:** the shared token *is* the secret (no per-user sessions), so
localStorage + a reactive 401 gate needs zero server session machinery and keeps
the no-token path pixel-identical. (A `Secure;HttpOnly` cookie via `POST /auth` is
a Phase-5 hardening option behind TLS.)

**Verify:** with `-auth-token` set: `curl /jobs` → 401; with the header → 200;
`GET /` and `/healthz` → 200 without token; UI prompts→stores→loads; CLI `-token`
works, without fails cleanly. With no token configured: unchanged.

---

## Phase 5 — Self-hosted deployment docs + hardening

**Goal:** make a single-VM/EC2 deployment reproducible and reasonably safe.

**Deliverables** (Markdown under `docs/` + flag/log polish)
- **Operator quickstart:** install Docker, `docker login` (or ECR helper + node
  IAM) so the controller can pull; build/push the runner image; run
  `mailer dashboard -addr :9000 -image <registry>/mailer-runner:tag -auth-token <secret>`;
  then developer-side `mailer deploy`.
- **Registry auth doc:** how Phase-1 credential resolution honors `docker login`
  and ECR (`credsStore`/`credHelpers`), and how to verify.
- **Security checklist:** front the controller with a TLS reverse proxy (the API/UI
  is plain HTTP — the bearer token must not cross the network in cleartext); bind
  `-addr` to a private interface / SSH tunnel; rotate the token; note the Docker
  control port is already `127.0.0.1`-only (`docker.go:119`); use resource limits
  as a runaway-job guardrail. Optional: systemd unit for `mailer dashboard`.
- **Backend selection doc:** Docker (default, single VM) vs Kubernetes
  (`-backend kubernetes` with `-pvc-size`/`-storage-class`/`-image-pull-secrets`).

**Verify:** fresh-VM dry run of the quickstart end-to-end (build runner → deploy a
sample SDK job → observe in UI with auth on → cancel).

---

## Cross-cutting risks / call-outs
- **New dependency:** `github.com/docker/cli` (Phase 1) is the only genuinely new
  module — `go get` it into `control/` (also promote indirect `go-units` /
  `distribution/reference` to direct). It's a dep change worth review.
- **`resource.MustParse` panics** on bad quantities → validate at submit (Phase 3).
- **Docker Hub domain mapping** (`docker.io` → `https://index.docker.io/v1/`) is the
  classic footgun in Phase-1 credential lookup — handle explicitly.

## Critical files
- `control/backend/docker.go` — pull + resources
- `control/backend/backend.go` — `LaunchSpec` (PullPolicy, Resources)
- `control/controller.go` — manifest schema, submit/launch, re-parse-at-launch
- `control/api/api.go` — auth middleware
- `control/cmd/mailer/main.go` (+ new `client.go`, `deploy.go`) — CLI
- Secondary: `control/backend/kubernetes.go`, `control/ui/index.html`, `control/go.mod`
