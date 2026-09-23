# Benchmarks and validation results

This page records the tests we ran against Weibo on real infrastructure, what
they measured, what they found, and what they did **not** measure. Every number
below was measured; nothing is extrapolated.

It has three parts:

1. [End-to-end correctness under load](#1-end-to-end-correctness-under-load): a
   Kafka to keyed window to S3 pipeline at 10,000 events/s, with failures injected,
   and a checker that recomputes the expected answer.
2. [Recovery and control-plane timings](#2-recovery-and-control-plane-timings)
3. [Microbenchmarks](#3-microbenchmarks) (`make bench`) and the
   [test suite](#4-test-suite-and-coverage)

The goal was correctness first. This is **not** a maximum-throughput benchmark:
the load was fixed at a rate the machine handled comfortably, so the results tell
you the pipeline is correct at that rate and how much of the machine it used, not
where it saturates.

## 1. End-to-end correctness under load

### Setup

| | |
|---|---|
| Host | AWS EC2 `t4g.large` (2 vCPU Graviton2, 8 GiB) for the 10k/s runs; `t4g.medium` (2 vCPU, 4 GiB) for the 50/s run |
| OS / Docker | Ubuntu 26.04 LTS arm64, Docker 29.1.3 |
| Kafka | `apache/kafka:3.7.0`, one KRaft broker, 512 MiB heap, 6 partitions (1 in the 50/s run) |
| Sink | MinIO (S3 API) |
| Controller | `weibo dashboard` from a GitHub release binary, Docker backend |
| Job | Kafka source, parse, `KeyBy(customer)`, 1-minute tumbling `Window`, `Reduce` (sum), JSON, S3 sink |
| Job settings (10k/s runs) | parallel per-partition Kafka reads, 4 keyed workers, checkpoints every 10 s, Pebble state, 1 CPU / 768 MiB container limit |

Everything runs on one machine: Kafka, MinIO, the producer, the controller and
the job compete for the same 2 vCPUs.

### Method

A **deterministic producer** writes orders to Kafka. Record number `n` always has:

- customer `cust-(n mod N)` (N = number of customers)
- amount `(n * 37) mod 100 + 1`
- event time `start + n / rate`

Because the input is a pure function of `n`, a **checker** can recompute, for every
(customer, window), the exact total the job must have produced, and compare it with
what actually landed in S3. A lost record makes a total too low; a record counted
twice makes it too high. The job emits a running total per record, so a window's
final value is the largest one written for that (customer, window).

Failures were injected while the load ran: `SIGKILL` on the job container (no
graceful drain), and `kill -9` on the controller.

### Results

| Run | Engine | Load | Result |
|---|---|---|---|
| 1 | v1.0.1 | 50/s, 10 customers, 1 partition, no failure injected, checkpointing not enabled | **Pass.** 28 one-minute windows, 270 (customer, window) totals, 0 mismatches, lag 0 |
| 2a | v1.0.2 | 10k/s, 1,000 customers, 6 partitions, 1 s watermark tolerance | **Fail.** 5,659 of 7,000 totals too low (see below) |
| 2b | v1.0.2 | same, watermark tolerance widened to 30 s | Windows without a failure exact; the window spanning the `SIGKILL` **lost 10.36%** |
| 3 | fixed build (v1.0.3) | same as 2a | **Pass.** 4,414,010 orders, 7,000 of 7,000 totals exact, including the window spanning the `SIGKILL` |

**Run 2a** lost data in every window: 0.6% to 7.6% at steady state, 18.6% in the
first window while the job caught up from a cold start, and 31% in the window that
contained the restart and catch-up. No error, log line or metric
reported it. The job's own counters showed 0 sink errors, so the loss was upstream
of the sink.

**Run 2b** separated the two causes. Widening the watermark tolerance made every
uninterrupted window exact, which pinned the steady-state loss on the watermark.
The window spanning the kill was still short by exactly **62,174 orders, the ~6.2
seconds that had been buffered in the window at the last checkpoint**.

**Run 3**, on the fixed build with the default 1 s tolerance:

| Check | Result |
|---|---|
| Orders produced | 4,414,010 over about 7.4 minutes |
| Closed windows checked | 7 |
| (customer, window) totals | 7,000 exact, 0 missing, 0 too low, 0 too high |
| Window spanning the `SIGKILL` (19:12) | exact (previously 10.36% short) |
| Records rejected as late | 0 (the counter series was never created) |
| Sink errors / failed records | 0 / 0 |

### Resource use at 10,000 events/s (run 3)

Sampled every ~34 s with `docker stats` on the 2 vCPU host. CPU is percent of one core.

| Component | CPU | Memory |
|---|---|---|
| Job | 37% to 100% (typically 50% to 90%) | 117 to 231 MiB (mostly 167 to 182; peaks of 213 and 231 MiB) |
| Kafka | 5% to 23% | 401 to 612 MiB (512 MiB heap) |
| MinIO | 0% to 5% | 70 to 92 MiB |

Consumer lag was 0 in 10 of 13 samples taken under load. The other three were 104,
643 (the largest, about 64 ms of input, sampled just before the kill) and 445
(right after the restart, while the job caught up).

Output volume (run 2a): 221 objects, 457 MiB for 4.30 million rows, about 106 bytes
per row. The job writes one row per input record because `Window(...).Reduce(...)`
emits a running total per record. If you only need the final value per window, use
`WindowReduce`, which emits one row per (key, window); see
[Core Concepts](concepts.md#window).

## 2. Recovery and control-plane timings

### Recovery

| Event | Measured |
|---|---|
| `SIGKILL` the job container, time until a new container is running | 6.44 s, 6.41 s, 6.47 s (three runs) |
| State after recovery | restored from the latest checkpoint; no records lost (run 3) |
| `kill -9` the controller while a job runs | the job kept running; the restarted controller adopted it with no second launch (same container, same run count before and after; observed once, in run 2a) |
| systemd `Restart=on-failure` after `kill -9` of the controller | active again with a new PID at the 6 s check (`RestartSec=3`, restart counter 1) |

The controller's transition log shows it noticing the exit about 2 s after the kill
and relaunching about 3 s after that; the rest is container start and checkpoint restore.

### Control-plane latency

| Endpoint | Measured |
|---|---|
| `/livez` | about 0.5 ms |
| `/readyz` | 2.0 to 2.2 s on the EC2 host |

`/readyz` asks Docker for container stats. A normal stats call blocks for about 1 s
while the daemon takes a second CPU sample (1.009 s measured, against 2.6 ms for a
one-shot call). Stats are now fetched concurrently and the whole call is bounded to
5 s, so it no longer grows with the container count (20 simulated containers at
100 ms each: 2.0 s serial, 0.32 s concurrent). It is still about 2 s on a real host,
roughly twice a single stats call; why it is twice, not once, was not investigated.
Use `/livez` for liveness probes.

## 3. Microbenchmarks

`make bench` runs the committed Go benchmarks. The baseline in
[`bench/BASELINE.md`](../bench/BASELINE.md) was captured on a container CPU
(Intel Xeon at 2.10 GHz, `GOMAXPROCS=4`), a different machine from the EC2 host
above, so the two sets of numbers are not comparable.

| Benchmark | Result |
|---|---|
| Pipeline throughput, memory backend, 100k keys | 733,372 records/s |
| Pipeline throughput, Pebble backend, 100k keys | 487,224 records/s |
| Keyed routing (`RouteKey`) | 7.9 ns/op, 0 allocations |
| Stateless operator chain | 1,267 ns/op, 2 allocations |

State backends at 100k keys, checkpoint and restore cost:

| Backend | Full checkpoint | Incremental checkpoint | Restore |
|---|---|---|---|
| Memory | 126 ms | 87 ms | 90 ms |
| Pebble, serialized ("compat") | 133 ms | 159 ms | 124 ms |
| Pebble, native hard-link checkpoints | 23 ms | 8.2 ms | 7 ms |

The property to watch is that the incremental checkpoint cost of the native Pebble
backend stays roughly flat as the key count grows (5.9 ms at 1k keys, 8.2 ms at 100k),
because it hard-links unchanged files instead of serializing state; the memory
backend's grows from 0.3 ms to 87 ms over the same range. Treat these as a baseline for catching
regressions of several times, not as exact figures.

## 4. Test suite and coverage

| Check | Result |
|---|---|
| `go test -race` (root and control modules, with and without the `kubernetes` tag) | passes |
| Fuzz smoke (every fuzz target, 10 s each) | passes in CI |
| Integration tiers | offline tiers on every pull request; live Kafka, Postgres, S3 (MinIO), kind and dashboard tiers on merge to `main` |
| Changed-package coverage gate | 50% minimum for any package a change touches |

Coverage of the packages changed while fixing what these tests found:

| Package | Statement coverage |
|---|---|
| `state` | 88.2% |
| `operator` | 83.6% |
| `source` | 65.5% |
| `control/cmd/weibo` | 62.9% |
| `control/backend` | 53.9% |

## What the testing found

Every item below was found by running the system, not by reading the code.

| Finding | Evidence | Fixed |
|---|---|---|
| Window records buffered at checkpoint time were overwritten after a restart (Pebble list append counter restarted at 0) | 62,174 orders short after a `SIGKILL`; job counters showed 0 sink errors | v1.0.3 |
| Kafka watermark followed the fastest partition, so slower partitions' records were dropped as late | 0.6% to 31% of each window lost at 10k/s | v1.0.3 |
| Records dropped as late left no trace | the two bugs above lost data with no signal | v1.0.3: `weibo_window_late_records_total` |
| `/readyz` took 7 to 8 s on a real host | serial per-container Docker stats | partly: concurrent and bounded to 5 s; still about 2 s |
| Controller refused to start without the YAML runner image, even for SDK-only hosts | first EC2 deploy | now a warning |
| Postgres sink lost rows on upsert with duplicate keys in a batch, and on shutdown flush | live Postgres CI tier | fixed |
| `sdk.Serve` silently ignores `CHECKPOINT_INTERVAL`; only `sdk.Run` enables checkpointing | first run had no checkpointing at all | the example pipeline uses `sdk.Run` |
| `Window(...).Reduce(...)` writes one row per input record, not per window | ~82,000 rows for 27 windows | documented; use `WindowReduce` |
| Self-hosting guide gaps: no step to install the binary, `-db` directory not created, service account could not store registry credentials | following the guide on a fresh Ubuntu machine | guide corrected |

## What was not measured

- **Maximum throughput.** The rate was fixed at 10,000 events/s. CPU headroom was
  not explored to saturation.
- **Multi-node behavior.** Everything ran on one machine with one Kafka broker.
- **Repeated or compound failures.** One job kill per run, and a separate controller
  kill. Repeated restarts, a crash during a checkpoint, and simultaneous controller
  and job failure were not tested.
- **Other backends.** Results are for Kafka parallel reads, the Pebble state backend
  and the S3 sink. The end-to-end exactly-once Kafka to Kafka path is covered by the
  test suite, not by this load test.
- **A partition that has not yet delivered any record** is not considered by the
  per-partition watermark (documented on `KafkaWithWatermarks`).
- **Long-duration soak.** Runs lasted minutes, not hours or days (see issue #23).

## Reproducing

The load harness is kept in separate repositories: a deterministic Kafka producer
(`weibo-order-producer`), the pipeline (`weibo-order-pipeline`), and a deploy
directory with the Compose file, the checker and the failure-injection scripts
(`weibo-ec2-deploy`). The method above is specific enough to rebuild the harness: the
record formula and the expected-total calculation are the whole of the checker.
