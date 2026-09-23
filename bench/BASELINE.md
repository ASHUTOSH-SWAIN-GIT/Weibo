# Benchmark baseline

Run with `make bench` (wraps `go test -run=^$ -bench=. -benchmem -short
./bench/... ./operator/... ./pipeline/...`). `-short` skips the 5M-key
tier of `BenchmarkStateScale`; drop `-short` (and raise `-timeout`) for
the full run.

Captured 2026-09-23 on a container CPU (Intel Xeon @ 2.10GHz, GOMAXPROCS=4).
Treat these as a rough baseline to catch regressions of several x, not
exact numbers — timing varies with host load.

```
pkg: github.com/ASHUTOSH-SWAIN-GIT/weibo/bench
BenchmarkStateScale/memory/1k-4                    218    5455339 ns/op    2437283 load-rec/s     160.0 lookup-ns       0 full-ckpt-ms      0.3420 incr-ckpt-ms      0 restore-ms
BenchmarkStateScale/memory/100k-4                    4  355217254 ns/op    1911713 load-rec/s     501.1 lookup-ns     126.0 full-ckpt-ms     86.97 incr-ckpt-ms   90.00 restore-ms
BenchmarkStateScale/pebble-compat/1k-4              43   24026305 ns/op     609739 load-rec/s    1101   lookup-ns       1.000 full-ckpt-ms    0.7600 incr-ckpt-ms    0 restore-ms
BenchmarkStateScale/pebble-compat/100k-4             2  608477694 ns/op     618943 load-rec/s    2236   lookup-ns     133.0 full-ckpt-ms    158.7 incr-ckpt-ms   124.0 restore-ms
BenchmarkStateScale/pebble-native/1k-4              30   38104141 ns/op     899385 load-rec/s     713.3 lookup-ns       5.000 full-ckpt-ms     5.913 incr-ckpt-ms    3.000 restore-ms
BenchmarkStateScale/pebble-native/100k-4             5  219052297 ns/op     788064 load-rec/s    2565   lookup-ns      23.00 full-ckpt-ms      8.169 incr-ckpt-ms    7.000 restore-ms
BenchmarkPipelineThroughput/memory/1k-4            370    3486341 ns/op     143270 rec/s
BenchmarkPipelineThroughput/memory/100k-4            6  184231474 ns/op     733372 rec/s
BenchmarkPipelineThroughput/pebble/1k-4             33   44646585 ns/op      25702 rec/s
BenchmarkPipelineThroughput/pebble/100k-4            5  240727600 ns/op     487224 rec/s

pkg: github.com/ASHUTOSH-SWAIN-GIT/weibo/operator
BenchmarkRouteKey-4          167678803    7.870 ns/op    0 B/op   0 allocs/op
BenchmarkSelectAndRoute-4     79973454   15.16 ns/op    0 B/op   0 allocs/op

pkg: github.com/ASHUTOSH-SWAIN-GIT/weibo/pipeline
BenchmarkStatelessChain-4    1806037    1267 ns/op   320 B/op   2 allocs/op
```

## What to watch for a regression

- `incr-ckpt-ms` for `pebble-native` should stay roughly flat as key
  count grows (cost is proportional to changed data, not total state) —
  this is the property `docs/concepts.md` documents native checkpoints
  for. `full-ckpt-ms` growing with key count is expected.
- `lookup-ns` growing materially between runs at the same key count
  usually means a state-backend regression, not noise.
- `rec/s` (`BenchmarkPipelineThroughput`) dropping by more than ~20%
  between runs at the same tier is worth a bisect before merging.

This is a baseline for humans doing local comparisons, not a CI gate —
none of these numbers are enforced automatically yet (see issue #23 for
the still-open soak-test follow-up).
