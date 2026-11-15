# Benchmarks

The performance gate is the job `test: perf` in `.github/workflows/ci.yml`. It runs the
defer and relay workload in release mode and compares defer with `baseline.json`.
It fails when a number is worse than the baseline by more than 20 percent.

The gate compares ratios, not absolute numbers: defer divided by relay, measured in the same
run (`throughput_vs_relay`, `p50_vs_relay`, `p99_vs_relay`). The CI runner is not the machine that
recorded the baseline, so absolute numbers differ between hosts. The absolute numbers are stored
and published here, but only the three ratios are gated.

| Metric | Defer | Relay | Defer / Relay |
|---|---|---|---|
| Throughput (ops/s) | 16172.92 | 2950.52 | 5.4814 |
| p50 batch lifecycle (us) | 5250 | 34911 | 0.1504 |
| p99 batch lifecycle (us) | 8661 | 39607 | 0.2187 |

Machine: Linux x86_64, 4 CPUs, rustc 1.99.1 (58cf8f9c3 2026-10-13)

Recorded on 2026-11-14 as the median throughput of three runs. The workload is 1000 messages in
10 batches of 100, with a 128-byte serialized payload, durable enqueue, committed lease and
committed ack, a single Raft voter and `fsync` always.

These numbers describe one host class and one single-voter setup. They are not a cluster claim.

Reproduce the baseline:

```sh
cargo test --release --locked -p defer --test it -- relay_performance_ceiling:: --ignored --nocapture
```

See also [relay-performance-ceiling.md](relay-performance-ceiling.md) and
[competitor-feature-matrix.md](competitor-feature-matrix.md).
