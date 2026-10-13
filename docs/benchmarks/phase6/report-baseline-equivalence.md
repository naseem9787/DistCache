# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING=Release (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 5 repeats per suite run, 1 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: baseline=cc459abeeea8ea18; phase5=3f017714e914184a; loadgen=0d05fc632bb6b284
- Command: `bench/harness/run_suite.py --suite E7 --repeats 5 --settle 3 --variant-filter SET only, P16: fsync everysec;SET only, P16: fsync always --binary baseline=build/distcache_baseline --binary phase5=build/distcache_phase5 --out results/equiv`

## E7: WAL durability policy

| variant | binary | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SET only, P16: fsync always | phase5 | 5 | 4.7k | 3.9k..5.7k | 15.4% | 132.12 ms | 343.93 ms | 461.37 ms |  | 0.1 |
| SET only, P16: fsync everysec | baseline | 5 | 975.3k | 926.4k..1,045.1k | 4.9% | 639 us | 1.41 ms | 2.16 ms |  | 3.4 |
| SET only, P16: fsync always | baseline | 5 | 5.1k | 4.9k..6.7k | 13.0% | 121.63 ms | 301.99 ms | 369.10 ms |  | 0.1 |
| SET only, P16: fsync everysec | phase5 | 5 | 980.2k | 784.2k..1,044.8k | 10.4% | 639 us | 1.47 ms | 2.29 ms |  | 3.3 |

Charts: `charts/E7-throughput.svg`, `charts/E7-p99.svg`

## Binary comparison (same variant, same session, interleaved)

Each other binary against the primary (`baseline`). p = two-sided Mann-Whitney U on the per-run requests/s; with n < 8 per side it is only a rough guide. 'diff' is the change in the median.

| variant | binary | n | median req/s | primary median | diff | p |
|---|---|---:|---:|---:|---:|---:|
| E7 SET only, P16: fsync always | phase5 | 5 / 5 | 4.7k | 5.1k | -8.3% | 0.402 |
| E7 SET only, P16: fsync everysec | phase5 | 5 / 5 | 980.2k | 975.3k | +0.5% | 1.000 |

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
