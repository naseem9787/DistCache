# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING=Release (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 6 repeats per suite run, 1 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: final=77be965f36f96619; baseline=3f017714e914184a; loadgen=0d05fc632bb6b284
- Command: `bench/harness/run_suite.py --suite E7 --repeats 6 --settle 3 --variant-filter SET only, P1: fsync always;SET only, P16: fsync always;80% GET, P16: fsync always --binary final=build/distcache --binary baseline=build/distcache_phase5 --out results/always-d`

## E7: WAL durability policy

| variant | binary | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SET only, P16: fsync always | final | 6 | 64.7k | 61.4k..73.8k | 6.7% | 11.67 ms | 16.52 ms | 20.19 ms |  | 0.3 |
| SET only, P16: fsync always | baseline | 6 | 5.4k | 4.8k..5.8k | 6.8% | 132.12 ms | 281.02 ms | 343.93 ms |  | 0.1 |
| 80% GET, P16: fsync always | baseline | 6 | 5.5k | 5.2k..6.1k | 6.4% | 133.17 ms | 262.14 ms | 318.77 ms |  | 0.1 |
| SET only, P1: fsync always | final | 6 | 4.1k | 3.2k..4.3k | 10.5% | 11.53 ms | 16.12 ms | 23.07 ms |  | 0.2 |
| 80% GET, P16: fsync always | final | 6 | 71.7k | 54.0k..76.2k | 12.1% | 10.88 ms | 14.29 ms | 20.97 ms |  | 0.2 |
| SET only, P1: fsync always | baseline | 6 | 349 | 274..376 | 11.2% | 132.12 ms | 266.34 ms | 331.35 ms |  | 0.1 |

Charts: `charts/E7-throughput.svg`, `charts/E7-p99.svg`

## Binary comparison (same variant, same session, interleaved)

Each other binary against the primary (`final`). p = two-sided Mann-Whitney U on the per-run requests/s; with n < 8 per side it is only a rough guide. 'diff' is the change in the median.

| variant | binary | n | median req/s | primary median | diff | p |
|---|---|---:|---:|---:|---:|---:|
| E7 SET only, P16: fsync always | baseline | 6 / 6 | 5.4k | 64.7k | -91.7% | 0.005 |
| E7 80% GET, P16: fsync always | baseline | 6 / 6 | 5.5k | 71.7k | -92.3% | 0.005 |
| E7 SET only, P1: fsync always | baseline | 6 / 6 | 349 | 4.1k | -91.5% | 0.005 |

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
