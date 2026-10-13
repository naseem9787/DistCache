# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING=Release (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 12 repeats per suite run, 1 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: mutex=369b60bd86c71243; spin=77be965f36f96619; loadgen=0d05fc632bb6b284
- Command: `bench/harness/run_suite.py --suite E7 --variant-filter SET only, P16: fsync no;SET only, P16: fsync everysec;80% GET, P16: fsync no --repeats 12 --settle 3 --binary mutex=build/distcache_step2c --binary spin=build/distcache_step3b --out results/ab-spin3`

## E7: WAL durability policy

| variant | binary | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SET only, P16: fsync everysec | mutex | 12 | 1,442.3k | 777.9k..1,488.9k | 20.2% | 492 us | 991 us | 1.28 ms |  | 3.8 |
| SET only, P16: fsync everysec | spin | 12 | 1,648.7k | 699.3k..1,718.3k | 18.1% | 426 us | 844 us | 1.08 ms |  | 4.0 |
| 80% GET, P16: fsync no | spin | 12 | 2,060.7k | 1,898.2k..2,122.0k | 2.8% | 352 us | 688 us | 852 us |  | 3.9 |
| SET only, P16: fsync no | mutex | 12 | 1,492.8k | 1,307.0k..1,548.8k | 4.1% | 483 us | 950 us | 1.18 ms |  | 3.8 |
| 80% GET, P16: fsync no | mutex | 12 | 2,021.8k | 1,927.7k..2,078.3k | 2.1% | 352 us | 704 us | 877 us |  | 3.8 |
| SET only, P16: fsync no | spin | 12 | 1,694.9k | 1,449.3k..1,736.4k | 5.9% | 426 us | 836 us | 1.05 ms |  | 4.0 |

Charts: `charts/E7-throughput.svg`, `charts/E7-p99.svg`

## Binary comparison (same variant, same session, interleaved)

Each other binary against the primary (`mutex`). p = two-sided Mann-Whitney U on the per-run requests/s; with n < 8 per side it is only a rough guide. 'diff' is the change in the median.

| variant | binary | n | median req/s | primary median | diff | p |
|---|---|---:|---:|---:|---:|---:|
| E7 SET only, P16: fsync everysec | spin | 12 / 12 | 1,648.7k | 1,442.3k | +14.3% | 0.002 |
| E7 80% GET, P16: fsync no | spin | 12 / 12 | 2,060.7k | 2,021.8k | +1.9% | 0.078 |
| E7 SET only, P16: fsync no | spin | 12 / 12 | 1,694.9k | 1,492.8k | +13.5% | 0.002 |

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
