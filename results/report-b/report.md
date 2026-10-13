# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING=Release (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 3 repeats per suite run, 1 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: final=77be965f36f96619; baseline=cc459abeeea8ea18; loadgen=0d05fc632bb6b284
- Command: `bench/harness/run_suite.py --suite all --repeats 3 --settle 2 --binary final=build/distcache_final --binary baseline=build/distcache_baseline --out results/run-b`

## E4: Concurrency design

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| single (1 thread, no locks) | 3 | 535.1k | 444.5k..537.7k | 10.5% | 1.47 ms | 1.97 ms | 3.01 ms | -73% | 1.0 |
| global mutex, 4 workers | 3 | 557.9k | 525.8k..608.5k | 7.4% | 1.31 ms | 2.56 ms | 3.21 ms | -72% | 2.5 |
| sharded, 4 workers | 3 | 1,961.8k | 1,892.2k..2,289.0k | 10.3% | 352 us | 737 us | 1.02 ms | baseline | 3.8 |
| sharded, 1 worker | 3 | 481.3k | 399.6k..496.6k | 11.4% | 1.57 ms | 2.36 ms | 3.41 ms | -75% | 1.0 |
| sharded, 8 workers | 3 | 2,818.2k | 2,416.0k..2,954.7k | 10.3% | 188 us | 737 us | 1.15 ms | +44% | 5.2 |
| sharded, 2 workers | 3 | 990.1k | 812.5k..1,115.8k | 15.7% | 721 us | 1.34 ms | 1.87 ms | -50% | 2.0 |

Charts: `charts/E4-throughput.svg`, `charts/E4-p99.svg`

## E7: WAL durability policy

| variant | binary | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SET only, P1: fsync no | final | 3 | 398.7k | 326.7k..404.4k | 11.5% | 100 us | 238 us | 336 us | -10% | 3.9 |
| 80% GET, P1: fsync always | baseline | 3 | 5.3k | 1.7k..6.1k | 53.8% | 8.13 ms | 20.97 ms | 28.84 ms | -99% | 0.2 |
| 80% GET, P1: fsync always | final | 3 | 63.2k | 60.6k..67.6k | 5.5% | 58 us | 3.93 ms | 4.98 ms | -83% | 1.1 |
| 80% GET, P16: fsync always | final | 3 | 214.5k | 200.7k..221.2k | 4.9% | 3.67 ms | 4.72 ms | 7.34 ms | -91% | 0.5 |
| 80% GET, P1: no persistence | final | 3 | 372.7k | 355.9k..451.4k | 13.0% | 94 us | 254 us | 451 us | baseline | 3.7 |
| 80% GET, P16: fsync everysec | baseline | 3 | 1,218.9k | 1,179.1k..1,655.8k | 19.6% | 451 us | 1.15 ms | 2.49 ms | -48% | 3.7 |
| SET only, P1: fsync always | baseline | 3 | 1.2k | 718..1.2k | 27.6% | 38.80 ms | 75.50 ms | 83.89 ms | -100% | 0.1 |
| SET only, P16: no persistence | final | 3 | 2,082.2k | 2,044.3k..2,446.0k | 10.1% | 336 us | 688 us | 885 us | baseline | 3.8 |
| 80% GET, P16: fsync everysec | final | 3 | 1,997.6k | 1,597.5k..2,098.8k | 14.0% | 360 us | 721 us | 918 us | -15% | 3.9 |
| SET only, P1: fsync everysec | baseline | 3 | 416.9k | 247.7k..426.9k | 27.7% | 100 us | 225 us | 295 us | -6% | 3.9 |
| SET only, P1: fsync everysec | final | 3 | 439.4k | 376.7k..449.5k | 9.4% | 90 us | 213 us | 287 us | -1% | 3.9 |
| 80% GET, P1: fsync everysec | final | 3 | 395.4k | 176.8k..439.6k | 41.7% | 94 us | 234 us | 344 us | +6% | 3.8 |
| 80% GET, P1: fsync no | baseline | 3 | 449.6k | 237.8k..453.5k | 32.4% | 88 us | 201 us | 270 us | +21% | 3.8 |
| SET only, P16: fsync always | final | 3 | 207.2k | 141.1k..215.3k | 21.7% | 3.67 ms | 4.98 ms | 7.08 ms | -90% | 0.6 |
| SET only, P16: fsync no | final | 3 | 1,472.9k | 375.4k..1,681.0k | 59.6% | 459 us | 999 us | 1.57 ms | -29% | 3.9 |
| SET only, P16: fsync everysec | final | 3 | 1,470.8k | 1,326.7k..1,684.6k | 12.1% | 459 us | 983 us | 1.41 ms | -29% | 4.0 |
| SET only, P1: no persistence | final | 3 | 443.4k | 441.2k..447.3k | 0.7% | 90 us | 205 us | 287 us | baseline | 3.8 |
| SET only, P16: fsync always | baseline | 3 | 18.3k | 5.2k..18.9k | 54.8% | 40.89 ms | 79.69 ms | 85.98 ms | -99% | 0.2 |
| 80% GET, P1: fsync no | final | 3 | 349.4k | 282.1k..439.0k | 22.1% | 98 us | 287 us | 483 us | -6% | 3.8 |
| 80% GET, P16: fsync always | baseline | 3 | 19.2k | 15.2k..19.8k | 13.8% | 38.80 ms | 75.50 ms | 83.89 ms | -99% | 0.2 |
| 80% GET, P16: no persistence | final | 3 | 2,339.8k | 1,807.9k..2,363.5k | 14.5% | 311 us | 606 us | 754 us | baseline | 3.8 |
| SET only, P1: fsync always | final | 3 | 14.2k | 6.4k..14.4k | 38.8% | 3.47 ms | 4.33 ms | 6.95 ms | -97% | 0.3 |
| 80% GET, P16: fsync no | final | 3 | 1,853.5k | 1,433.8k..2,146.6k | 19.8% | 385 us | 770 us | 1.02 ms | -21% | 3.8 |
| 80% GET, P1: fsync everysec | baseline | 3 | 441.0k | 413.3k..450.7k | 4.5% | 90 us | 209 us | 278 us | +18% | 3.8 |
| 80% GET, P16: fsync no | baseline | 3 | 1,625.5k | 1,556.4k..1,936.2k | 11.9% | 418 us | 901 us | 1.31 ms | -31% | 3.8 |
| SET only, P16: fsync everysec | baseline | 3 | 813.1k | 746.5k..1,199.4k | 26.6% | 623 us | 1.47 ms | 2.69 ms | -61% | 3.7 |
| SET only, P1: fsync no | baseline | 3 | 370.2k | 348.3k..397.2k | 6.6% | 104 us | 254 us | 418 us | -17% | 3.9 |
| SET only, P16: fsync no | baseline | 3 | 1,157.5k | 621.3k..1,259.4k | 33.8% | 573 us | 1.28 ms | 2.16 ms | -44% | 3.8 |

Charts: `charts/E7-throughput.svg`, `charts/E7-p99.svg`

## E2: Client count

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 clients, P16 | 3 | 1,999.6k | 934.1k..2,148.2k | 39.1% | 1.90 ms | 3.21 ms | 4.46 ms | +2% | 3.8 |
| 64 clients, P16 | 3 | 1,902.4k | 1,736.6k..2,234.4k | 12.9% | 459 us | 950 us | 1.41 ms | -3% | 3.8 |
| 64 clients, P1 | 3 | 466.6k | 424.5k..471.1k | 5.7% | 108 us | 250 us | 344 us | +5% | 3.8 |
| 16 clients, P1 | 3 | 295.3k | 290.9k..304.3k | 2.3% | 44 us | 84 us | 113 us | -34% | 3.5 |
| 4 clients, P1 | 3 | 58.4k | 57.7k..62.6k | 4.4% | 60 us | 86 us | 148 us | -87% | 1.9 |
| 50 clients, P1 | 3 | 445.5k | 427.2k..447.0k | 2.5% | 90 us | 205 us | 278 us | baseline | 3.8 |
| 16 clients, P16 | 3 | 1,810.4k | 1,578.0k..2,241.2k | 17.9% | 111 us | 250 us | 426 us | -7% | 3.8 |
| 1 clients, P16 | 3 | 234.5k | 205.9k..241.2k | 8.3% | 58 us | 94 us | 125 us | -88% | 0.6 |
| 256 clients, P1 | 3 | 464.9k | 457.3k..479.8k | 2.5% | 475 us | 950 us | 1.44 ms | +4% | 3.9 |
| 4 clients, P16 | 3 | 765.6k | 688.5k..791.3k | 7.1% | 74 us | 96 us | 148 us | -61% | 2.2 |
| 1 clients, P1 | 3 | 17.5k | 12.2k..18.8k | 21.5% | 49 us | 80 us | 111 us | -96% | 0.5 |
| 50 clients, P16 | 3 | 1,953.4k | 1,780.2k..1,977.3k | 5.7% | 360 us | 754 us | 1.08 ms | baseline | 3.8 |

Charts: `charts/E2-throughput.svg`, `charts/E2-p99.svg`

## E1: Workload mix

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 80% GET, P16 | 3 | 2,292.4k | 1,062.9k..2,307.6k | 37.8% | 320 us | 623 us | 770 us | baseline | 3.8 |
| SET only, P16 | 3 | 1,889.7k | 1,203.4k..2,332.8k | 31.5% | 336 us | 786 us | 1.47 ms | -18% | 3.8 |
| 50% GET, P1 | 3 | 423.2k | 422.3k..466.0k | 5.7% | 88 us | 217 us | 328 us | -1% | 3.7 |
| GET only, P1 | 3 | 418.6k | 409.7k..439.9k | 3.7% | 90 us | 213 us | 320 us | -2% | 3.7 |
| GET only, P16 | 3 | 1,924.0k | 1,754.1k..2,397.0k | 16.4% | 352 us | 803 us | 1.18 ms | -16% | 3.8 |
| SET only, P1 | 3 | 431.7k | 402.6k..458.4k | 6.5% | 88 us | 213 us | 303 us | +1% | 3.8 |
| 80% GET, P1 | 3 | 428.4k | 249.7k..444.4k | 28.9% | 92 us | 213 us | 303 us | baseline | 3.8 |
| 50% GET, P16 | 3 | 1,945.0k | 1,243.7k..2,274.3k | 28.9% | 352 us | 754 us | 1.11 ms | -15% | 3.8 |

Charts: `charts/E1-throughput.svg`, `charts/E1-p99.svg`

## E3: Pipeline depth

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| P16 | 3 | 2,132.6k | 2,028.7k..2,308.5k | 6.6% | 336 us | 672 us | 868 us | +385% | 3.8 |
| P4 | 3 | 1,185.2k | 1,179.5k..1,200.9k | 0.9% | 148 us | 311 us | 401 us | +169% | 3.9 |
| P2 | 3 | 741.2k | 658.5k..763.2k | 7.7% | 113 us | 254 us | 344 us | +68% | 3.9 |
| P1 | 3 | 440.1k | 423.4k..448.1k | 2.9% | 90 us | 209 us | 278 us | baseline | 3.8 |
| P64 | 3 | 2,600.0k | 2,439.4k..2,976.8k | 10.3% | 1.02 ms | 2.23 ms | 3.28 ms | +491% | 3.6 |

Charts: `charts/E3-throughput.svg`, `charts/E3-p99.svg`

## E5: TTL cost

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TTL 200 ms (constant expiry churn) (hit 71%) | 3 | 1,323.0k | 987.9k..1,331.5k | 16.1% | 541 us | 1.08 ms | 1.47 ms | -40% | 3.6 |
| no TTL | 3 | 2,221.2k | 1,770.1k..2,338.2k | 14.2% | 320 us | 639 us | 803 us | baseline | 3.8 |
| TTL 60 s (never fires) | 3 | 1,215.5k | 1,199.2k..1,368.4k | 7.4% | 590 us | 1.18 ms | 1.51 ms | -45% | 3.7 |

Charts: `charts/E5-throughput.svg`, `charts/E5-p99.svg`

## E6: LRU eviction cost

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| capacity 200k (no eviction) | 3 | 2,227.4k | 2,130.8k..2,361.9k | 5.2% | 320 us | 639 us | 803 us | +23% | 3.8 |
| capacity 50k, uniform (hit 50%) | 3 | 1,994.4k | 1,749.4k..2,341.5k | 14.7% | 360 us | 721 us | 950 us | +10% | 3.7 |
| unlimited | 3 | 1,810.6k | 1,285.0k..2,303.4k | 28.3% | 369 us | 819 us | 1.25 ms | baseline | 3.8 |
| capacity 10k, uniform (hit 10%) | 3 | 1,903.0k | 1,742.1k..2,223.7k | 12.5% | 369 us | 754 us | 1.02 ms | +5% | 3.4 |
| capacity 50k, zipf 0.99 (hit 92%) | 3 | 2,732.5k | 2,218.4k..2,831.5k | 12.7% | 254 us | 524 us | 672 us | +51% | 3.8 |

Charts: `charts/E6-throughput.svg`, `charts/E6-p99.svg`

## Binary comparison (same variant, same session, interleaved)

Each other binary against the primary (`final`). p = two-sided Mann-Whitney U on the per-run requests/s; with n < 8 per side it is only a rough guide. 'diff' is the change in the median.

| variant | binary | n | median req/s | primary median | diff | p |
|---|---|---:|---:|---:|---:|---:|
| E7 80% GET, P1: fsync always | baseline | 3 / 3 | 5.3k | 63.2k | -91.6% | 0.081 |
| E7 80% GET, P16: fsync everysec | baseline | 3 / 3 | 1,218.9k | 1,997.6k | -39.0% | 0.190 |
| E7 SET only, P1: fsync always | baseline | 3 / 3 | 1.2k | 14.2k | -91.4% | 0.081 |
| E7 SET only, P1: fsync everysec | baseline | 3 / 3 | 416.9k | 439.4k | -5.1% | 0.383 |
| E7 80% GET, P1: fsync no | baseline | 3 / 3 | 449.6k | 349.4k | +28.6% | 0.663 |
| E7 SET only, P16: fsync always | baseline | 3 / 3 | 18.3k | 207.2k | -91.2% | 0.081 |
| E7 80% GET, P16: fsync always | baseline | 3 / 3 | 19.2k | 214.5k | -91.1% | 0.081 |
| E7 80% GET, P1: fsync everysec | baseline | 3 / 3 | 441.0k | 395.4k | +11.5% | 0.190 |
| E7 80% GET, P16: fsync no | baseline | 3 / 3 | 1,625.5k | 1,853.5k | -12.3% | 1.000 |
| E7 SET only, P16: fsync everysec | baseline | 3 / 3 | 813.1k | 1,470.8k | -44.7% | 0.081 |
| E7 SET only, P1: fsync no | baseline | 3 / 3 | 370.2k | 398.7k | -7.1% | 0.663 |
| E7 SET only, P16: fsync no | baseline | 3 / 3 | 1,157.5k | 1,472.9k | -21.4% | 0.663 |

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
