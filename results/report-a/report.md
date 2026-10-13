# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING= (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 3 repeats per suite run, 1 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: current=cc459abeeea8ea18; loadgen=a405d8a859fa7745
- Command: `bench/harness/run_suite.py --suite all --repeats 3 --out results/run-a`

## E4: Concurrency design

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| global mutex, 4 workers | 3 | 436.7k | 436.4k..528.3k | 11.3% | 1.61 ms | 3.34 ms | 4.59 ms | -77% | 2.5 |
| single (1 thread, no locks) | 3 | 511.9k | 461.9k..545.0k | 8.3% | 1.51 ms | 2.23 ms | 3.08 ms | -73% | 1.0 |
| sharded, 4 workers | 3 | 1,921.6k | 1,778.5k..1,988.9k | 5.7% | 360 us | 754 us | 1.05 ms | baseline | 3.8 |
| sharded, 1 worker | 3 | 479.2k | 383.1k..483.7k | 12.7% | 1.61 ms | 2.36 ms | 3.21 ms | -75% | 1.0 |
| sharded, 8 workers | 3 | 2,295.0k | 1,587.2k..2,698.9k | 25.7% | 225 us | 901 us | 1.51 ms | +19% | 4.9 |
| sharded, 2 workers | 3 | 993.6k | 922.4k..1,005.4k | 4.6% | 737 us | 1.31 ms | 1.80 ms | -48% | 2.0 |

Charts: `charts/E4-throughput.svg`, `charts/E4-p99.svg`

## E2: Client count

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 50 clients, P1 | 3 | 371.1k | 342.7k..391.8k | 6.7% | 92 us | 258 us | 500 us | baseline | 3.7 |
| 4 clients, P1 | 3 | 60.3k | 52.0k..62.0k | 9.2% | 60 us | 76 us | 131 us | -84% | 1.9 |
| 256 clients, P16 | 3 | 1,685.0k | 1,085.7k..1,916.0k | 27.4% | 2.29 ms | 3.80 ms | 6.03 ms | -7% | 3.7 |
| 16 clients, P1 | 3 | 257.1k | 186.1k..270.6k | 19.1% | 47 us | 100 us | 188 us | -31% | 3.3 |
| 64 clients, P16 | 3 | 1,924.9k | 1,567.6k..2,125.0k | 15.1% | 467 us | 967 us | 1.34 ms | +6% | 3.8 |
| 4 clients, P16 | 3 | 724.5k | 589.4k..767.5k | 13.4% | 78 us | 104 us | 160 us | -60% | 2.2 |
| 64 clients, P1 | 3 | 443.3k | 427.7k..455.7k | 3.2% | 113 us | 262 us | 401 us | +19% | 3.8 |
| 1 clients, P16 | 3 | 201.8k | 197.2k..203.2k | 1.6% | 68 us | 115 us | 164 us | -89% | 0.6 |
| 50 clients, P16 | 3 | 1,811.2k | 1,745.2k..1,986.0k | 6.7% | 369 us | 819 us | 1.28 ms | baseline | 3.8 |
| 256 clients, P1 | 3 | 486.7k | 436.2k..517.9k | 8.6% | 451 us | 885 us | 1.21 ms | +31% | 3.9 |
| 16 clients, P16 | 3 | 1,841.6k | 767.8k..1,971.3k | 43.3% | 108 us | 242 us | 369 us | +2% | 3.8 |
| 1 clients, P1 | 3 | 17.3k | 17.2k..17.9k | 2.1% | 48 us | 80 us | 111 us | -95% | 0.5 |

Charts: `charts/E2-throughput.svg`, `charts/E2-p99.svg`

## E1: Workload mix

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 50% GET, P1 | 3 | 382.9k | 363.0k..423.6k | 7.9% | 88 us | 250 us | 500 us | -6% | 3.7 |
| GET only, P1 | 3 | 355.5k | 170.6k..440.0k | 42.8% | 96 us | 250 us | 508 us | -13% | 3.6 |
| 80% GET, P16 | 3 | 1,822.4k | 1,565.8k..2,035.9k | 13.0% | 377 us | 803 us | 1.15 ms | baseline | 3.8 |
| SET only, P1 | 3 | 397.0k | 396.7k..435.5k | 5.4% | 88 us | 229 us | 410 us | -2% | 3.8 |
| 80% GET, P1 | 3 | 406.5k | 371.5k..427.4k | 7.0% | 90 us | 217 us | 320 us | baseline | 3.7 |
| 50% GET, P16 | 3 | 2,052.4k | 1,633.4k..2,100.3k | 13.3% | 336 us | 688 us | 918 us | +13% | 3.7 |
| GET only, P16 | 3 | 1,994.6k | 1,439.4k..2,195.0k | 20.9% | 344 us | 721 us | 1.02 ms | +9% | 3.8 |
| SET only, P16 | 3 | 2,034.4k | 1,835.6k..2,116.9k | 7.2% | 352 us | 704 us | 901 us | +12% | 3.8 |

Charts: `charts/E1-throughput.svg`, `charts/E1-p99.svg`

## E7: WAL durability policy

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SET only, P1: no persistence | 3 | 349.5k | 327.8k..350.4k | 3.7% | 92 us | 278 us | 639 us | baseline | 3.7 |
| 80% GET, P16: fsync always | 3 | 13.4k | 5.7k..14.8k | 43.7% | 54.53 ms | 104.86 ms | 301.99 ms | -99% | 0.2 |
| 80% GET, P1: fsync always | 3 | 3.9k | 1.8k..5.5k | 50.7% | 11.27 ms | 29.88 ms | 44.04 ms | -99% | 0.2 |
| 80% GET, P16: fsync no | 3 | 1,701.3k | 1,574.6k..1,739.0k | 5.2% | 410 us | 868 us | 1.21 ms | -14% | 3.8 |
| 80% GET, P1: fsync everysec | 3 | 409.5k | 391.2k..427.4k | 4.4% | 90 us | 229 us | 369 us | +2% | 3.8 |
| SET only, P1: fsync always | 3 | 1.1k | 972..1.1k | 7.7% | 41.94 ms | 85.98 ms | 113.25 ms | -100% | 0.1 |
| SET only, P1: fsync no | 3 | 332.3k | 330.7k..354.1k | 3.9% | 108 us | 287 us | 573 us | -5% | 3.8 |
| SET only, P16: fsync everysec | 3 | 1,003.2k | 783.9k..1,050.9k | 15.1% | 606 us | 1.57 ms | 2.56 ms | -51% | 3.6 |
| 80% GET, P16: fsync everysec | 3 | 1,573.4k | 1,107.0k..1,610.9k | 19.6% | 426 us | 918 us | 1.34 ms | -21% | 3.7 |
| SET only, P16: fsync no | 3 | 1,010.0k | 469.7k..1,142.8k | 40.8% | 573 us | 1.31 ms | 1.97 ms | -51% | 3.6 |
| SET only, P16: fsync always | 3 | 14.3k | 4.6k..17.0k | 54.1% | 50.33 ms | 98.57 ms | 130.02 ms | -99% | 0.2 |
| SET only, P16: no persistence | 3 | 2,057.5k | 1,963.5k..2,072.7k | 2.9% | 336 us | 704 us | 967 us | baseline | 3.8 |
| 80% GET, P1: no persistence | 3 | 400.1k | 395.8k..433.2k | 5.0% | 90 us | 225 us | 336 us | baseline | 3.8 |
| SET only, P1: fsync everysec | 3 | 345.0k | 289.2k..353.6k | 10.6% | 106 us | 278 us | 475 us | -1% | 3.9 |
| 80% GET, P1: fsync no | 3 | 368.9k | 355.6k..401.9k | 6.3% | 98 us | 254 us | 442 us | -8% | 3.7 |
| 80% GET, P16: no persistence | 3 | 1,980.2k | 1,804.4k..1,997.1k | 5.5% | 352 us | 737 us | 1.02 ms | baseline | 3.8 |

Charts: `charts/E7-throughput.svg`, `charts/E7-p99.svg`

## E6: LRU eviction cost

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| capacity 50k, zipf 0.99 (hit 92%) | 3 | 2,331.8k | 2,321.1k..2,414.5k | 2.2% | 287 us | 639 us | 901 us | +21% | 3.7 |
| capacity 10k, uniform (hit 10%) | 3 | 1,475.0k | 1,053.1k..1,855.2k | 27.5% | 459 us | 1.05 ms | 1.61 ms | -24% | 3.3 |
| capacity 200k (no eviction) | 3 | 1,966.3k | 1,766.9k..1,980.6k | 6.3% | 360 us | 737 us | 1.03 ms | +2% | 3.8 |
| unlimited | 3 | 1,935.0k | 1,715.0k..1,997.3k | 7.9% | 360 us | 754 us | 1.03 ms | baseline | 3.8 |
| capacity 50k, uniform (hit 50%) | 3 | 1,788.6k | 1,724.5k..1,928.8k | 5.8% | 377 us | 836 us | 1.15 ms | -8% | 3.7 |

Charts: `charts/E6-throughput.svg`, `charts/E6-p99.svg`

## E3: Pipeline depth

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| P4 | 3 | 985.6k | 762.4k..1,161.6k | 20.6% | 156 us | 385 us | 754 us | +131% | 3.8 |
| P16 | 3 | 1,475.0k | 1,210.2k..2,096.9k | 28.6% | 401 us | 1.05 ms | 2.29 ms | +246% | 3.7 |
| P2 | 3 | 702.1k | 658.1k..744.4k | 6.2% | 111 us | 262 us | 385 us | +65% | 3.8 |
| P1 | 3 | 426.1k | 410.5k..437.7k | 3.2% | 88 us | 221 us | 328 us | baseline | 3.7 |
| P64 | 3 | 2,675.9k | 2,577.8k..2,746.8k | 3.2% | 1.03 ms | 2.10 ms | 2.75 ms | +528% | 3.6 |

Charts: `charts/E3-throughput.svg`, `charts/E3-p99.svg`

## E5: TTL cost

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TTL 200 ms (constant expiry churn) (hit 65%) | 3 | 1,055.9k | 936.3k..1,128.4k | 9.3% | 655 us | 1.41 ms | 2.16 ms | -41% | 3.6 |
| TTL 60 s (never fires) | 3 | 993.0k | 880.9k..1,207.9k | 16.2% | 688 us | 1.47 ms | 2.16 ms | -45% | 3.6 |
| no TTL | 3 | 1,798.0k | 1,651.5k..2,077.7k | 11.8% | 360 us | 786 us | 1.31 ms | baseline | 3.7 |

Charts: `charts/E5-throughput.svg`, `charts/E5-p99.svg`

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
