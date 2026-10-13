# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING=Release (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 3 repeats per suite run, 2 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: final=77be965f36f96619; baseline=cc459abeeea8ea18; loadgen=0d05fc632bb6b284
- Command: `bench/harness/run_suite.py --suite all --repeats 3 --settle 2 --binary final=build/distcache_final --binary baseline=build/distcache_baseline --out results/run-b`

## E4: Concurrency design

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| single (1 thread, no locks) | 6 | 506.2k | 217.8k..552.4k | 27.3% | 1.51 ms | 2.20 ms | 3.18 ms | -74% | 1.0 |
| global mutex, 4 workers | 6 | 543.7k | 399.6k..608.5k | 13.5% | 1.33 ms | 2.62 ms | 3.38 ms | -72% | 2.5 |
| sharded, 4 workers | 6 | 1,940.9k | 1,676.8k..2,289.0k | 11.4% | 352 us | 754 us | 1.08 ms | baseline | 3.8 |
| sharded, 1 worker | 6 | 489.0k | 399.6k..517.0k | 9.0% | 1.56 ms | 2.13 ms | 3.28 ms | -75% | 1.0 |
| sharded, 8 workers | 6 | 2,719.7k | 2,416.0k..3,029.3k | 8.7% | 192 us | 762 us | 1.20 ms | +40% | 5.3 |
| sharded, 2 workers | 6 | 1,024.9k | 461.1k..1,191.3k | 28.5% | 704 us | 1.29 ms | 1.80 ms | -47% | 2.0 |

Charts: `charts/E4-throughput.svg`, `charts/E4-p99.svg`

## E7: WAL durability policy

| variant | binary | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SET only, P1: fsync no | final | 6 | 401.5k | 279.2k..436.1k | 16.2% | 98 us | 236 us | 336 us | -10% | 3.9 |
| 80% GET, P1: fsync always | baseline | 6 | 5.3k | 1.1k..6.1k | 52.2% | 8.13 ms | 21.23 ms | 30.67 ms | -99% | 0.2 |
| 80% GET, P1: fsync always | final | 6 | 62.2k | 56.9k..69.5k | 7.4% | 58 us | 3.93 ms | 5.18 ms | -84% | 1.1 |
| 80% GET, P16: fsync always | final | 6 | 217.8k | 200.7k..238.5k | 6.5% | 3.57 ms | 4.65 ms | 7.27 ms | -90% | 0.5 |
| 80% GET, P1: no persistence | final | 6 | 396.9k | 83.4k..456.3k | 39.2% | 93 us | 236 us | 377 us | baseline | 3.8 |
| 80% GET, P16: fsync everysec | baseline | 6 | 1,479.2k | 1,179.1k..2,016.9k | 20.6% | 422 us | 934 us | 1.88 ms | -30% | 3.8 |
| SET only, P1: fsync always | baseline | 6 | 1.2k | 242..1.3k | 42.8% | 38.27 ms | 74.45 ms | 83.89 ms | -100% | 0.1 |
| SET only, P16: no persistence | final | 6 | 2,219.0k | 1,883.8k..2,499.8k | 11.2% | 320 us | 647 us | 819 us | baseline | 3.8 |
| 80% GET, P16: fsync everysec | final | 6 | 1,997.4k | 1,597.5k..2,098.8k | 10.8% | 356 us | 729 us | 999 us | -6% | 3.8 |
| SET only, P1: fsync everysec | baseline | 6 | 413.2k | 247.7k..426.9k | 23.7% | 100 us | 225 us | 303 us | -7% | 3.9 |
| SET only, P1: fsync everysec | final | 6 | 421.3k | 373.7k..449.5k | 7.8% | 92 us | 221 us | 307 us | -5% | 3.9 |
| 80% GET, P1: fsync everysec | final | 6 | 413.2k | 176.8k..442.7k | 27.5% | 93 us | 223 us | 315 us | +4% | 3.8 |
| 80% GET, P1: fsync no | baseline | 6 | 435.8k | 237.8k..453.5k | 20.7% | 90 us | 211 us | 283 us | +10% | 3.8 |
| SET only, P16: fsync always | final | 6 | 193.1k | 141.1k..215.3k | 14.2% | 3.77 ms | 6.23 ms | 8.78 ms | -91% | 0.6 |
| SET only, P16: fsync no | final | 6 | 1,474.6k | 375.4k..1,681.0k | 51.5% | 463 us | 999 us | 1.52 ms | -34% | 3.9 |
| SET only, P16: fsync everysec | final | 6 | 1,567.6k | 1,326.7k..1,684.6k | 8.5% | 446 us | 909 us | 1.23 ms | -29% | 4.0 |
| SET only, P1: no persistence | final | 6 | 445.4k | 335.3k..469.5k | 11.1% | 88 us | 203 us | 278 us | baseline | 3.8 |
| SET only, P16: fsync always | baseline | 6 | 18.4k | 4.2k..19.5k | 51.8% | 40.37 ms | 78.64 ms | 87.03 ms | -99% | 0.2 |
| 80% GET, P1: fsync no | final | 6 | 394.2k | 282.1k..450.1k | 19.8% | 94 us | 236 us | 381 us | -1% | 3.7 |
| 80% GET, P16: fsync always | baseline | 6 | 19.2k | 15.2k..20.8k | 10.2% | 38.80 ms | 75.50 ms | 83.89 ms | -99% | 0.2 |
| 80% GET, P16: no persistence | final | 6 | 2,126.9k | 1,502.2k..2,363.5k | 17.1% | 336 us | 688 us | 942 us | baseline | 3.8 |
| SET only, P1: fsync always | final | 6 | 14.3k | 6.4k..15.1k | 26.4% | 3.44 ms | 4.16 ms | 6.88 ms | -97% | 0.3 |
| 80% GET, P16: fsync no | final | 6 | 2,026.3k | 1,433.8k..2,175.1k | 14.1% | 352 us | 704 us | 901 us | -5% | 3.8 |
| 80% GET, P1: fsync everysec | baseline | 6 | 427.2k | 295.0k..452.6k | 14.8% | 91 us | 217 us | 307 us | +8% | 3.8 |
| 80% GET, P16: fsync no | baseline | 6 | 1,683.1k | 1,411.4k..2,086.7k | 14.5% | 410 us | 860 us | 1.21 ms | -21% | 3.8 |
| SET only, P16: fsync everysec | baseline | 6 | 1,006.3k | 481.7k..1,337.0k | 35.2% | 598 us | 1.36 ms | 2.23 ms | -55% | 3.7 |
| SET only, P1: fsync no | baseline | 6 | 389.3k | 348.3k..435.8k | 7.7% | 104 us | 242 us | 328 us | -13% | 3.9 |
| SET only, P16: fsync no | baseline | 6 | 1,115.6k | 514.0k..1,388.6k | 35.4% | 590 us | 1.33 ms | 2.26 ms | -50% | 3.8 |

Charts: `charts/E7-throughput.svg`, `charts/E7-p99.svg`

## E2: Client count

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 clients, P16 | 6 | 2,051.9k | 934.1k..2,148.2k | 25.3% | 1.90 ms | 3.01 ms | 4.26 ms | +4% | 3.8 |
| 64 clients, P16 | 6 | 2,139.2k | 1,736.6k..2,234.4k | 9.3% | 438 us | 844 us | 1.13 ms | +9% | 3.8 |
| 64 clients, P1 | 6 | 461.2k | 387.2k..488.5k | 8.2% | 110 us | 254 us | 348 us | +6% | 3.8 |
| 16 clients, P1 | 6 | 291.6k | 257.1k..304.3k | 5.8% | 44 us | 85 us | 121 us | -33% | 3.5 |
| 4 clients, P1 | 6 | 62.9k | 57.7k..65.8k | 5.5% | 58 us | 72 us | 105 us | -86% | 1.9 |
| 50 clients, P1 | 6 | 436.3k | 202.0k..452.4k | 24.5% | 90 us | 211 us | 287 us | baseline | 3.7 |
| 16 clients, P16 | 6 | 1,914.2k | 1,187.9k..2,241.2k | 21.1% | 106 us | 234 us | 360 us | -3% | 3.8 |
| 1 clients, P16 | 6 | 232.2k | 137.4k..242.9k | 18.8% | 59 us | 92 us | 123 us | -88% | 0.6 |
| 256 clients, P1 | 6 | 474.8k | 457.3k..550.4k | 8.3% | 446 us | 909 us | 1.36 ms | +9% | 3.9 |
| 4 clients, P16 | 6 | 778.5k | 688.5k..829.0k | 7.4% | 73 us | 95 us | 148 us | -60% | 2.2 |
| 1 clients, P1 | 6 | 18.3k | 12.2k..19.1k | 14.9% | 48 us | 76 us | 108 us | -96% | 0.5 |
| 50 clients, P16 | 6 | 1,965.4k | 1,780.2k..2,238.1k | 9.5% | 356 us | 746 us | 1.04 ms | baseline | 3.8 |

Charts: `charts/E2-throughput.svg`, `charts/E2-p99.svg`

## E1: Workload mix

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 80% GET, P16 | 6 | 2,093.3k | 652.7k..2,307.6k | 40.9% | 336 us | 688 us | 934 us | baseline | 3.8 |
| SET only, P16 | 6 | 1,975.5k | 1,203.4k..2,332.8k | 22.3% | 340 us | 745 us | 1.20 ms | -6% | 3.8 |
| 50% GET, P1 | 6 | 422.8k | 343.4k..466.0k | 12.5% | 89 us | 217 us | 328 us | +20% | 3.7 |
| GET only, P1 | 6 | 430.9k | 409.7k..454.3k | 4.4% | 90 us | 211 us | 303 us | +22% | 3.7 |
| GET only, P16 | 6 | 1,949.0k | 1,754.1k..2,397.0k | 13.1% | 348 us | 762 us | 1.18 ms | -7% | 3.8 |
| SET only, P1 | 6 | 444.0k | 336.4k..469.6k | 11.7% | 88 us | 203 us | 283 us | +26% | 3.8 |
| 80% GET, P1 | 6 | 352.0k | 221.8k..446.6k | 30.8% | 92 us | 262 us | 627 us | baseline | 3.6 |
| 50% GET, P16 | 6 | 2,051.5k | 1,243.7k..2,378.7k | 20.1% | 336 us | 713 us | 1.01 ms | -2% | 3.8 |

Charts: `charts/E1-throughput.svg`, `charts/E1-p99.svg`

## E3: Pipeline depth

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| P16 | 6 | 1,975.8k | 1,629.7k..2,308.5k | 13.2% | 352 us | 737 us | 1.02 ms | +355% | 3.8 |
| P4 | 6 | 1,185.1k | 931.5k..1,252.9k | 9.8% | 148 us | 311 us | 397 us | +173% | 3.9 |
| P2 | 6 | 751.6k | 483.8k..822.7k | 17.1% | 111 us | 250 us | 344 us | +73% | 3.9 |
| P1 | 6 | 434.5k | 69.2k..448.5k | 40.1% | 90 us | 211 us | 291 us | baseline | 3.8 |
| P64 | 6 | 2,720.0k | 2,439.4k..3,208.8k | 10.1% | 1.02 ms | 2.08 ms | 2.92 ms | +526% | 3.6 |

Charts: `charts/E3-throughput.svg`, `charts/E3-p99.svg`

## E5: TTL cost

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TTL 200 ms (constant expiry churn) (hit 71%) | 6 | 1,300.3k | 987.9k..1,358.0k | 11.3% | 549 us | 1.10 ms | 1.49 ms | -38% | 3.7 |
| no TTL | 6 | 2,084.1k | 1,770.1k..2,338.2k | 11.4% | 332 us | 688 us | 1.01 ms | baseline | 3.8 |
| TTL 60 s (never fires) | 6 | 1,324.6k | 1,199.2k..1,392.9k | 6.5% | 541 us | 1.07 ms | 1.36 ms | -36% | 3.7 |

Charts: `charts/E5-throughput.svg`, `charts/E5-p99.svg`

## E6: LRU eviction cost

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| capacity 200k (no eviction) | 6 | 2,179.1k | 892.4k..2,361.9k | 27.7% | 324 us | 655 us | 836 us | +6% | 3.8 |
| capacity 50k, uniform (hit 50%) | 6 | 2,066.6k | 1,749.4k..2,376.7k | 12.2% | 348 us | 696 us | 909 us | +0% | 3.7 |
| unlimited | 6 | 2,057.0k | 1,285.0k..2,377.7k | 23.3% | 340 us | 721 us | 1.02 ms | baseline | 3.8 |
| capacity 10k, uniform (hit 10%) | 6 | 1,905.7k | 1,742.1k..2,356.0k | 12.6% | 369 us | 754 us | 1.02 ms | -7% | 3.4 |
| capacity 50k, zipf 0.99 (hit 92%) | 6 | 2,618.7k | 2,210.9k..3,031.2k | 13.0% | 262 us | 557 us | 762 us | +27% | 3.8 |

Charts: `charts/E6-throughput.svg`, `charts/E6-p99.svg`

## Binary comparison (same variant, same session, interleaved)

Each other binary against the primary (`final`). p = two-sided Mann-Whitney U on the per-run requests/s; with n < 8 per side it is only a rough guide. 'diff' is the change in the median.

| variant | binary | n | median req/s | primary median | diff | p |
|---|---|---:|---:|---:|---:|---:|
| E7 80% GET, P1: fsync always | baseline | 6 / 6 | 5.3k | 62.2k | -91.5% | 0.005 |
| E7 80% GET, P16: fsync everysec | baseline | 6 / 6 | 1,479.2k | 1,997.4k | -25.9% | 0.045 |
| E7 SET only, P1: fsync always | baseline | 6 / 6 | 1.2k | 14.3k | -91.4% | 0.005 |
| E7 SET only, P1: fsync everysec | baseline | 6 / 6 | 413.2k | 421.3k | -1.9% | 0.378 |
| E7 80% GET, P1: fsync no | baseline | 6 / 6 | 435.8k | 394.2k | +10.5% | 0.810 |
| E7 SET only, P16: fsync always | baseline | 6 / 6 | 18.4k | 193.1k | -90.5% | 0.005 |
| E7 80% GET, P16: fsync always | baseline | 6 / 6 | 19.2k | 217.8k | -91.2% | 0.005 |
| E7 80% GET, P1: fsync everysec | baseline | 6 / 6 | 427.2k | 413.2k | +3.4% | 0.471 |
| E7 80% GET, P16: fsync no | baseline | 6 / 6 | 1,683.1k | 2,026.3k | -16.9% | 0.173 |
| E7 SET only, P16: fsync everysec | baseline | 6 / 6 | 1,006.3k | 1,567.6k | -35.8% | 0.008 |
| E7 SET only, P1: fsync no | baseline | 6 / 6 | 389.3k | 401.5k | -3.1% | 0.810 |
| E7 SET only, P16: fsync no | baseline | 6 / 6 | 1,115.6k | 1,474.6k | -24.3% | 0.378 |

## Reproducibility between independent suite runs

Median requests/s of the same configuration in each independent invocation of the suite, and the
relative difference between the largest and smallest of those medians.

| config | run 1 | run 2 | max diff |
|---|---:|---:|---:|
| E1 80% GET, P1 | 428.4k | 275.6k | 43.4% |
| E7 SET only, P16: fsync everysec [baseline] | 813.1k | 1,261.9k | 43.3% |
| E7 80% GET, P1: fsync no | 349.4k | 449.9k | 25.1% |
| E6 unlimited | 1,810.6k | 2,312.9k | 24.4% |
| E7 80% GET, P16: fsync everysec [baseline] | 1,218.9k | 1,539.9k | 23.3% |
| E3 P16 | 2,132.6k | 1,698.3k | 22.7% |
| E1 50% GET, P1 | 423.2k | 346.6k | 19.9% |
| E7 80% GET, P16: no persistence | 2,339.8k | 1,951.7k | 18.1% |
| E1 80% GET, P16 | 2,292.4k | 1,934.9k | 16.9% |
| E2 256 clients, P1 | 464.9k | 541.5k | 15.2% |
| E7 80% GET, P1: fsync everysec [baseline] | 441.0k | 385.9k | 13.3% |
| E5 no TTL | 2,221.2k | 1,947.1k | 13.1% |
| E2 64 clients, P16 | 1,902.4k | 2,154.1k | 12.4% |
| E7 SET only, P16: no persistence | 2,082.2k | 2,355.8k | 12.3% |
| E7 80% GET, P1: no persistence | 372.7k | 421.1k | 12.2% |
| E5 TTL 60 s (never fires) | 1,215.5k | 1,369.9k | 11.9% |
| E4 single (1 thread, no locks) | 535.1k | 477.3k | 11.4% |
| E2 4 clients, P1 | 58.4k | 65.4k | 11.4% |
| E2 16 clients, P16 | 1,810.4k | 2,018.0k | 10.8% |
| E2 50 clients, P1 | 445.5k | 400.7k | 10.6% |
| E2 50 clients, P16 | 1,953.4k | 2,153.1k | 9.7% |
| E7 SET only, P16: fsync always | 207.2k | 189.1k | 9.1% |
| E7 80% GET, P16: fsync no | 1,853.5k | 2,027.0k | 8.9% |
| E7 SET only, P16: fsync everysec | 1,470.8k | 1,606.9k | 8.8% |
| E6 capacity 50k, zipf 0.99 | 2,732.5k | 2,504.8k | 8.7% |
| E1 SET only, P16 | 1,889.7k | 2,061.4k | 8.7% |
| E7 80% GET, P1: fsync everysec | 395.4k | 431.0k | 8.6% |
| E7 SET only, P1: fsync no [baseline] | 370.2k | 402.4k | 8.3% |
| E1 GET only, P1 | 418.6k | 453.4k | 8.0% |
| E3 P64 | 2,600.0k | 2,813.7k | 7.9% |
| E7 SET only, P16: fsync no [baseline] | 1,157.5k | 1,073.7k | 7.5% |
| E4 sharded, 8 workers | 2,818.2k | 2,621.1k | 7.2% |
| E6 capacity 50k, uniform | 1,994.4k | 2,138.8k | 7.0% |
| E7 80% GET, P16: fsync no [baseline] | 1,625.5k | 1,740.7k | 6.8% |
| E4 sharded, 2 workers | 990.1k | 1,059.6k | 6.8% |
| E7 SET only, P1: fsync everysec | 439.4k | 411.2k | 6.6% |
| E4 sharded, 1 worker | 481.3k | 511.6k | 6.1% |
| E1 50% GET, P16 | 1,945.0k | 2,066.9k | 6.1% |
| E2 1 clients, P1 | 17.5k | 18.6k | 5.9% |
| E2 16 clients, P1 | 295.3k | 278.5k | 5.9% |
| E7 80% GET, P16: fsync always | 214.5k | 227.1k | 5.7% |
| E1 SET only, P1 | 431.7k | 456.3k | 5.5% |
| E7 SET only, P1: fsync no | 398.7k | 420.8k | 5.4% |
| E4 global mutex, 4 workers | 557.9k | 529.5k | 5.2% |
| E2 256 clients, P16 | 1,999.6k | 2,104.1k | 5.1% |
| E6 capacity 200k (no eviction) | 2,227.4k | 2,117.4k | 5.1% |
| E2 4 clients, P16 | 765.6k | 804.2k | 4.9% |
| E7 80% GET, P1: fsync no [baseline] | 449.6k | 431.1k | 4.2% |
| E5 TTL 200 ms (constant expiry churn) | 1,323.0k | 1,277.6k | 3.5% |
| E7 SET only, P1: fsync always [baseline] | 1.2k | 1.3k | 3.2% |
| E7 80% GET, P1: fsync always | 63.2k | 61.2k | 3.2% |
| E3 P2 | 741.2k | 762.0k | 2.8% |
| E3 P1 | 440.1k | 428.9k | 2.6% |
| E1 GET only, P16 | 1,924.0k | 1,973.9k | 2.6% |
| E2 64 clients, P1 | 466.6k | 455.8k | 2.3% |
| E4 sharded, 4 workers | 1,961.8k | 1,920.1k | 2.1% |
| E2 1 clients, P16 | 234.5k | 229.9k | 2.0% |
| E7 SET only, P1: no persistence | 443.4k | 450.8k | 1.7% |
| E7 80% GET, P1: fsync always [baseline] | 5.3k | 5.2k | 1.5% |
| E7 SET only, P1: fsync always | 14.2k | 14.4k | 1.4% |
| E7 SET only, P1: fsync everysec [baseline] | 416.9k | 412.6k | 1.0% |
| E7 SET only, P16: fsync always [baseline] | 18.3k | 18.4k | 0.3% |
| E6 capacity 10k, uniform | 1,903.0k | 1,908.3k | 0.3% |
| E7 SET only, P16: fsync no | 1,472.9k | 1,476.3k | 0.2% |
| E3 P4 | 1,185.2k | 1,184.9k | 0.0% |
| E7 80% GET, P16: fsync everysec | 1,997.6k | 1,997.2k | 0.0% |
| E7 80% GET, P16: fsync always [baseline] | 19.2k | 19.2k | 0.0% |

Across 67 configurations: median difference 6.8%, 90th percentile 18.1%, worst 43.4%.

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
