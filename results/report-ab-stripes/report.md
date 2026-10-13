# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING=Release (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 5 repeats per suite run, 1 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: step2=31b8aa6207d2895f; step3=dde23aab4c7e8b82; loadgen=0d05fc632bb6b284
- Command: `bench/harness/run_suite.py --suite E7 --variant-filter SET only, P16 --repeats 5 --binary step2=build/distcache_step2 --binary step3=build/distcache_step3 --out results/ab-stripes`

## E7: WAL durability policy

| variant | binary | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 80% GET, P16: fsync always | step3 | 5 | 91.2k | 79.0k..204.4k | 45.1% | 8.91 ms | 11.53 ms | 13.89 ms | -96% | 0.2 |
| 80% GET, P16: fsync everysec | step2 | 5 | 1,475.4k | 1,232.0k..1,899.6k | 17.8% | 418 us | 967 us | 1.90 ms | -29% | 3.7 |
| 80% GET, P16: fsync always | step2 | 5 | 94.6k | 68.8k..234.4k | 55.9% | 8.26 ms | 9.96 ms | 16.78 ms | -95% | 0.3 |
| SET only, P16: fsync everysec | step3 | 5 | 1,393.7k | 720.9k..1,550.5k | 34.4% | 483 us | 1.03 ms | 1.51 ms | -32% | 3.9 |
| SET only, P1: fsync always | step2 | 5 | 11.8k | 3.0k..14.5k | 49.6% | 3.80 ms | 5.90 ms | 10.75 ms | -97% | 0.4 |
| SET only, P16: fsync no | step3 | 5 | 1,206.9k | 1,166.4k..1,471.8k | 11.7% | 475 us | 1.02 ms | 1.80 ms | -42% | 4.0 |
| SET only, P1: no persistence | step2 | 5 | 432.6k | 415.9k..446.6k | 3.0% | 88 us | 205 us | 295 us | baseline | 3.8 |
| 80% GET, P16: no persistence | step2 | 5 | 2,066.5k | 1,724.2k..2,271.6k | 11.9% | 344 us | 704 us | 918 us | baseline | 3.8 |
| SET only, P1: fsync no | step2 | 5 | 366.1k | 255.3k..426.9k | 21.1% | 102 us | 250 us | 393 us | -15% | 3.7 |
| 80% GET, P16: fsync no | step3 | 5 | 1,932.3k | 1,438.8k..1,950.9k | 12.0% | 369 us | 754 us | 950 us | -6% | 3.8 |
| SET only, P1: fsync always | step3 | 5 | 5.0k | 3.1k..11.8k | 58.9% | 8.91 ms | 17.30 ms | 21.50 ms | -99% | 0.2 |
| SET only, P16: fsync everysec | step2 | 5 | 1,018.4k | 288.3k..1,300.4k | 59.2% | 573 us | 1.41 ms | 2.23 ms | -51% | 3.4 |
| SET only, P16: fsync always | step3 | 5 | 78.9k | 25.7k..143.9k | 51.4% | 9.70 ms | 13.11 ms | 41.94 ms | -96% | 0.3 |
| SET only, P16: no persistence | step2 | 5 | 2,064.7k | 1,170.3k..2,167.7k | 21.9% | 344 us | 704 us | 901 us | baseline | 3.8 |
| SET only, P16: fsync always | step2 | 5 | 110.2k | 66.2k..177.1k | 42.4% | 7.86 ms | 14.94 ms | 44.04 ms | -95% | 0.4 |
| 80% GET, P16: fsync no | step2 | 5 | 1,953.5k | 1,162.7k..2,030.2k | 20.2% | 369 us | 737 us | 918 us | -5% | 3.8 |
| SET only, P1: fsync everysec | step2 | 5 | 390.3k | 328.6k..402.0k | 7.7% | 100 us | 242 us | 352 us | -10% | 3.9 |
| SET only, P16: fsync no | step2 | 5 | 1,352.5k | 776.2k..1,429.1k | 27.5% | 541 us | 1.08 ms | 1.38 ms | -34% | 3.5 |
| SET only, P1: fsync no | step3 | 5 | 408.7k | 320.9k..425.0k | 10.8% | 98 us | 234 us | 320 us | -6% | 3.9 |
| 80% GET, P16: fsync everysec | step3 | 5 | 1,907.0k | 1,764.6k..2,000.0k | 4.9% | 377 us | 770 us | 1.02 ms | -8% | 3.8 |
| SET only, P1: fsync everysec | step3 | 5 | 405.2k | 350.9k..413.7k | 7.9% | 98 us | 229 us | 328 us | -6% | 3.9 |

Charts: `charts/E7-throughput.svg`, `charts/E7-p99.svg`

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
