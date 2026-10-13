# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING=Release (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 6 repeats per suite run, 1 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: baseline=cc459abeeea8ea18; step1_deferred_ack=51bc8ecb521f58c0; step2_fast_crc=369b60bd86c71243; final=77be965f36f96619; loadgen=0d05fc632bb6b284
- Command: `bench/harness/run_suite.py --suite E7 --repeats 6 --settle 3 --variant-filter SET only, P1: fsync always;SET only, P16: fsync always;SET only, P16: fsync no;SET only, P16: fsync everysec;SET only, P16: no persistence --binary baseline=build/distcache_baseline --binary step1_deferred_ack=build/distcache_step1 --binary step2_fast_crc=build/distcache_step2c --binary final=build/distcache_final --out results/ladder`

## E7: WAL durability policy

| variant | binary | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SET only, P16: fsync everysec | step1_deferred_ack | 6 | 738.0k | 575.1k..886.7k | 14.4% | 639 us | 1.62 ms | 2.62 ms | -66% | 2.7 |
| SET only, P16: fsync no | baseline | 6 | 719.5k | 590.1k..983.9k | 21.2% | 565 us | 1.26 ms | 2.05 ms | -66% | 3.0 |
| SET only, P16: fsync always | step2_fast_crc | 6 | 45.9k | 21.1k..57.4k | 27.9% | 13.63 ms | 36.70 ms | 86.51 ms | -98% | 0.2 |
| SET only, P16: fsync always | step1_deferred_ack | 6 | 48.8k | 39.4k..69.8k | 23.1% | 13.63 ms | 35.65 ms | 49.28 ms | -98% | 0.2 |
| SET only, P16: fsync everysec | baseline | 6 | 644.1k | 523.1k..852.0k | 18.1% | 623 us | 1.49 ms | 2.46 ms | -70% | 2.8 |
| SET only, P1: fsync always | baseline | 6 | 240 | 124..319 | 28.1% | 184.55 ms | 373.29 ms | 595.59 ms |  | 0.0 |
| SET only, P16: fsync no | final | 6 | 764.6k | 482.5k..1,186.0k | 33.9% | 459 us | 1.03 ms | 1.97 ms | -64% | 2.8 |
| SET only, P16: fsync always | baseline | 6 | 4.1k | 2.9k..5.5k | 22.3% | 163.58 ms | 335.54 ms | 515.90 ms | -100% | 0.1 |
| SET only, P1: fsync always | final | 6 | 2.9k | 1.5k..4.3k | 34.4% | 12.85 ms | 44.56 ms | 56.10 ms |  | 0.1 |
| SET only, P16: fsync no | step1_deferred_ack | 6 | 748.0k | 677.8k..1,174.2k | 23.8% | 573 us | 1.36 ms | 2.65 ms | -65% | 3.2 |
| SET only, P16: fsync everysec | step2_fast_crc | 6 | 727.0k | 405.9k..1,030.1k | 29.3% | 590 us | 1.44 ms | 2.36 ms | -66% | 2.6 |
| SET only, P16: fsync everysec | final | 6 | 584.9k | 325.9k..1,191.4k | 47.6% | 496 us | 1.21 ms | 2.39 ms | -73% | 3.7 |
| SET only, P16: fsync no | step2_fast_crc | 6 | 922.1k | 620.3k..1,166.8k | 21.2% | 508 us | 1.18 ms | 2.02 ms | -57% | 3.0 |
| SET only, P1: fsync always | step1_deferred_ack | 6 | 3.2k | 2.5k..3.6k | 15.9% | 12.32 ms | 44.04 ms | 50.33 ms |  | 0.1 |
| SET only, P16: fsync always | final | 6 | 49.2k | 34.4k..69.0k | 28.0% | 12.58 ms | 45.61 ms | 51.90 ms | -98% | 0.2 |
| SET only, P1: fsync always | step2_fast_crc | 6 | 3.4k | 2.9k..4.4k | 15.3% | 12.85 ms | 23.59 ms | 39.32 ms |  | 0.1 |
| SET only, P16: no persistence | baseline | 6 | 2,140.9k | 1,222.8k..2,299.4k | 22.3% | 332 us | 680 us | 860 us | baseline | 3.8 |

Charts: `charts/E7-throughput.svg`, `charts/E7-p99.svg`

## Binary comparison (same variant, same session, interleaved)

Each other binary against the primary (`baseline`). p = two-sided Mann-Whitney U on the per-run requests/s; with n < 8 per side it is only a rough guide. 'diff' is the change in the median.

| variant | binary | n | median req/s | primary median | diff | p |
|---|---|---:|---:|---:|---:|---:|
| E7 SET only, P16: fsync everysec | step1_deferred_ack | 6 / 6 | 738.0k | 644.1k | +14.6% | 0.471 |
| E7 SET only, P16: fsync always | step2_fast_crc | 6 / 6 | 45.9k | 4.1k | +1012.6% | 0.005 |
| E7 SET only, P16: fsync always | step1_deferred_ack | 6 / 6 | 48.8k | 4.1k | +1083.9% | 0.005 |
| E7 SET only, P16: fsync no | final | 6 / 6 | 764.6k | 719.5k | +6.3% | 0.936 |
| E7 SET only, P1: fsync always | final | 6 / 6 | 2.9k | 240 | +1098.4% | 0.005 |
| E7 SET only, P16: fsync no | step1_deferred_ack | 6 / 6 | 748.0k | 719.5k | +4.0% | 0.471 |
| E7 SET only, P16: fsync everysec | step2_fast_crc | 6 / 6 | 727.0k | 644.1k | +12.9% | 0.575 |
| E7 SET only, P16: fsync everysec | final | 6 / 6 | 584.9k | 644.1k | -9.2% | 0.689 |
| E7 SET only, P16: fsync no | step2_fast_crc | 6 / 6 | 922.1k | 719.5k | +28.2% | 0.230 |
| E7 SET only, P1: fsync always | step1_deferred_ack | 6 / 6 | 3.2k | 240 | +1230.9% | 0.005 |
| E7 SET only, P16: fsync always | final | 6 / 6 | 49.2k | 4.1k | +1092.8% | 0.005 |
| E7 SET only, P1: fsync always | step2_fast_crc | 6 / 6 | 3.4k | 240 | +1307.3% | 0.005 |

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
