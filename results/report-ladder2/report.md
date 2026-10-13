# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING=Release (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 8 repeats per suite run, 1 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: baseline=cc459abeeea8ea18; step1_deferred_ack=51bc8ecb521f58c0; step2_fast_crc=369b60bd86c71243; final_spin=77be965f36f96619; loadgen=0d05fc632bb6b284
- Command: `bench/harness/run_suite.py --suite E7 --repeats 8 --settle 4 --variant-filter SET only, P16: fsync no;SET only, P16: fsync everysec;SET only, P16: no persistence --binary baseline=build/distcache_baseline --binary step1_deferred_ack=build/distcache_step1 --binary step2_fast_crc=build/distcache_step2c --binary final_spin=build/distcache_final --out results/ladder2`

## E7: WAL durability policy

| variant | binary | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SET only, P16: fsync everysec | baseline | 8 | 1,160.9k | 471.0k..1,235.5k | 23.3% | 590 us | 1.26 ms | 1.70 ms | -43% | 3.8 |
| SET only, P16: fsync everysec | step1_deferred_ack | 8 | 1,113.8k | 869.4k..1,232.6k | 13.8% | 598 us | 1.29 ms | 1.79 ms | -45% | 3.7 |
| SET only, P16: fsync everysec | step2_fast_crc | 8 | 1,215.8k | 977.0k..1,325.0k | 9.6% | 549 us | 1.23 ms | 1.93 ms | -40% | 3.8 |
| SET only, P16: fsync no | final_spin | 8 | 1,339.4k | 957.3k..1,447.3k | 15.0% | 500 us | 1.11 ms | 1.82 ms | -34% | 4.0 |
| SET only, P16: fsync no | step2_fast_crc | 8 | 1,291.3k | 830.4k..1,353.9k | 14.1% | 549 us | 1.11 ms | 1.44 ms | -37% | 3.9 |
| SET only, P16: no persistence | baseline | 8 | 2,043.1k | 1,437.3k..2,095.2k | 11.2% | 344 us | 704 us | 909 us | baseline | 3.8 |
| SET only, P16: fsync everysec | final_spin | 8 | 1,321.9k | 1,258.5k..1,472.0k | 6.0% | 492 us | 1.02 ms | 1.43 ms | -35% | 4.0 |
| SET only, P16: fsync no | baseline | 8 | 1,127.6k | 918.9k..1,260.8k | 10.6% | 606 us | 1.28 ms | 1.79 ms | -45% | 3.9 |
| SET only, P16: fsync no | step1_deferred_ack | 8 | 1,122.2k | 715.5k..1,238.9k | 15.3% | 598 us | 1.26 ms | 1.90 ms | -45% | 3.8 |

Charts: `charts/E7-throughput.svg`, `charts/E7-p99.svg`

## Binary comparison (same variant, same session, interleaved)

Each other binary against the primary (`baseline`). p = two-sided Mann-Whitney U on the per-run requests/s; with n < 8 per side it is only a rough guide. 'diff' is the change in the median.

| variant | binary | n | median req/s | primary median | diff | p |
|---|---|---:|---:|---:|---:|---:|
| E7 SET only, P16: fsync everysec | step1_deferred_ack | 8 / 8 | 1,113.8k | 1,160.9k | -4.1% | 0.431 |
| E7 SET only, P16: fsync everysec | step2_fast_crc | 8 / 8 | 1,215.8k | 1,160.9k | +4.7% | 0.189 |
| E7 SET only, P16: fsync no | final_spin | 8 / 8 | 1,339.4k | 1,127.6k | +18.8% | 0.066 |
| E7 SET only, P16: fsync no | step2_fast_crc | 8 / 8 | 1,291.3k | 1,127.6k | +14.5% | 0.066 |
| E7 SET only, P16: fsync everysec | final_spin | 8 / 8 | 1,321.9k | 1,160.9k | +13.9% | 0.001 |
| E7 SET only, P16: fsync no | step1_deferred_ack | 8 / 8 | 1,122.2k | 1,127.6k | -0.5% | 1.042 |

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
