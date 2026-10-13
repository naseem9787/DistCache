# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING=Release (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 6 repeats per suite run, 1 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: mutex=369b60bd86c71243; spin=77be965f36f96619; loadgen=0d05fc632bb6b284
- Command: `bench/harness/run_suite.py --suite E7 --variant-filter SET only, P16: no persistence;SET only, P16: fsync no;SET only, P16: fsync everysec --repeats 6 --binary mutex=build/distcache_step2c --binary spin=build/distcache_step3b --out results/ab-spin`

## E7: WAL durability policy

| variant | binary | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SET only, P16: fsync no | spin | 6 | 1,106.8k | 802.3k..1,578.0k | 23.3% | 451 us | 901 us | 1.20 ms | -45% | 3.9 |
| SET only, P16: fsync everysec | mutex | 6 | 575.1k | 447.9k..1,083.8k | 36.8% | 565 us | 1.20 ms | 2.03 ms | -72% | 2.3 |
| SET only, P16: fsync everysec | spin | 6 | 1,025.9k | 328.6k..1,381.1k | 41.4% | 459 us | 926 us | 1.26 ms | -49% | 4.0 |
| SET only, P16: no persistence | mutex | 6 | 2,018.2k | 1,966.3k..2,288.1k | 6.2% | 352 us | 713 us | 909 us | baseline | 3.8 |
| SET only, P16: fsync no | mutex | 6 | 967.6k | 522.4k..1,428.1k | 33.1% | 512 us | 1.13 ms | 1.77 ms | -52% | 3.5 |

Charts: `charts/E7-throughput.svg`, `charts/E7-p99.svg`

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
