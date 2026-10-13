# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING=Release (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 3 repeats per suite run, 1 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: diag=b3f18bde846bc7a8; loadgen=0d05fc632bb6b284
- Command: `bench/harness/run_suite.py --suite W1 --binary diag=build/distcache_diag --repeats 3 --out results/w1-a`

## W1: WAL overhead attribution (diagnostics build)

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| log to tmpfs (/tmp) | 3 | 833.6k | 810.1k..843.6k | 2.1% | 754 us | 1.90 ms | 3.28 ms | -47% | 3.7 |
| no persistence | 3 | 1,561.8k | 1,167.3k..1,654.8k | 17.7% | 410 us | 983 us | 1.80 ms | baseline | 3.7 |
| log, flusher discards data (sink=null) | 3 | 854.8k | 851.0k..949.3k | 6.3% | 737 us | 1.74 ms | 2.82 ms | -45% | 3.5 |
| log to the real disk | 3 | 945.5k | 820.8k..954.2k | 8.2% | 737 us | 1.51 ms | 2.49 ms | -39% | 3.7 |

Instrumentation (diagnostics build; median over runs). `avg` = nanoseconds per call:

| variant | encode avg ns | append_wait avg ns | append_locked avg ns | shard_wait avg ns | shard_hold avg ns | flush_write avg ns | flush_sync avg ns | flush_swap avg ns |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| log to tmpfs (/tmp) | 498 | 277 | 64 | 570 | 2082 | 2508112 | 0 | 752 |
| no persistence | 0 | 0 | 0 | 259 | 799 | 0 | 0 | 0 |
| log, flusher discards data (sink=null) | 483 | 283 | 63 | 517 | 2027 | 226 | 0 | 624 |
| log to the real disk | 477 | 287 | 64 | 483 | 1910 | 1699899 | 0 | 729 |

Charts: `charts/W1-throughput.svg`, `charts/W1-p99.svg`

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
