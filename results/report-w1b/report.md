# Benchmark report

## Environment

- CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz (12 logical CPUs, siblings of cpu0: 0-1), hypervisor: Microsoft
- Memory: 3776 MiB; kernel 6.18.33.2-microsoft-standard-WSL2
- Compiler: g++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0; build: CMAKE_BUILD_TYPE:STRING=Release (CMakeLists defaults to Release); flags: -O3  -DNDEBUG -std=gnu++17
- Data directory filesystem: ext2/ext3; vm.dirty_ratio=20, dirty_background_ratio=10
- Pinning: server on CPUs 0-5, load generator on CPUs 6-11
- Per measurement: 1.5 s warm-up (discarded) + 8 s measured, 5 repeats per suite run, 1 independent suite run(s)
- Git commit: cdd855d (dirty working tree)
- Binaries: diag=a4654de75515df7e; loadgen=0d05fc632bb6b284
- Command: `bench/harness/run_suite.py --suite W1 --binary diag=build/distcache_diag --repeats 5 --out results/w1-b`

## W1: WAL overhead attribution (diagnostics build)

| variant | runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| log to tmpfs (/tmp) | 5 | 1,135.6k | 1,123.2k..1,153.8k | 1.1% | 655 us | 1.25 ms | 1.51 ms | -44% | 3.6 |
| no persistence | 5 | 2,024.8k | 1,876.5k..2,046.2k | 3.5% | 360 us | 704 us | 852 us | baseline | 3.8 |
| log, flusher discards data (sink=null) | 5 | 1,113.0k | 1,089.5k..1,122.2k | 1.2% | 655 us | 1.28 ms | 1.51 ms | -45% | 3.5 |
| log to the real disk | 5 | 1,079.8k | 1,064.6k..1,100.7k | 1.5% | 688 us | 1.34 ms | 1.57 ms | -47% | 3.6 |

Instrumentation (diagnostics build; median over runs). `avg` = nanoseconds per call:

| variant | encode avg ns | append_wait avg ns | append_locked avg ns | shard_wait avg ns | shard_hold avg ns | flush_write avg ns | flush_sync avg ns | flush_swap avg ns |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| log to tmpfs (/tmp) | 255 | 283 | 61 | 392 | 1574 | 1137484 | 0 | 583 |
| no persistence | 0 | 0 | 0 | 183 | 606 | 0 | 0 | 0 |
| log, flusher discards data (sink=null) | 250 | 341 | 65 | 405 | 1650 | 197 | 0 | 612 |
| log to the real disk | 256 | 347 | 66 | 414 | 1692 | 1286729 | 0 | 700 |

Worst single observation (median over runs of each run's max), microseconds, and log-buffer behaviour:

| variant | max append_wait | max append_locked | max shard_wait | max shard_hold | max flush_write | buffer reallocations | peak pending log MiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| log to tmpfs (/tmp) | 1,159 | 1,041 | 4,103 | 4,085 | 15,052 | 3 | 2.3 |
| no persistence | 0 | 0 | 1,783 | 1,804 | 0 | 0 | 0.0 |
| log, flusher discards data (sink=null) | 847 | 589 | 3,997 | 4,090 | 2 | 2 | 1.3 |
| log to the real disk | 853 | 677 | 1,875 | 1,789 | 3,265 | 2 | 1.5 |

Charts: `charts/W1-throughput.svg`, `charts/W1-p99.svg`

## Method notes

- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).
- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during
  stalls is understated relative to an open-loop arrival process.
- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it
  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.
- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.
