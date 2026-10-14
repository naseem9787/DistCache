# Cluster benchmark report

Environment: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz, 12 logical CPUs, kernel 6.18.33.2-microsoft-standard-WSL2; nodes pinned to CPUs [0, 2, 4, 1] (one each), router on CPUs 6-7, load generator on CPUs 8-11. 8 s measured (+1.5 s warm-up) per run, 5 repeats, commit 12d938b (dirty).

## Scale-out: nodes x topology

| variant | runs | req/s (median) | min..max | CV | p50 | p99 | hit rate | node CPU (cores) | router CPU (cores) | keys per node (last run) |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---|
| node-1, P1 | 5 | 73.1k | 60.3k..91.5k | 14.4% | 672 us | 1,638 us | 100.0% | 0.99 | 0.00 | [100000] |
| smart-2, P1 | 5 | 127.5k | 122.8k..128.9k | 1.7% | 336 us | 1,376 us | 100.0% | 1.94 | 0.00 | [51171, 48829] |
| smart-3, P1 | 5 | 223.3k | 219.3k..234.7k | 2.4% | 139 us | 983 us | 100.0% | 2.79 | 0.00 | [36463, 31673, 31864] |
| router-1, P1 | 5 | 67.5k | 66.2k..76.6k | 5.6% | 672 us | 2,032 us | 100.0% | 0.65 | 1.96 | [100000] |
| router-2, P1 | 5 | 53.4k | 52.8k..55.6k | 1.9% | 868 us | 2,556 us | 100.0% | 0.72 | 1.97 | [51171, 48829] |
| router-3, P1 | 5 | 48.3k | 48.2k..57.3k | 7.1% | 967 us | 2,752 us | 100.0% | 0.75 | 1.97 | [36463, 31673, 31864] |
| node-1, P16 | 5 | 404.6k | 383.1k..497.5k | 9.5% | 1,737 us | 3,932 us | 100.0% | 0.99 | 0.00 | [100000] |
| smart-2, P16 | 5 | 599.3k | 579.3k..604.5k | 1.5% | 967 us | 2,949 us | 100.0% | 1.97 | 0.00 | [51171, 48829] |
| smart-3, P16 | 5 | 719.4k | 702.5k..853.5k | 7.5% | 516 us | 2,294 us | 100.0% | 2.90 | 0.00 | [36463, 31673, 31864] |
| router-1, P16 | 5 | 520.2k | 477.3k..688.9k | 13.4% | 1,245 us | 3,932 us | 100.0% | 0.99 | 1.42 | [100000] |
| router-2, P16 | 5 | 540.5k | 536.0k..652.2k | 8.0% | 1,311 us | 3,998 us | 100.0% | 1.32 | 1.96 | [51171, 48829] |
| router-3, P16 | 5 | 331.1k | 317.6k..395.7k | 8.1% | 2,228 us | 5,767 us | 100.0% | 1.23 | 1.97 | [36463, 31673, 31864] |
| node-1, P64 | 5 | 719.5k | 670.2k..919.4k | 11.4% | 4,325 us | 8,651 us | 100.0% | 1.00 | 0.00 | [100000] |
| smart-3, P64 | 5 | 1,406.0k | 1,150.4k..1,882.2k | 16.4% | 688 us | 4,063 us | 100.0% | 2.84 | 0.00 | [36463, 31673, 31864] |
| router-3, P64 | 5 | 957.7k | 946.4k..1,224.7k | 10.3% | 3,146 us | 8,913 us | 100.0% | 1.82 | 1.97 | [36463, 31673, 31864] |

Relative to `node-1` with the same pipeline depth (median req/s):

| variant | vs node-1 |
|---|---:|
| smart-2, P1 | 1.74x |
| smart-3, P1 | 3.05x |
| router-1, P1 | 0.92x |
| router-2, P1 | 0.73x |
| router-3, P1 | 0.66x |
| smart-2, P16 | 1.48x |
| smart-3, P16 | 1.78x |
| router-1, P16 | 1.29x |
| router-2, P16 | 1.34x |
| router-3, P16 | 0.82x |
| smart-3, P64 | 1.95x |
| router-3, P64 | 1.33x |

## Adding a node to a running cache (3 -> 4 nodes), GET-only load

| variant | runs | keys moved (reply) | RING ADD duration | hit rate before | lowest hit rate during | hit rate after | req/s before | req/s during | req/s after |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| RING ADD | 5 | - | 0.00 s | 100.0% | 75.8% | 75.9% | 332.6k | 270.2k | 260.6k |
| RING ADD MIGRATE | 5 | scanned=100000 moved=24113 superseded=0 vanished=0 errors=0 | 2.25 s | 100.0% | 100.0% | 100.0% | 333.7k | 89.1k | 260.3k |
