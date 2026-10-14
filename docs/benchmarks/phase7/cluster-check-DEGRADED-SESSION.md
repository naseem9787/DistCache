# Cluster benchmark report

Environment: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz, 12 logical CPUs, kernel 6.18.33.2-microsoft-standard-WSL2; nodes pinned to CPUs [0, 2, 4, 1] (one each), router on CPUs 6-7, load generator on CPUs 8-11. 8 s measured (+1.5 s warm-up) per run, 8 repeats, commit 12d938b (dirty).

## Scale-out: nodes x topology

| variant | runs | req/s (median) | min..max | CV | p50 | p99 | hit rate | node CPU (cores) | router CPU (cores) | keys per node (last run) |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---|
| node-1, P16 | 8 | 217.9k | 159.1k..386.9k | 31.7% | 2,982 us | 8,913 us | 100.0% | 0.99 | 0.00 | [100000] |
| router-1, P16 | 8 | 303.7k | 116.3k..455.2k | 35.7% | 2,392 us | 6,357 us | 100.0% | 0.96 | 1.44 | [100000] |

Relative to `node-1` with the same pipeline depth (median req/s):

| variant | vs node-1 |
|---|---:|
| router-1, P16 | 1.39x |
