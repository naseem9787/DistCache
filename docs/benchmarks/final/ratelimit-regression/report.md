# Rate limiter benchmark report

Machine: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz, 12 logical CPUs, kernel 6.18.33.2-microsoft-standard-WSL2. Nodes pinned to one physical core each (CPUs 0-1, 2-3, 4-5; the 4-thread single node uses 0-3), router on 6-7, load generators on 8-11. 8 s measured (+1.5 s warm-up) per run, 50 clients unless stated, commit 39432e0 (dirty).

## Throughput and latency by topology and workload

Workloads: **allow** = many keys, always allowed (every call writes); **deny** = many keys, almost always denied (no write); **hot** = one key, capacity 100, 1000 tokens/s, saturated. The `denied` column is the measured share of calls refused: with 100,000 keys and capacity 5, **deny at pipeline 1 is only partly denied** (few requests per key), at pipeline 16 it is >90 % denied.

| workload | pipeline | topology | runs | req/s median (min..max) | CV | p50 | p95 | p99 | vs single (2 thr) | denied | server CPU | safety bound held |
|---|---:|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| allow | 1 | single (2 thr, 1 core) | 3 | 137.5k (132.9k..140.4k) | 2.3% | 328 us | 623 us | 885 us | +0% | 0% | 2.0 | 3/3 |
| allow | 16 | single (2 thr, 1 core) | 3 | 462.6k (458.1k..466.9k) | 0.8% | 1,573 us | 2,949 us | 3,670 us | +0% | 0% | 1.9 | 3/3 |
| deny | 1 | single (2 thr, 1 core) | 3 | 159.4k (157.8k..163.2k) | 1.4% | 287 us | 524 us | 721 us | +0% | 78% | 2.0 | 3/3 |
| deny | 16 | single (2 thr, 1 core) | 3 | 828.4k (820.4k..887.5k) | 3.5% | 885 us | 1,638 us | 2,032 us | +0% | 100% | 2.0 | 3/3 |
| hot | 1 | single (2 thr, 1 core) | 3 | 188.5k (183.4k..195.9k) | 2.7% | 238 us | 451 us | 606 us | +0% | 99% | 2.0 | 3/3 |
| hot | 16 | single (2 thr, 1 core) | 3 | 1,019.0k (973.4k..1,119.1k) | 5.9% | 737 us | 1,212 us | 1,638 us | +0% | 100% | 1.8 | 3/3 |

### Accuracy of the hot key

One bucket, capacity 100, refill 1000/s, hammered by 50 clients. `granted / bound` where bound = capacity + rate x (warm-up + duration + 0.25 s): **it must be <= 1 (safety) and should be close to 1 (the limiter is not merely refusing everything).**

`fluid` = granted / (capacity + rate x total time) with no slack; a value a hair above 1 only means the bucket started a few ms before the load generator's own clock did.

| topology | pipeline | runs | granted / bound: min..max | granted / fluid limit: min..max | any run over the bound |
|---|---:|---:|---|---|---|
| single (2 thr, 1 core) | 1 | 3 | 0.9745..0.9746 | 0.9999..1.0000 | no |
| single (2 thr, 1 core) | 16 | 3 | 0.9745..0.9746 | 0.9999..1.0000 | no |
