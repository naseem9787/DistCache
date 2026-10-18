# Rate limiter benchmark report

Machine: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz, 12 logical CPUs, kernel 6.18.33.2-microsoft-standard-WSL2. Nodes pinned to one physical core each (CPUs 0-1, 2-3, 4-5; the 4-thread single node uses 0-3), router on 6-7, load generators on 8-11. 8 s measured (+1.5 s warm-up) per run, 50 clients unless stated, commit 43b4490 (dirty).

## Throughput and latency by topology and workload

Workloads: **allow** = many keys, always allowed (every call writes); **deny** = many keys, almost always denied (no write); **hot** = one key, capacity 100, 1000 tokens/s, saturated. The `denied` column is the measured share of calls refused: with 100,000 keys and capacity 5, **deny at pipeline 1 is only partly denied** (few requests per key), at pipeline 16 it is >90 % denied.

| workload | pipeline | topology | runs | req/s median (min..max) | CV | p50 | p95 | p99 | vs single (2 thr) | denied | server CPU | safety bound held |
|---|---:|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| allow | 1 | single (2 thr, 1 core) | 5 | 132.5k (126.6k..140.0k) | 3.9% | 344 us | 606 us | 901 us | +0% | 0% | 2.0 | 5/5 |
| allow | 1 | single (4 thr, 2 cores) | 5 | 296.2k (295.4k..301.9k) | 0.8% | 148 us | 311 us | 426 us | +124% | 0% | 3.9 | 5/5 |
| allow | 1 | cluster-3 smart client | 5 | 408.3k (401.8k..426.7k) | 2.3% | 80 us | 209 us | 410 us | +208% | 0% | 4.9 | 5/5 |
| allow | 1 | cluster-3 router | 5 | 48.8k (45.5k..50.4k) | 3.5% | 1,016 us | 1,573 us | 2,228 us | -63% | 0% | 0.9 | 5/5 |
| allow | 1 | async 1 replica | 5 | 85.3k (64.8k..94.1k) | 12.4% | 467 us | 1,114 us | 1,999 us | -36% | 0% | 2.2 | 5/5 |
| allow | 1 | sync 1, 1 replica | 5 | 68.6k (56.5k..70.9k) | 8.0% | 655 us | 1,311 us | 1,770 us | -48% | 0% | 1.9 | 5/5 |
| allow | 16 | single (2 thr, 1 core) | 5 | 475.9k (463.2k..488.1k) | 2.2% | 1,540 us | 2,556 us | 3,473 us | +0% | 0% | 2.0 | 5/5 |
| allow | 16 | single (4 thr, 2 cores) | 5 | 828.9k (733.1k..842.7k) | 5.0% | 852 us | 1,704 us | 2,228 us | +74% | 0% | 3.7 | 5/5 |
| allow | 16 | cluster-3 smart client | 5 | 924.1k (883.9k..968.5k) | 3.3% | 426 us | 1,180 us | 1,704 us | +94% | 0% | 5.7 | 5/5 |
| allow | 16 | cluster-3 router | 5 | 333.8k (325.0k..356.4k) | 3.4% | 2,359 us | 3,932 us | 5,636 us | -30% | 0% | 2.1 | 5/5 |
| allow | 16 | async 1 replica | 5 | 226.7k (208.5k..248.0k) | 6.3% | 2,949 us | 7,340 us | 10,224 us | -52% | 0% | 2.5 | 5/5 |
| allow | 16 | sync 1, 1 replica | 5 | 259.5k (240.0k..270.4k) | 4.4% | 2,752 us | 5,898 us | 8,126 us | -45% | 0% | 2.5 | 5/5 |
| deny | 1 | single (2 thr, 1 core) | 5 | 153.2k (141.0k..161.6k) | 4.5% | 303 us | 541 us | 721 us | +0% | 76% | 2.0 | 5/5 |
| deny | 1 | single (4 thr, 2 cores) | 5 | 421.7k (418.9k..426.1k) | 0.7% | 96 us | 221 us | 303 us | +175% | 97% | 3.9 | 5/5 |
| deny | 1 | cluster-3 smart client | 5 | 395.8k (368.5k..401.3k) | 3.1% | 76 us | 168 us | 287 us | +158% | 99% | 4.1 | 5/5 |
| deny | 1 | cluster-3 router | 5 | 50.6k (44.4k..51.1k) | 5.2% | 999 us | 1,475 us | 1,900 us | -67% | 19% | 1.0 | 5/5 |
| deny | 1 | async 1 replica | 5 | 101.1k (86.2k..114.7k) | 10.7% | 393 us | 934 us | 1,704 us | -34% | 56% | 2.3 | 5/5 |
| deny | 1 | sync 1, 1 replica | 5 | 63.2k (56.8k..71.7k) | 8.0% | 672 us | 1,737 us | 3,604 us | -59% | 30% | 1.9 | 5/5 |
| deny | 16 | single (2 thr, 1 core) | 5 | 838.7k (582.3k..907.3k) | 14.5% | 868 us | 1,376 us | 1,999 us | +0% | 100% | 2.0 | 5/5 |
| deny | 16 | single (4 thr, 2 cores) | 5 | 1,724.1k (681.9k..1,752.9k) | 27.5% | 418 us | 803 us | 1,049 us | +106% | 100% | 3.9 | 5/5 |
| deny | 16 | cluster-3 smart client | 5 | 1,825.9k (1,701.0k..1,894.5k) | 3.8% | 221 us | 590 us | 852 us | +118% | 100% | 5.7 | 5/5 |
| deny | 16 | cluster-3 router | 5 | 328.0k (242.7k..347.5k) | 12.6% | 2,359 us | 3,801 us | 5,243 us | -61% | 97% | 1.5 | 5/5 |
| deny | 16 | async 1 replica | 5 | 697.3k (661.2k..726.8k) | 3.0% | 967 us | 2,163 us | 3,342 us | -17% | 96% | 2.1 | 5/5 |
| deny | 16 | sync 1, 1 replica | 5 | 638.8k (582.9k..687.4k) | 5.7% | 1,049 us | 2,359 us | 3,932 us | -24% | 96% | 2.1 | 5/5 |
| hot | 1 | single (2 thr, 1 core) | 5 | 179.9k (171.8k..185.2k) | 2.4% | 254 us | 467 us | 623 us | +0% | 99% | 2.0 | 5/5 |
| hot | 1 | single (4 thr, 2 cores) | 5 | 437.2k (328.2k..451.8k) | 11.0% | 84 us | 213 us | 303 us | +143% | 100% | 3.6 | 5/5 |
| hot | 1 | cluster-3 smart client | 5 | 180.3k (178.9k..188.4k) | 1.9% | 250 us | 467 us | 623 us | +0% | 99% | 2.0 | 5/5 |
| hot | 1 | cluster-3 router | 5 | 56.2k (53.0k..59.0k) | 3.6% | 852 us | 1,409 us | 1,933 us | -69% | 98% | 0.7 | 5/5 |
| hot | 1 | async 1 replica | 5 | 176.2k (169.8k..190.1k) | 3.7% | 258 us | 483 us | 623 us | -2% | 99% | 2.0 | 5/5 |
| hot | 1 | sync 1, 1 replica | 5 | 142.8k (137.3k..145.5k) | 1.9% | 320 us | 590 us | 786 us | -21% | 99% | 2.0 | 5/5 |
| hot | 16 | single (2 thr, 1 core) | 5 | 1,046.6k (994.0k..1,160.6k) | 5.9% | 737 us | 1,081 us | 1,573 us | +0% | 100% | 1.8 | 5/5 |
| hot | 16 | single (4 thr, 2 cores) | 5 | 1,182.3k (1,172.7k..1,264.6k) | 2.9% | 623 us | 1,212 us | 1,475 us | +13% | 100% | 3.0 | 5/5 |
| hot | 16 | cluster-3 smart client | 5 | 1,005.8k (970.7k..1,121.7k) | 5.2% | 737 us | 1,147 us | 1,671 us | -4% | 100% | 1.8 | 5/5 |
| hot | 16 | cluster-3 router | 5 | 702.0k (692.3k..720.9k) | 1.7% | 1,114 us | 1,802 us | 2,359 us | -33% | 100% | 0.9 | 5/5 |
| hot | 16 | async 1 replica | 5 | 1,001.8k (931.5k..1,075.6k) | 4.8% | 754 us | 1,245 us | 1,704 us | -4% | 100% | 1.9 | 5/5 |
| hot | 16 | sync 1, 1 replica | 5 | 898.0k (843.2k..918.1k) | 2.8% | 819 us | 1,475 us | 1,868 us | -14% | 100% | 1.9 | 5/5 |

### Accuracy of the hot key

One bucket, capacity 100, refill 1000/s, hammered by 50 clients. `granted / bound` where bound = capacity + rate x (warm-up + duration + 0.25 s): **it must be <= 1 (safety) and should be close to 1 (the limiter is not merely refusing everything).**

`fluid` = granted / (capacity + rate x total time) with no slack; a value a hair above 1 only means the bucket started a few ms before the load generator's own clock did.

| topology | pipeline | runs | granted / bound: min..max | granted / fluid limit: min..max | any run over the bound |
|---|---:|---:|---|---|---|
| single (2 thr, 1 core) | 1 | 5 | 0.9745..0.9746 | 0.9999..1.0000 | no |
| single (2 thr, 1 core) | 16 | 5 | 0.9744..0.9746 | 0.9998..1.0000 | no |
| single (4 thr, 2 cores) | 1 | 5 | 0.9743..0.9746 | 0.9997..1.0000 | no |
| single (4 thr, 2 cores) | 16 | 5 | 0.9744..0.9746 | 0.9998..1.0000 | no |
| cluster-3 smart client | 1 | 5 | 0.9708..0.9736 | 0.9960..0.9990 | no |
| cluster-3 smart client | 16 | 5 | 0.9701..0.9731 | 0.9953..0.9984 | no |
| cluster-3 router | 1 | 5 | 0.9744..0.9745 | 0.9998..0.9999 | no |
| cluster-3 router | 16 | 5 | 0.9744..0.9745 | 0.9998..0.9999 | no |
| async 1 replica | 1 | 5 | 0.9745..0.9746 | 0.9999..1.0000 | no |
| async 1 replica | 16 | 5 | 0.9745..0.9746 | 0.9999..1.0000 | no |
| sync 1, 1 replica | 1 | 5 | 0.9742..0.9746 | 0.9996..1.0000 | no |
| sync 1, 1 replica | 16 | 5 | 0.9744..0.9746 | 0.9998..1.0000 | no |

## Client-count sweep (pipeline 1)

### workload: allow

| topology | 1 clients | 4 clients | 16 clients | 50 clients | 256 clients |
|---|---:|---:|---:|---:|---:|
| single (2 thr, 1 core) | 17.8k (p99 125 us) | 80.2k (p99 90 us) | 116.3k (p99 311 us) | 130.2k (p99 885 us) | 123.4k (p99 3,211 us) |
| cluster-3 smart client | 17.2k (p99 106 us) | 62.6k (p99 98 us) | 237.9k (p99 164 us) | 409.2k (p99 410 us) | 432.6k (p99 2,294 us) |
| cluster-3 router | 8.5k (p99 192 us) | 38.1k (p99 205 us) | 50.6k (p99 672 us) | 49.9k (p99 1,966 us) | 40.4k (p99 12,321 us) |

### workload: hot

| topology | 1 clients | 4 clients | 16 clients | 50 clients | 256 clients |
|---|---:|---:|---:|---:|---:|
| single (2 thr, 1 core) | 18.7k (p99 115 us) | 90.8k (p99 82 us) | 152.2k (p99 238 us) | 183.1k (p99 590 us) | 221.2k (p99 2,097 us) |
| cluster-3 smart client | 18.6k (p99 115 us) | 88.1k (p99 90 us) | 149.0k (p99 246 us) | 182.8k (p99 623 us) | 203.3k (p99 2,359 us) |
| cluster-3 router | 9.4k (p99 192 us) | 40.3k (p99 172 us) | 59.8k (p99 541 us) | 57.4k (p99 1,868 us) | 46.8k (p99 10,748 us) |

## Over-admission across a failover (kill -9 of the primary after 40 % of the tokens are granted)

Bucket capacity 200,000, refill 0.001/s (none that matters), 8 clients through the router, shard = 1 primary + 2 replicas, lease 500 ms / probe 100 ms. `told allowed` counts only answers the clients actually received.

| policy | kills | told allowed: each run | over-admission (told allowed - capacity): each run | bucket drained |
|---|---:|---|---|---|
| async | 5 (5 killed) | 200,001, 200,000, 199,999, 200,000, 200,000 | +1, +0, -1, +0, +0 | 5/5 |
| sync 1 | 5 (5 killed) | 200,000, 199,997, 199,996, 199,993, 199,998 | +0, -3, -4, -7, -2 | 5/5 |
