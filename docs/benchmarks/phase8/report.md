# Replication benchmark report

Machine: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz, 12 logical CPUs, kernel 6.18.33.2-microsoft-standard-WSL2; primary on CPUs 0-3, replicas/router on 4-5, load generators on 6-11. 8 s measured (+1.5 s warm-up) per run, 50 clients unless stated, commit c771b7b (dirty).

## Write-path cost by topology and acknowledgement policy

| topology | workload | runs | req/s (median) | min..max | CV | p50 | p95 | p99 | vs single | primary CPU | replica CPU | end lag (records) |
|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| single node (no replication flags) | SET only, P1 | 5 | 458.7k | 447.8k..466.8k | 1.4% | 88 us | 197 us | 262 us | +0% | 3.83 | 0.00 | - |
| single node (no replication flags) | SET only, P16 | 5 | 2,331.6k | 2,314.3k..2,370.3k | 0.8% | 311 us | 623 us | 754 us | +0% | 3.83 | 0.00 | - |
| single node (no replication flags) | 80% GET, P16 | 5 | 2,275.6k | 2,172.6k..2,298.5k | 2.0% | 320 us | 623 us | 770 us | +0% | 3.82 | 0.00 | - |
| replication log only (0 replicas) | SET only, P1 | 5 | 416.7k | 414.4k..431.5k | 1.5% | 100 us | 225 us | 303 us | -9% | 3.87 | 0.00 | - |
| replication log only (0 replicas) | SET only, P16 | 5 | 1,115.5k | 1,110.0k..1,124.5k | 0.5% | 655 us | 1,278 us | 1,540 us | -52% | 3.33 | 0.00 | - |
| replication log only (0 replicas) | 80% GET, P16 | 5 | 1,929.1k | 1,895.6k..1,937.6k | 0.8% | 377 us | 737 us | 901 us | -15% | 3.71 | 0.00 | - |
| async, 1 replica | SET only, P1 | 5 | 264.5k | 261.0k..266.2k | 0.7% | 125 us | 508 us | 950 us | -42% | 3.41 | 0.51 | max 0 |
| async, 1 replica | SET only, P16 | 5 | 653.5k | 642.2k..916.5k | 15.1% | 1,147 us | 2,163 us | 2,818 us | -72% | 3.01 | 0.98 | max 0 |
| async, 1 replica | 80% GET, P16 | 5 | 1,115.6k | 1,072.0k..1,141.5k | 2.1% | 623 us | 1,409 us | 1,966 us | -51% | 3.19 | 0.54 | max 0 |
| async, 2 replicas | SET only, P1 | 5 | 194.5k | 179.4k..199.2k | 4.3% | 168 us | 688 us | 1,147 us | -58% | 3.37 | 0.90 | max 0 |
| async, 2 replicas | SET only, P16 | 5 | 508.4k | 500.1k..512.9k | 0.9% | 1,475 us | 2,818 us | 3,539 us | -78% | 3.09 | 1.76 | max 0 |
| async, 2 replicas | 80% GET, P16 | 5 | 870.9k | 843.2k..888.2k | 1.7% | 819 us | 1,770 us | 2,425 us | -62% | 3.20 | 0.94 | max 0 |
| sync 1, 1 replica | SET only, P1 | 5 | 137.7k | 133.2k..149.9k | 4.1% | 328 us | 655 us | 901 us | -70% | 2.85 | 0.45 | max 0 |
| sync 1, 1 replica | SET only, P16 | 5 | 644.3k | 633.0k..647.9k | 0.8% | 1,180 us | 1,966 us | 2,490 us | -72% | 3.07 | 0.97 | max 0 |
| sync 1, 1 replica | 80% GET, P16 | 5 | 1,081.4k | 1,062.9k..1,093.1k | 1.2% | 655 us | 1,475 us | 1,966 us | -52% | 3.10 | 0.55 | max 0 |
| sync 1, 2 replicas | SET only, P1 | 5 | 125.0k | 123.9k..125.5k | 0.5% | 344 us | 819 us | 1,212 us | -73% | 3.33 | 0.83 | max 0 |
| sync 1, 2 replicas | SET only, P16 | 5 | 506.3k | 502.6k..516.1k | 0.9% | 1,409 us | 3,015 us | 3,801 us | -78% | 3.17 | 1.70 | max 0 |
| sync 1, 2 replicas | 80% GET, P16 | 5 | 881.2k | 871.5k..898.7k | 1.2% | 803 us | 1,868 us | 2,490 us | -61% | 3.21 | 0.92 | max 0 |
| sync 2, 2 replicas | SET only, P1 | 5 | 106.3k | 104.9k..109.2k | 1.4% | 434 us | 819 us | 1,081 us | -77% | 3.07 | 0.76 | max 0 |
| sync 2, 2 replicas | SET only, P16 | 5 | 506.0k | 493.6k..515.4k | 1.4% | 1,475 us | 2,818 us | 3,473 us | -78% | 3.17 | 1.71 | max 0 |
| sync 2, 2 replicas | 80% GET, P16 | 5 | 855.3k | 834.1k..860.7k | 1.1% | 852 us | 1,770 us | 2,359 us | -62% | 3.17 | 0.91 | max 0 |

## Client-count and pipeline-depth sweeps (SET only)

### Clients (pipeline 1)

| topology | clients=1 | clients=4 | clients=16 | clients=50 | clients=256 |
|---|---:|---:|---:|---:|---:|
| single node (no replication flags) | 19.2k (p99 98 us) | 65.4k (p99 80 us) | 313.6k (p99 104 us) | 465.9k (p99 258 us) | 547.1k (p99 1,032 us) |
| async, 1 replica | 14.4k (p99 96 us) | 55.3k (p99 152 us) | 178.4k (p99 492 us) | 260.4k (p99 967 us) | 263.5k (p99 3,408 us) |
| sync 1, 1 replica | 5.0k (p99 287 us) | 22.0k (p99 278 us) | 69.8k (p99 426 us) | 139.7k (p99 868 us) | 197.0k (p99 3,932 us) |

### Pipeline depth (50 clients)

| topology | pipeline=1 | pipeline=4 | pipeline=16 | pipeline=64 |
|---|---:|---:|---:|---:|
| single node (no replication flags) | 465.9k (p99 258 us) | 1,260.4k (p99 369 us) | 2,339.0k (p99 754 us) | 3,174.7k (p99 2,163 us) |
| async, 1 replica | 260.4k (p99 967 us) | 423.9k (p99 1,573 us) | 655.7k (p99 2,752 us) | 706.7k (p99 9,437 us) |
| sync 1, 1 replica | 139.7k (p99 868 us) | 379.6k (p99 1,245 us) | 642.2k (p99 2,490 us) | 704.2k (p99 6,422 us) |

## Replication lag under sustained write load (async, 1 replica)

| pipeline | replica readers | runs | writer req/s | primary-side lag (records): median / p95 / p99 / max | replica-seen lag_ms (p99 / max) | drain after load (s) | replica read req/s | replica read hit rate |
|---:|---|---:|---:|---|---|---:|---:|---:|
| 1 | no | 5 | 259.1k | 229 / 1,023 / 2,053 / 2,271 | 0 / 0 | 0.00 | - | - |
| 1 | yes | 5 | 231.4k | 575 / 1,524 / 2,188 / 3,761 | 0 / 0 | 0.00 | 672.6k | 100.0% |
| 16 | no | 5 | 646.0k | 810 / 11,772 / 13,033 / 14,299,229 | 0 / 0 | 0.00 | - | - |
| 16 | yes | 5 | 887.8k | 390,868 / 13,044,162 / 14,995,642 / 15,058,206 | 0 / 0 | 0.37 | 684.0k | 100.0% |
| 64 | no | 5 | 714.3k | 45,923 / 53,045 / 57,273 / 16,968,405 | 0 / 0 | 0.04 | - | - |
| 64 | yes | 5 | 1,010.0k | 400,450 / 14,449,676 / 16,771,917 / 17,343,015 | 0 / 0 | 0.36 | 730.4k | 100.0% |

## Failover: kill -9 of the primary under write traffic through the router

| configuration | kills | first error after kill (s) | writes possible again (s), median [min..max] | acknowledged writes | acknowledged writes MISSING | old primary rejoined as synced replica (s) |
|---|---:|---:|---|---:|---:|---:|
| default timings (lease 2000, probe 500), async | 5 (5 promoted) | 0.00 | 2.62 [2.61..2.64] | 6602 | **0** | 0.51 |
| default timings (lease 2000, probe 500), sync 1 | 5 (5 promoted) | 0.00 | 2.59 [2.58..2.61] | 6260 | **0** | 0.51 |
| fast timings (lease 500, probe 100), async | 5 (5 promoted) | 0.00 | 0.69 [0.66..0.71] | 6605 | **0** | 0.17 |
| fast timings (lease 500, probe 100), sync 1 | 5 (5 promoted) | 0.00 | 0.67 [0.65..0.69] | 6261 | **0** | 0.17 |

## Resynchronisation

| kind | detail | runs | seconds, median [min..max] | rate |
|---|---|---:|---|---|
| full resync | 100,000 keys (64 B values) | 5 | 0.06 [0.06..0.08] | 1,562,500 keys/s |
| full resync | 500,000 keys (64 B values) | 5 | 0.33 [0.32..0.35] | 1,497,006 keys/s |
| full resync | 1,000,000 keys (64 B values) | 5 | 0.69 [0.66..0.75] | 1,459,854 keys/s |
| link drop under sustained writes (REPLICAOF to the same primary) | partial resyncs [1], full [0, 5], max lag 0 records | 5 | 0.05 [0.05..0.05] to ONLINE again | - |
