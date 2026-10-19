# Replication benchmark report

Machine: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz, 12 logical CPUs, kernel 6.18.33.2-microsoft-standard-WSL2; primary on CPUs 0-3, replicas/router on 4-5, load generators on 6-11. 8 s measured (+1.5 s warm-up) per run, 50 clients unless stated, commit 39432e0 (dirty).

## Write-path cost by topology and acknowledgement policy

| topology | workload | runs | req/s (median) | min..max | CV | p50 | p95 | p99 | vs single | primary CPU | replica CPU | end lag (records) |
|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| single node (no replication flags) | SET only, P1 | 5 | 449.1k | 442.4k..451.5k | 0.9% | 88 us | 205 us | 287 us | +0% | 3.83 | 0.00 | - |
| single node (no replication flags) | SET only, P16 | 5 | 2,118.2k | 2,036.6k..2,145.3k | 1.8% | 328 us | 704 us | 967 us | +0% | 3.80 | 0.00 | - |
| single node (no replication flags) | 80% GET, P16 | 5 | 2,134.8k | 2,078.2k..2,157.5k | 1.3% | 336 us | 688 us | 868 us | +0% | 3.81 | 0.00 | - |
| replication log only (0 replicas) | SET only, P1 | 5 | 399.0k | 391.5k..408.4k | 1.4% | 102 us | 242 us | 336 us | -11% | 3.87 | 0.00 | - |
| replication log only (0 replicas) | SET only, P16 | 5 | 1,011.8k | 992.3k..1,028.1k | 1.2% | 704 us | 1,442 us | 1,933 us | -52% | 3.29 | 0.00 | - |
| replication log only (0 replicas) | 80% GET, P16 | 5 | 1,774.3k | 1,685.1k..1,781.4k | 2.1% | 393 us | 836 us | 1,114 us | -17% | 3.70 | 0.00 | - |
| async, 1 replica | SET only, P1 | 5 | 237.7k | 235.6k..240.7k | 0.8% | 135 us | 590 us | 1,081 us | -47% | 3.38 | 0.53 | max 0 |
| async, 1 replica | SET only, P16 | 5 | 594.4k | 543.4k..596.9k | 3.7% | 1,212 us | 2,490 us | 3,342 us | -72% | 2.97 | 1.00 | max 0 |
| async, 1 replica | 80% GET, P16 | 5 | 1,005.7k | 803.3k..1,037.1k | 8.7% | 672 us | 1,638 us | 2,294 us | -53% | 3.17 | 0.55 | max 0 |
| async, 2 replicas | SET only, P1 | 5 | 177.2k | 174.4k..181.6k | 1.4% | 184 us | 754 us | 1,278 us | -61% | 3.34 | 0.92 | max 0 |
| async, 2 replicas | SET only, P16 | 5 | 471.4k | 456.7k..483.3k | 1.8% | 1,540 us | 3,080 us | 4,063 us | -78% | 3.08 | 1.80 | max 0 |
| async, 2 replicas | 80% GET, P16 | 5 | 801.2k | 776.8k..822.9k | 2.3% | 885 us | 1,966 us | 2,752 us | -62% | 3.18 | 0.96 | max 0 |
| sync 1, 1 replica | SET only, P1 | 5 | 130.3k | 129.0k..131.0k | 0.6% | 336 us | 721 us | 1,016 us | -71% | 2.84 | 0.46 | max 0 |
| sync 1, 1 replica | SET only, P16 | 5 | 586.7k | 582.0k..587.1k | 0.3% | 1,278 us | 2,294 us | 3,146 us | -72% | 3.04 | 0.98 | max 0 |
| sync 1, 1 replica | 80% GET, P16 | 5 | 991.0k | 868.7k..1,012.0k | 5.5% | 704 us | 1,638 us | 2,228 us | -54% | 3.07 | 0.55 | max 0 |
| sync 1, 2 replicas | SET only, P1 | 5 | 118.8k | 114.3k..122.7k | 2.3% | 352 us | 868 us | 1,311 us | -74% | 3.30 | 0.86 | max 0 |
| sync 1, 2 replicas | SET only, P16 | 5 | 465.3k | 429.0k..468.8k | 3.3% | 1,540 us | 3,408 us | 4,456 us | -78% | 3.14 | 1.74 | max 0 |
| sync 1, 2 replicas | 80% GET, P16 | 5 | 794.3k | 778.4k..804.3k | 1.1% | 868 us | 2,163 us | 3,080 us | -63% | 3.18 | 0.94 | max 0 |
| sync 2, 2 replicas | SET only, P1 | 5 | 99.9k | 93.4k..101.0k | 3.1% | 451 us | 901 us | 1,245 us | -78% | 3.03 | 0.79 | max 0 |
| sync 2, 2 replicas | SET only, P16 | 5 | 464.4k | 462.6k..474.8k | 1.0% | 1,606 us | 3,146 us | 4,129 us | -78% | 3.15 | 1.74 | max 0 |
| sync 2, 2 replicas | 80% GET, P16 | 5 | 778.8k | 739.8k..791.9k | 2.5% | 918 us | 2,032 us | 2,884 us | -64% | 3.15 | 0.94 | max 0 |

## Client-count and pipeline-depth sweeps (SET only)

### Clients (pipeline 1)

| topology | clients=1 | clients=4 | clients=16 | clients=50 | clients=256 |
|---|---:|---:|---:|---:|---:|
| single node (no replication flags) | 18.3k (p99 98 us) | 63.7k (p99 98 us) | 296.5k (p99 115 us) | 447.8k (p99 287 us) | 520.8k (p99 1,180 us) |
| async, 1 replica | 14.0k (p99 115 us) | 52.5k (p99 172 us) | 163.3k (p99 516 us) | 240.1k (p99 1,049 us) | 245.3k (p99 3,670 us) |
| sync 1, 1 replica | 4.9k (p99 320 us) | 21.1k (p99 320 us) | 67.9k (p99 475 us) | 130.0k (p99 1,032 us) | 187.1k (p99 4,194 us) |

### Pipeline depth (50 clients)

| topology | pipeline=1 | pipeline=4 | pipeline=16 | pipeline=64 |
|---|---:|---:|---:|---:|
| single node (no replication flags) | 447.8k (p99 287 us) | 1,192.8k (p99 434 us) | 2,130.8k (p99 967 us) | 2,713.8k (p99 2,949 us) |
| async, 1 replica | 240.1k (p99 1,049 us) | 390.9k (p99 1,737 us) | 593.1k (p99 3,277 us) | 637.0k (p99 11,010 us) |
| sync 1, 1 replica | 130.0k (p99 1,032 us) | 356.4k (p99 1,409 us) | 588.3k (p99 3,211 us) | 615.0k (p99 8,258 us) |

## Replication lag under sustained write load (async, 1 replica)

| pipeline | replica readers | runs | writer req/s | primary-side lag (records): median / p95 / p99 / max | replica-seen lag_ms (p99 / max) | drain after load (s) | replica read req/s | replica read hit rate | runs where the replica was cut off (backlog overflow) |
|---:|---|---:|---:|---|---|---:|---:|---:|---:|
| 1 | no | 5 | 237.4k | 228 / 1,182 / 1,619 / 1,765 | 0 / 0 | 0.00 | - | - | 0 of 5 |
| 1 | yes | 5 | 209.7k | 482 / 1,452 / 1,903 / 2,390 | 0 / 0 | 0.00 | 620.5k | 100.0% | 0 of 5 |
| 16 | no | 5 | 587.7k | 3,237 / 19,514 / 20,023 / 25,523 | 0 / 0 | 0.00 | - | - | 0 of 5 |
| 16 | yes | 5 | 798.7k | 385,414 / 11,563,776 / 13,391,154 / 13,523,451 | 0 / 0 | 0.47 | 645.3k | 100.0% | 5 of 5 |
| 64 | no | 5 | 615.5k | 55,539 / 68,302 / 68,782 / 100,753 | 0 / 0 | 0.06 | - | - | 0 of 5 |
| 64 | yes | 5 | 904.3k | 394,704 / 13,827,596 / 15,120,213 / 15,575,019 | 0 / 0 | 0.10 | 689.9k | 100.0% | 5 of 5 |

## Failover: kill -9 of the primary under write traffic through the router

| configuration | kills | first error after kill (s) | writes possible again (s), median [min..max] | acknowledged writes | acknowledged writes MISSING | old primary rejoined as synced replica (s) |
|---|---:|---:|---|---:|---:|---:|
| default timings (lease 2000, probe 500), async | 5 (5 promoted) | 0.00 | 2.61 [2.58..2.63] | 6548 | **0** | 0.51 |
| default timings (lease 2000, probe 500), sync 1 | 5 (5 promoted) | 0.00 | 2.59 [2.55..2.61] | 6207 | **0** | 0.51 |
| fast timings (lease 500, probe 100), async | 5 (5 promoted) | 0.00 | 0.67 [0.66..0.70] | 6560 | **0** | 0.20 |
| fast timings (lease 500, probe 100), sync 1 | 5 (5 promoted) | 0.00 | 0.65 [0.65..0.67] | 6227 | **0** | 0.17 |

## Resynchronisation

| kind | detail | runs | seconds, median [min..max] | rate |
|---|---|---:|---|---|
| full resync | 100,000 keys (64 B values) | 5 | 0.07 [0.06..0.07] | 1,515,152 keys/s |
| full resync | 500,000 keys (64 B values) | 5 | 0.38 [0.33..0.38] | 1,329,787 keys/s |
| full resync | 1,000,000 keys (64 B values) | 5 | 0.79 [0.77..0.83] | 1,262,626 keys/s |
| link drop under sustained writes (REPLICAOF to the same primary) | partial resyncs [1], full [5], max lag 20,825 records | 5 | 0.05 [0.05..0.15] to ONLINE again | - |
