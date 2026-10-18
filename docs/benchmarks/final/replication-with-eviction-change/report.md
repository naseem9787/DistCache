# Replication benchmark report

Machine: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz, 12 logical CPUs, kernel 6.18.33.2-microsoft-standard-WSL2; primary on CPUs 0-3, replicas/router on 4-5, load generators on 6-11. 8 s measured (+1.5 s warm-up) per run, 50 clients unless stated, commit dc6ef46 (dirty).

## Write-path cost by topology and acknowledgement policy

| topology | workload | runs | req/s (median) | min..max | CV | p50 | p95 | p99 | vs single | primary CPU | replica CPU | end lag (records) |
|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| single node (no replication flags) | SET only, P1 | 5 | 445.8k | 440.3k..451.9k | 1.0% | 88 us | 205 us | 287 us | +0% | 3.85 | 0.00 | - |
| single node (no replication flags) | SET only, P16 | 5 | 2,138.7k | 2,125.5k..2,152.1k | 0.4% | 328 us | 672 us | 868 us | +0% | 3.82 | 0.00 | - |
| single node (no replication flags) | 80% GET, P16 | 5 | 2,102.2k | 2,089.7k..2,107.8k | 0.3% | 344 us | 688 us | 868 us | +0% | 3.80 | 0.00 | - |
| replication log only (0 replicas) | SET only, P1 | 5 | 399.9k | 390.0k..410.1k | 1.8% | 102 us | 238 us | 328 us | -10% | 3.87 | 0.00 | - |
| replication log only (0 replicas) | SET only, P16 | 5 | 1,173.0k | 1,168.4k..1,180.5k | 0.4% | 623 us | 1,212 us | 1,507 us | -45% | 3.47 | 0.00 | - |
| replication log only (0 replicas) | 80% GET, P16 | 5 | 1,822.4k | 1,782.3k..1,832.6k | 1.1% | 393 us | 786 us | 999 us | -13% | 3.72 | 0.00 | - |
| async, 1 replica | SET only, P1 | 5 | 251.7k | 251.6k..252.7k | 0.2% | 129 us | 541 us | 1,016 us | -44% | 3.42 | 0.53 | max 0 |
| async, 1 replica | SET only, P16 | 5 | 612.8k | 605.9k..617.5k | 0.6% | 1,212 us | 2,359 us | 3,080 us | -71% | 2.98 | 1.00 | max 0 |
| async, 1 replica | 80% GET, P16 | 5 | 1,042.0k | 1,027.7k..1,049.4k | 0.7% | 655 us | 1,540 us | 2,163 us | -50% | 3.19 | 0.55 | max 0 |
| async, 2 replicas | SET only, P1 | 5 | 189.0k | 180.7k..189.5k | 1.8% | 168 us | 721 us | 1,212 us | -58% | 3.38 | 0.92 | max 0 |
| async, 2 replicas | SET only, P16 | 5 | 484.0k | 469.0k..494.7k | 1.7% | 1,507 us | 2,949 us | 3,801 us | -77% | 3.10 | 1.84 | max 0 |
| async, 2 replicas | 80% GET, P16 | 5 | 808.7k | 772.8k..816.3k | 1.9% | 885 us | 1,933 us | 2,621 us | -62% | 3.19 | 0.95 | max 0 |
| sync 1, 1 replica | SET only, P1 | 5 | 132.2k | 132.1k..133.5k | 0.4% | 336 us | 688 us | 950 us | -70% | 2.84 | 0.46 | max 0 |
| sync 1, 1 replica | SET only, P16 | 5 | 599.5k | 592.7k..607.0k | 0.8% | 1,278 us | 2,097 us | 2,621 us | -72% | 3.09 | 0.98 | max 0 |
| sync 1, 1 replica | 80% GET, P16 | 5 | 992.3k | 986.4k..998.3k | 0.4% | 721 us | 1,638 us | 2,228 us | -53% | 3.08 | 0.56 | max 0 |
| sync 1, 2 replicas | SET only, P1 | 5 | 118.5k | 116.2k..119.7k | 1.1% | 360 us | 868 us | 1,278 us | -73% | 3.32 | 0.85 | max 0 |
| sync 1, 2 replicas | SET only, P16 | 5 | 478.7k | 473.2k..479.6k | 0.5% | 1,540 us | 3,146 us | 3,998 us | -78% | 3.19 | 1.75 | max 0 |
| sync 1, 2 replicas | 80% GET, P16 | 5 | 820.2k | 804.2k..833.8k | 1.3% | 852 us | 2,032 us | 2,752 us | -61% | 3.19 | 0.95 | max 0 |
| sync 2, 2 replicas | SET only, P1 | 5 | 97.8k | 92.9k..100.9k | 3.1% | 467 us | 918 us | 1,245 us | -78% | 3.04 | 0.78 | max 0 |
| sync 2, 2 replicas | SET only, P16 | 5 | 472.2k | 462.2k..475.7k | 1.0% | 1,606 us | 3,015 us | 3,736 us | -78% | 3.18 | 1.73 | max 0 |
| sync 2, 2 replicas | 80% GET, P16 | 5 | 804.8k | 781.7k..808.9k | 1.2% | 901 us | 1,933 us | 2,556 us | -62% | 3.16 | 0.93 | max 0 |

## Client-count and pipeline-depth sweeps (SET only)

### Clients (pipeline 1)

| topology | clients=1 | clients=4 | clients=16 | clients=50 | clients=256 |
|---|---:|---:|---:|---:|---:|
| single node (no replication flags) | 18.3k (p99 123 us) | 63.5k (p99 100 us) | 294.3k (p99 121 us) | 455.1k (p99 278 us) | 530.2k (p99 1,114 us) |
| async, 1 replica | 14.0k (p99 113 us) | 53.7k (p99 160 us) | 170.2k (p99 500 us) | 251.8k (p99 1,016 us) | 252.3k (p99 3,408 us) |
| sync 1, 1 replica | 5.0k (p99 328 us) | 21.3k (p99 311 us) | 68.3k (p99 459 us) | 132.8k (p99 950 us) | 184.5k (p99 4,129 us) |

### Pipeline depth (50 clients)

| topology | pipeline=1 | pipeline=4 | pipeline=16 | pipeline=64 |
|---|---:|---:|---:|---:|
| single node (no replication flags) | 455.1k (p99 278 us) | 1,209.3k (p99 410 us) | 2,148.1k (p99 868 us) | 2,828.4k (p99 2,556 us) |
| async, 1 replica | 251.8k (p99 1,016 us) | 431.5k (p99 1,606 us) | 614.3k (p99 3,080 us) | 651.5k (p99 10,486 us) |
| sync 1, 1 replica | 132.8k (p99 950 us) | 354.9k (p99 1,409 us) | 597.5k (p99 2,621 us) | 616.2k (p99 6,816 us) |

## Replication lag under sustained write load (async, 1 replica)

| pipeline | replica readers | runs | writer req/s | primary-side lag (records): median / p95 / p99 / max | replica-seen lag_ms (p99 / max) | drain after load (s) | replica read req/s | replica read hit rate | runs where the replica was cut off (backlog overflow) |
|---:|---|---:|---:|---|---|---:|---:|---:|---:|
| 1 | no | 5 | 255.7k | 255 / 1,359 / 1,751 / 2,153 | 0 / 0 | 0.00 | - | - | 0 of 5 |
| 1 | yes | 5 | 223.6k | 756 / 1,812 / 2,288 / 2,815 | 0 / 0 | 0.00 | 615.3k | 100.0% | 0 of 5 |
| 16 | no | 5 | 607.7k | 41,238 / 45,506 / 48,042 / 48,508 | 0 / 0 | 0.06 | - | - | 0 of 5 |
| 16 | yes | 5 | 943.0k | 416,071 / 13,591,887 / 15,495,418 / 16,021,796 | 0 / 0 | 0.66 | 666.7k | 100.0% | 5 of 5 |
| 64 | no | 5 | 646.5k | 50,744 / 86,725 / 93,019 / 17,113,227 | 0 / 0 | 0.04 | - | - | 2 of 5 |
| 64 | yes | 5 | 1,090.9k | 410,890 / 16,373,743 / 18,177,412 / 18,403,606 | 0 / 0 | 0.29 | 704.0k | 100.0% | 5 of 5 |

## Failover: kill -9 of the primary under write traffic through the router

| configuration | kills | first error after kill (s) | writes possible again (s), median [min..max] | acknowledged writes | acknowledged writes MISSING | old primary rejoined as synced replica (s) |
|---|---:|---:|---|---:|---:|---:|
| default timings (lease 2000, probe 500), async | 5 (5 promoted) | 0.00 | 2.62 [2.58..2.62] | 6537 | **0** | 0.51 |
| default timings (lease 2000, probe 500), sync 1 | 5 (5 promoted) | 0.00 | 2.57 [2.54..2.58] | 6214 | **0** | 0.51 |
| fast timings (lease 500, probe 100), async | 5 (5 promoted) | 0.00 | 0.68 [0.68..0.71] | 6517 | **0** | 0.21 |
| fast timings (lease 500, probe 100), sync 1 | 5 (5 promoted) | 0.00 | 0.65 [0.64..0.66] | 6187 | **0** | 0.18 |

## Resynchronisation

| kind | detail | runs | seconds, median [min..max] | rate |
|---|---|---:|---|---|
| full resync | 100,000 keys (64 B values) | 5 | 0.08 [0.06..0.09] | 1,190,476 keys/s |
| full resync | 500,000 keys (64 B values) | 5 | 0.36 [0.33..0.39] | 1,400,560 keys/s |
| full resync | 1,000,000 keys (64 B values) | 5 | 0.88 [0.81..0.94] | 1,140,251 keys/s |
| link drop under sustained writes (REPLICAOF to the same primary) | partial resyncs [1], full [8], max lag 41,193 records | 5 | 0.10 [0.10..0.15] to ONLINE again | - |
