# Replication benchmark report

Machine: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz, 12 logical CPUs, kernel 6.18.33.2-microsoft-standard-WSL2; primary on CPUs 0-3, replicas/router on 4-5, load generators on 6-11. 8 s measured (+1.5 s warm-up) per run, 50 clients unless stated, commit c771b7b (dirty).

## Replication lag under sustained write load (async, 1 replica)

| pipeline | replica readers | runs | writer req/s | primary-side lag (records): median / p95 / p99 / max | replica-seen lag_ms (p99 / max) | drain after load (s) | replica read req/s | replica read hit rate | runs where the replica was cut off (backlog overflow) |
|---:|---|---:|---:|---|---|---:|---:|---:|---:|
| 1 | no | 5 | 259.1k | 325 / 1,362 / 2,020 / 2,663 | 0 / 0 | 0.00 | - | - | 0 of 5 |
| 1 | yes | 5 | 231.1k | 536 / 1,761 / 2,596 / 2,780 | 0 / 0 | 0.00 | 664.1k | 100.0% | 0 of 5 |
| 16 | no | 5 | 635.9k | 485 / 8,878 / 10,768 / 32,475 | 0 / 0 | 0.00 | - | - | 0 of 5 |
| 16 | yes | 5 | 885.8k | 387,191 / 13,056,666 / 14,652,691 / 14,906,702 | 0 / 0 | 0.41 | 686.3k | 100.0% | 5 of 5 |
| 64 | no | 5 | 694.3k | 53,196 / 62,704 / 64,143 / 13,976,257 | 0 / 0 | 0.00 | - | - | 1 of 5 |
| 64 | yes | 5 | 1,015.8k | 403,834 / 14,912,057 / 17,060,261 / 17,256,272 | 0 / 0 | 0.45 | 725.3k | 100.0% | 5 of 5 |
