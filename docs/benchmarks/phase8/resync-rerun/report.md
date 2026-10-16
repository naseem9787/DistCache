# Replication benchmark report

Machine: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz, 12 logical CPUs, kernel 6.18.33.2-microsoft-standard-WSL2; primary on CPUs 0-3, replicas/router on 4-5, load generators on 6-11. 8 s measured (+1.5 s warm-up) per run, 50 clients unless stated, commit c771b7b (dirty).

## Resynchronisation

| kind | detail | runs | seconds, median [min..max] | rate |
|---|---|---:|---|---|
| full resync | 100,000 keys (64 B values) | 5 | 0.08 [0.06..0.09] | 1,190,476 keys/s |
| full resync | 500,000 keys (64 B values) | 5 | 0.37 [0.31..0.41] | 1,344,086 keys/s |
| full resync | 1,000,000 keys (64 B values) | 5 | 0.72 [0.66..0.93] | 1,379,310 keys/s |
| link drop under sustained writes (REPLICAOF to the same primary) | partial resyncs [1], full [0, 5, 6], max lag 0 records | 5 | 0.05 [0.05..0.05] to ONLINE again | - |
