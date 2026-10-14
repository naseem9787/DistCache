# Cluster benchmark, first complete session ("session A")

Same suite, same machine, same binaries and commands as `cluster-report.md` ("session B"), run about
40 minutes earlier. **Two differences, both of which make it less trustworthy than session B:**

* the router-CPU column was wrong (it summed the CPU of threads that were still alive when the
  run ended, and the router's per-client threads were gone by then), so it is omitted here;
* the P64 variants did not exist yet.

The raw files of this session were discarded when the suite was re-run with the corrected CPU
accounting; this table is the report text as printed. It is kept because it shows how far two
runs of the same suite on this machine can differ in absolute terms (session B was 15-20 %
slower across the board; the machine's health probe drifted during the evening) and that the
*ratios* between topologies held.

5 repeats, 8 s measured (+1.5 s warm-up) per run, 50 clients, 80 % GET, 100k keys, preloaded.

| variant | runs | req/s (median) | min..max | CV | p50 | p99 |
|---|---:|---:|---|---:|---:|---:|
| node-1, P1 | 5 | 81.7k | 67.9k..112.5k | 17.5% | 590 us | 1,212 us |
| smart-2, P1 | 5 | 145.5k | 138.6k..146.2k | 2.0% | 303 us | 1,114 us |
| smart-3, P1 | 5 | 238.5k | 198.9k..249.9k | 8.9% | 129 us | 868 us |
| router-1, P1 | 5 | 76.4k | 71.9k..76.8k | 2.8% | 639 us | 1,376 us |
| router-2, P1 | 5 | 62.9k | 60.3k..63.2k | 1.7% | 770 us | 1,540 us |
| router-3, P1 | 5 | 57.4k | 56.9k..57.7k | 0.6% | 885 us | 1,638 us |
| node-1, P16 | 5 | 496.1k | 484.3k..511.7k | 1.9% | 1,573 us | 3,146 us |
| smart-2, P16 | 5 | 780.6k | 749.9k..789.5k | 2.1% | 868 us | 1,966 us |
| smart-3, P16 | 5 | 859.0k | 832.3k..865.7k | 1.4% | 442 us | 1,606 us |
| router-1, P16 | 5 | 683.8k | 428.2k..687.8k | 16.1% | 1,114 us | 2,425 us |
| router-2, P16 | 5 | 651.8k | 582.3k..665.9k | 4.7% | 1,114 us | 2,687 us |
| router-3, P16 | 5 | 399.4k | 371.3k..404.0k | 3.1% | 1,999 us | 4,129 us |

Relative to `node-1` at the same pipeline depth:

| variant | vs node-1 |
|---|---:|
| smart-2, P1 | 1.78x |
| smart-3, P1 | 2.92x |
| router-1, P1 | 0.93x |
| router-2, P1 | 0.77x |
| router-3, P1 | 0.70x |
| smart-2, P16 | 1.57x |
| smart-3, P16 | 1.73x |
| router-1, P16 | 1.38x |
| router-2, P16 | 1.31x |
| router-3, P16 | 0.81x |

Rebalance (3 -> 4 nodes, GET-only, 5 repeats):

| variant | keys moved (reply) | RING ADD duration | hit rate before / lowest during / after | req/s before / during / after |
|---|---|---:|---|---|
| RING ADD | - | 0.00 s | 100.0% / 75.9% / 75.9% | 405.3k / 328.0k / 327.9k |
| RING ADD MIGRATE | scanned=100000 moved=24113 errors=0 | 1.80 s | 100.0% / 100.0% / 100.0% | 407.9k / 127.9k / 325.2k |
