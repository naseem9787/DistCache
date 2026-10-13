| keys | history | log records | log size | log-only recovery (median, min..max) | snapshot size | snapshot recovery (median, min..max) | speedup | SAVE time |
|---:|---|---:|---:|---|---:|---|---:|---:|
| 100,000 | unique | 100,000 | 11 MB | 0.10 s (0.10..0.12) | 11 MB | 0.08 s (0.06..0.14) | 1.3x | 0.09 s |
| 100,000 | churn | 6,825,632 | 771 MB | 7.00 s (6.04..7.84) | 11 MB | 0.08 s (0.08..0.10) | 83.4x | 0.21 s |
| 500,000 | unique | 500,000 | 57 MB | 0.76 s (0.51..0.91) | 57 MB | 0.33 s (0.31..0.33) | 2.3x | 0.49 s |
| 500,000 | churn | 5,711,104 | 645 MB | 8.33 s (6.74..9.66) | 57 MB | 0.47 s (0.37..0.54) | 17.6x | 0.59 s |
| 1,000,000 | unique | 1,000,000 | 113 MB | 1.17 s (1.08..1.25) | 113 MB | 0.64 s (0.59..0.72) | 1.8x | 0.88 s |
| 1,000,000 | churn | 6,309,056 | 713 MB | 12.11 s (10.18..13.03) | 113 MB | 0.96 s (0.95..1.11) | 12.6x | 1.27 s |

Key count identical after every restart: yes (30 recoveries pairs)
