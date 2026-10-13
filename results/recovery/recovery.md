| keys | history | log records | log size | log-only recovery (median, min..max) | snapshot size | snapshot recovery (median, min..max) | speedup | SAVE time |
|---:|---|---:|---:|---|---:|---|---:|---:|
| 100,000 | unique | 100,000 | 11 MB | 0.17 s (0.13..0.31) | 11 MB | 0.10 s (0.08..0.13) | 1.6x | 0.22 s |
| 100,000 | churn | 4,591,328 | 519 MB | 7.56 s (5.11..11.53) | 11 MB | 0.11 s (0.08..0.21) | 68.1x | 0.34 s |
| 500,000 | unique | 500,000 | 57 MB | 0.57 s (0.51..0.64) | 57 MB | 0.29 s (0.29..0.41) | 2.0x | 0.82 s |
| 500,000 | churn | 5,024,400 | 568 MB | 9.75 s (6.42..20.00) | 57 MB | 0.61 s (0.45..0.82) | 15.9x | 1.43 s |
| 1,000,000 | unique | 1,000,000 | 113 MB | 1.19 s (1.14..1.41) | 113 MB | 0.77 s (0.66..0.89) | 1.5x | 1.93 s |
| 1,000,000 | churn | 4,506,464 | 509 MB | 8.81 s (8.47..12.27) | 113 MB | 1.16 s (0.98..2.71) | 7.6x | 2.98 s |

Key count identical after every restart: yes (30 recoveries pairs)
