#!/usr/bin/env python3
"""Summarize docs/benchmarks/network.csv: median [min-max] throughput and median latency."""
import csv, statistics, sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "docs/benchmarks/network.csv"
rows = defaultdict(list)
for r in csv.DictReader(open(path)):
    rows[(r["test"], int(r["clients"]), int(r["pipeline"]), r["config"])].append(r)

configs = ["single", "global-4w", "sharded-4w"]
for test in ("GET", "SET"):
    print(f"\n### {test}   (median of runs; rps in thousands, [min-max]; latency in ms: p50 / p99)")
    print(f"{'clients':>7} {'pipe':>4} | " + " | ".join(f"{c:^34}" for c in configs))
    for clients, pipe in [(1, 1), (10, 1), (50, 1), (200, 1), (50, 16)]:
        cells = []
        for cfg in configs:
            rs = rows.get((test, clients, pipe, cfg), [])
            if not rs:
                cells.append(f"{'-':^34}")
                continue
            rps = [float(r["rps"]) / 1000 for r in rs]
            p50 = statistics.median(float(r["p50_ms"]) for r in rs)
            p99 = statistics.median(float(r["p99_ms"]) for r in rs)
            cells.append(f"{statistics.median(rps):6.1f}k [{min(rps):5.1f}-{max(rps):5.1f}] {p50:5.3f}/{p99:5.3f}")
        print(f"{clients:>7} {pipe:>4} | " + " | ".join(f"{c:^34}" for c in cells))

# Sanity check: redis-benchmark time resolution is coarse, so very short runs are unreliable.
print("\n### shortest run per config (requests / rps), excluding 1-client runs")
for cfg in configs:
    ds = [float(r["requests"]) / float(r["rps"]) for k, v in rows.items() if k[3] == cfg and k[1] != 1 for r in v]
    if ds:
        print(f"{cfg:>12}: {min(ds):5.1f} s")
