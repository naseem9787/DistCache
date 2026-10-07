#!/usr/bin/env python3
"""Summarize docs/benchmarks/engine.csv: median / min / max ops per second per configuration."""
import csv, statistics, sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "docs/benchmarks/engine.csv"
runs = defaultdict(list)
for r in csv.DictReader(open(path)):
    runs[(r["engine"], int(r["threads"]), int(r["shards"]), int(r["read_pct"]))].append(float(r["ops_per_sec"]))

def row(key):
    v = runs[key]
    return statistics.median(v), min(v), max(v)

def fmt(x):
    return f"{x / 1e6:6.2f}M"

for rp in (90, 50):
    base = row(("none", 1, 1, rp))[0]
    print(f"\n### {rp}% GET / {100 - rp}% SET, 100k keys, 16 shards (median of runs, [min-max]) -- ops/sec")
    print(f"unsynchronized, 1 thread (baseline): {fmt(base)}")
    print("threads |        global mutex          |        sharded (16)         | sharded / global")
    for t in (1, 2, 4, 8, 12):
        g, s = row(("global", t, 1, rp)), row(("sharded", t, 16, rp))
        print(f"{t:7d} | {fmt(g[0])} [{fmt(g[1])}-{fmt(g[2])}] | {fmt(s[0])} [{fmt(s[1])}-{fmt(s[2])}] | {s[0] / g[0]:5.2f}x")

print("\n### shard count sweep: 8 threads, 90% GET -- ops/sec")
print("shards |   median   [min - max]")
for sh in (1, 2, 4, 8, 16, 64, 256):
    key = ("sharded", 8, sh, 90)
    if key in runs:
        m = row(key)
        print(f"{sh:6d} | {fmt(m[0])} [{fmt(m[1])} - {fmt(m[2])}]")
