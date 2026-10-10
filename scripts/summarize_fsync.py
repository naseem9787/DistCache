#!/usr/bin/env python3
"""Summarize docs/benchmarks/fsync.csv: SET throughput and latency per fsync policy."""
import csv, statistics, sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "docs/benchmarks/fsync.csv"
rows = defaultdict(list)
for r in csv.DictReader(open(path)):
    rows[(r["config"], int(r["pipeline"]))].append(r)

configs = ["no-persistence", "fsync-no", "fsync-everysec", "fsync-always"]
print("SET, 50 clients, 64-byte values, 4 workers. median of runs; rps in thousands [min-max]; latency ms p50 / p99")
print(f"{'config':<16} | {'pipeline 1':^38} | {'pipeline 16':^38}")
base = {}
for cfg in configs:
    cells = []
    for p in (1, 16):
        rs = rows.get((cfg, p), [])
        if not rs:
            cells.append(f"{'-':^38}")
            continue
        rps = [float(r["rps"]) / 1000 for r in rs]
        med = statistics.median(rps)
        base.setdefault(p, med)
        p50 = statistics.median(float(r["p50_ms"]) for r in rs)
        p99 = statistics.median(float(r["p99_ms"]) for r in rs)
        cells.append(f"{med:7.1f}k [{min(rps):6.1f}-{max(rps):6.1f}] {p50:6.3f}/{p99:7.3f}")
    print(f"{cfg:<16} | {cells[0]:^38} | {cells[1]:^38}")

print("\nthroughput relative to no persistence:")
for cfg in configs:
    out = []
    for p in (1, 16):
        rs = rows.get((cfg, p), [])
        out.append(f"{statistics.median(float(r['rps']) for r in rs) / 1000 / base[p] * 100:5.0f}%" if rs else "   -")
    print(f"  {cfg:<16} pipeline 1: {out[0]}   pipeline 16: {out[1]}")

print("\ngroup commit under fsync=always: log records written per fsync")
print("(INFO counters are cumulative over the 3 runs of a combination, so the last run is used)")
for p in (1, 16):
    rs = rows.get(("fsync-always", p), [])
    if not rs:
        continue
    last = [r for r in rs if r["rep"] == max(x["rep"] for x in rs)][0]
    if last["wal_fsyncs"] and float(last["wal_fsyncs"]) > 0:
        print(f"  pipeline {p:>2}: {float(last['wal_records']) / float(last['wal_fsyncs']):8.1f} records per fsync")

print("\nshortest run per config (requests / rps); runs under ~2 s are unreliable:")
for cfg in configs:
    ds = [float(r["requests"]) / float(r["rps"]) for p in (1, 16) for r in rows.get((cfg, p), [])]
    if ds:
        print(f"  {cfg:<16}: {min(ds):5.1f} s")
