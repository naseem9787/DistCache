#!/usr/bin/env python3
"""Summarize the output of scripts/microbench_wal.sh: median [min..max] of the headline metric per
configuration. Usage: summarize_micro.py docs/benchmarks/phase6/wal-microbench.txt"""
import collections, statistics, sys

HEADLINE = {"crc": "ns_per_call", "encode": "ns_per_record", "append": "records_per_s",
            "write": "MB_per_s_write_only", "engine": "ops_per_s"}
EXTRA = {"append": ["ns_per_append_per_thread"], "write": ["MB_per_s_incl_sync"], "engine": ["ns_per_op_per_thread"]}
DROP = {"rep", "iters", "sink", "append_phase_s", "drain_total_s", "MB_written", "seconds", "wal_records",
        "total_s", "write_phase_s", "records_per_thread", "ops_per_thread", "total_mb", "record_bytes"}


def parse(line):
    parts = line.strip().split(",")
    kv = {}
    for p in parts[1:]:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k] = v
    rep = kv.get("rep")
    name = parts[1] if len(parts) > 1 else "?"
    return name, parts, rep


def main():
    path = sys.argv[1]
    groups = collections.OrderedDict()
    for line in open(path):
        if not line.strip() or line.startswith("#"):
            continue
        parts = line.strip().split(",")
        test = parts[1]
        kv = dict(p.split("=", 1) for p in parts[2:] if "=" in p)
        key_params = tuple((k, v) for k, v in kv.items()
                           if k not in DROP and not k.startswith(("ns_", "MB_", "records_per_s", "ops_per_s")))
        g = groups.setdefault((test, key_params), collections.defaultdict(list))
        for m in [HEADLINE[test]] + EXTRA.get(test, []):
            if m in kv:
                g[m].append(float(kv[m]))
    cur = None
    for (test, params), metrics in groups.items():
        if test != cur:
            print(f"\n### {test}")
            cur = test
        desc = " ".join(f"{k}={v}" for k, v in params)
        cells = []
        for m, vals in metrics.items():
            med = statistics.median(vals)
            cells.append(f"{m}={med:,.1f} [{min(vals):,.1f}..{max(vals):,.1f}]")
        print(f"  {desc:<62} " + "  ".join(cells))


if __name__ == "__main__":
    main()
