#!/usr/bin/env python3
"""Turn raw.jsonl file(s) from run_suite.py into a Markdown report plus SVG charts.

    bench/harness/report.py results/run-a/raw.jsonl [results/run-b/raw.jsonl ...] --out results/report

Pass several raw files (independent invocations of the suite) to get the cross-run
reproducibility table. No third-party packages: charts are written as plain SVG.
"""
import argparse, collections, html, json, math, os, statistics


def load(paths):
    runs = []  # (file_index, record)
    for i, p in enumerate(paths):
        with open(p) as f:
            for line in f:
                line = line.strip()
                if line:
                    runs.append((i, json.loads(line)))
    return runs


def mann_whitney_p(xs, ys):
    """Two-sided Mann-Whitney U test p-value (normal approximation with tie correction).
    Non-parametric: it compares the two samples' ranks, so it does not assume a distribution and
    one outlier cannot dominate. With fewer than ~8 samples per side treat the p-value as rough."""
    n1, n2 = len(xs), len(ys)
    if n1 < 3 or n2 < 3:
        return float("nan")
    pooled = sorted([(v, 0) for v in xs] + [(v, 1) for v in ys])
    ranks, i, tie_term = {}, 0, 0.0
    ordered = [v for v, _ in pooled]
    r = [0.0] * len(pooled)
    while i < len(pooled):
        j = i
        while j + 1 < len(pooled) and ordered[j + 1] == ordered[i]:
            j += 1
        avg = (i + j) / 2 + 1
        for k in range(i, j + 1):
            r[k] = avg
        t = j - i + 1
        tie_term += t ** 3 - t
        i = j + 1
    r1 = sum(rank for rank, (_, g) in zip(r, pooled) if g == 0)
    u1 = r1 - n1 * (n1 + 1) / 2
    mu = n1 * n2 / 2
    n = n1 + n2
    var = n1 * n2 / 12 * ((n + 1) - tie_term / (n * (n - 1)))
    if var <= 0:
        return 1.0
    z = (abs(u1 - mu) - 0.5) / math.sqrt(var)  # continuity correction
    return math.erfc(z / math.sqrt(2))


def pct(a, b):
    return (a - b) / b * 100 if b else float("nan")


def cv(xs):
    return statistics.stdev(xs) / statistics.mean(xs) * 100 if len(xs) > 1 and statistics.mean(xs) else 0.0


def fmt_rps(x):
    return f"{x/1000:,.1f}k" if x >= 1000 else f"{x:,.0f}"


def fmt_us(x):
    return f"{x/1000:.2f} ms" if x >= 1000 else f"{x:.0f} us"


def per_second_cv(rec):
    ps = rec["result"]["result"]["per_second"]
    ps = ps[:-1] if len(ps) > 2 else ps  # last bucket can be partial
    return cv([float(x) for x in ps if x > 0]) if len(ps) > 1 else 0.0


def summarize(runs):
    groups = collections.OrderedDict()
    for fi, r in runs:
        if "error" in r or "result" not in r:
            key = (r.get("exp"), r.get("variant"), r.get("binary", "current"))
            groups.setdefault(key, dict(recs=[], errors=[]))["errors"].append(r.get("error", "?"))
            continue
        key = (r["exp"], r["variant"], r.get("binary", "current"))
        g = groups.setdefault(key, dict(recs=[], errors=[]))
        g["recs"].append((fi, r))
    out = collections.OrderedDict()
    for key, g in groups.items():
        recs = g["recs"]
        if not recs:
            out[key] = dict(errors=g["errors"], n=0)
            continue
        res = [r["result"]["result"] for _, r in recs]
        rps = [x["rps"] for x in res]
        lat = lambda q: [x["latency_us"][q] for x in res]
        hits = sum(x["hits"] for x in res)
        misses = sum(x["misses"] for x in res)
        cpu_cores = []
        thread_cores = collections.defaultdict(list)
        for _, r in recs:
            wall = r["wall_s"]
            cpu = r.get("server_cpu_seconds", {})
            cpu_cores.append(sum(cpu.values()) / wall if wall else 0)
            for name, secs in cpu.items():
                thread_cores[name].append(secs / wall if wall else 0)
        by_file = collections.defaultdict(list)
        for fi, r in recs:
            by_file[fi].append(r["result"]["result"]["rps"])
        any_rec = recs[0][1]
        diag = collections.defaultdict(list)
        for _, r in recs:
            for k, val in r["result"].get("server", {}).items():
                if k.startswith("diag_") and k.endswith(("_avg_ns", "_calls", "_total_ns", "_max_ns", "_grow_events", "_peak_bytes")):
                    try:
                        diag[k].append(float(val))
                    except ValueError:
                        pass
        out[key] = dict(
            diag={k: statistics.median(v) for k, v in diag.items()},
            n=len(recs), rps=rps, rps_median=statistics.median(rps), rps_min=min(rps), rps_max=max(rps), rps_cv=cv(rps),
            p50=statistics.median(lat("p50")), p95=statistics.median(lat("p95")), p99=statistics.median(lat("p99")),
            p99_min=min(lat("p99")), p99_max=max(lat("p99")), p999=statistics.median(lat("p999")),
            hit_ratio=(hits / (hits + misses) * 100) if hits + misses else None,
            sets=sum(x["sets"] for x in res), gets=sum(x["gets"] for x in res),
            cpu_cores=statistics.median(cpu_cores), thread_cores={k: statistics.median(v) for k, v in thread_cores.items()},
            rss_mb=statistics.median([r.get("server_rss_mb") or 0 for _, r in recs]),
            intra_cv=statistics.median([per_second_cv(r) for _, r in recs]),
            by_file={fi: statistics.median(v) for fi, v in by_file.items()},
            group=any_rec.get("group", ""), base=any_rec.get("base", False),
            title=any_rec.get("exp_title", key[0]), server_cfg=any_rec.get("server_cfg", {}), load_cfg=any_rec.get("load_cfg", {}),
            errors=g["errors"], server_info=any_rec["result"].get("server", {}),
        )
    return out


# ---- SVG ---------------------------------------------------------------------------------
PALETTE = ["#4c78a8", "#f58518", "#54a24b", "#b279a2", "#e45756", "#72b7b2", "#9d755d", "#bab0ac"]


def hbar_svg(title, rows, unit, path, note=""):
    """rows: list of dict(label, value, lo, hi, color). Horizontal bars with min-max whiskers."""
    if not rows:
        return
    label_w, bar_w, row_h, pad = 330, 560, 26, 40
    vmax = max(max(r["hi"], r["value"]) for r in rows) or 1
    h = pad + row_h * len(rows) + 50
    w = label_w + bar_w + 130
    s = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" font-family="Helvetica,Arial,sans-serif" font-size="12">',
         f'<rect width="{w}" height="{h}" fill="white"/>',
         f'<text x="10" y="22" font-size="15" font-weight="bold">{html.escape(title)}</text>']
    if note:
        s.append(f'<text x="10" y="{h-10}" fill="#555" font-size="11">{html.escape(note)}</text>')
    for i, r in enumerate(rows):
        y = pad + i * row_h
        x0 = label_w
        bw = r["value"] / vmax * bar_w
        s.append(f'<text x="{label_w-8}" y="{y+16}" text-anchor="end">{html.escape(r["label"])}</text>')
        s.append(f'<rect x="{x0}" y="{y+3}" width="{max(bw,1):.1f}" height="{row_h-8}" fill="{r["color"]}"/>')
        lo, hi = r["lo"] / vmax * bar_w, r["hi"] / vmax * bar_w
        cy = y + row_h / 2
        s.append(f'<line x1="{x0+lo:.1f}" y1="{cy}" x2="{x0+hi:.1f}" y2="{cy}" stroke="#222" stroke-width="1.5"/>')
        s.append(f'<line x1="{x0+lo:.1f}" y1="{cy-5}" x2="{x0+lo:.1f}" y2="{cy+5}" stroke="#222" stroke-width="1.5"/>')
        s.append(f'<line x1="{x0+hi:.1f}" y1="{cy-5}" x2="{x0+hi:.1f}" y2="{cy+5}" stroke="#222" stroke-width="1.5"/>')
        s.append(f'<text x="{x0+max(hi,bw)+8:.1f}" y="{y+16}">{r["text"]}</text>')
    s.append(f'<text x="{label_w}" y="{h-26}" fill="#555">{html.escape(unit)} (bar = median, whisker = min..max over all runs)</text>')
    s.append("</svg>")
    with open(path, "w") as f:
        f.write("\n".join(s))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("raw", nargs="+")
    ap.add_argument("--out", default="results/report")
    args = ap.parse_args()
    os.makedirs(os.path.join(args.out, "charts"), exist_ok=True)
    runs = load(args.raw)
    # the first binary of a suite run is the primary: baselines/comparisons are relative to it
    primary = next((r.get("primary_binary") for _, r in runs if r.get("primary_binary")), "current")
    S = summarize(runs)
    envs = []
    for p in args.raw:
        ep = os.path.join(os.path.dirname(p), "env.json")
        if os.path.exists(ep):
            envs.append(json.load(open(ep)))

    md = ["# Benchmark report", ""]
    if envs:
        e = envs[0]
        md += ["## Environment", "",
               f"- CPU: {e.get('cpu_model')} ({e.get('logical_cpus')} logical CPUs, siblings of cpu0: {e.get('cpu_topology_siblings')}), hypervisor: {e.get('hypervisor') or 'none'}",
               f"- Memory: {e.get('mem_total_mb')} MiB; kernel {e.get('kernel')}",
               f"- Compiler: {e.get('compiler')}; build: {e.get('cmake_build_type')}; flags: {e.get('compile_flags_sample')}",
               f"- Data directory filesystem: {e.get('data_dir_filesystem')}; vm.dirty_ratio={e.get('dirty_ratio')}, dirty_background_ratio={e.get('dirty_background_ratio')}",
               f"- Pinning: server on CPUs {e.get('server_cpus')}, load generator on CPUs {e.get('loadgen_cpus')}",
               f"- Per measurement: {e.get('warmup_s')} s warm-up (discarded) + {e.get('duration_s')} s measured, {e.get('repeats')} repeats per suite run, {len(envs)} independent suite run(s)",
               f"- Git commit: {e.get('git_commit')}{' (dirty working tree)' if e.get('git_dirty') else ''}",
               f"- Binaries: " + "; ".join(f"{k}={v['sha256_16']}" for k, v in e.get("binaries", {}).items()) + f"; loadgen={e.get('loadgen', {}).get('sha256_16')}",
               f"- Command: `{e.get('command_line')}`", ""]

    by_exp = collections.OrderedDict()
    for key, v in S.items():
        by_exp.setdefault(key[0], []).append((key, v))

    summary_json = {}
    for exp, items in by_exp.items():
        title = next((v.get("title") for _, v in items if v.get("n")), exp)
        md += [f"## {exp}: {title}", ""]
        # baseline per group
        base_of = {}
        for key, v in items:
            if v.get("n") and v.get("base") and key[2] == primary:
                base_of[v["group"]] = v["rps_median"]
        multi_bin = len({k[2] for k, _ in items}) > 1
        md += ["| variant | " + ("binary | " if multi_bin else "") + "runs | requests/s (median) | min..max | CV | p50 | p95 | p99 | vs baseline | server CPU (cores) |",
               "|---|" + ("---|" if multi_bin else "") + "---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        rows_chart_rps, rows_chart_p99 = [], []
        color_of_group = {}
        for key, v in items:
            if not v.get("n"):
                md.append(f"| {key[1]} | " + (f"{key[2]} | " if multi_bin else "") + f"0 | FAILED: {'; '.join(set(v['errors']))[:80]} | | | | | | | |")
                continue
            grp = v["group"] or ""
            color = color_of_group.setdefault(grp + ("|" + key[2] if multi_bin else ""), PALETTE[len(color_of_group) % len(PALETTE)])
            base = base_of.get(grp)
            is_base = v.get("base") and key[2] == primary
            # a non-primary binary is compared with the primary's baseline variant of the same group
            rel = f"{pct(v['rps_median'], base):+.0f}%" if base and not is_base else ("baseline" if is_base else "")
            hit = f" (hit {v['hit_ratio']:.0f}%)" if v["hit_ratio"] is not None and v["hit_ratio"] < 99.9 else ""
            label = key[1] + (f" [{key[2]}]" if multi_bin else "")
            md.append(f"| {key[1]}{hit} | " + (f"{key[2]} | " if multi_bin else "") +
                      f"{v['n']} | {fmt_rps(v['rps_median'])} | {fmt_rps(v['rps_min'])}..{fmt_rps(v['rps_max'])} | {v['rps_cv']:.1f}% | "
                      f"{fmt_us(v['p50'])} | {fmt_us(v['p95'])} | {fmt_us(v['p99'])} | {rel} | {v['cpu_cores']:.1f} |")
            rows_chart_rps.append(dict(label=label, value=v["rps_median"], lo=v["rps_min"], hi=v["rps_max"], color=color,
                                       text=fmt_rps(v["rps_median"]) + "/s"))
            rows_chart_p99.append(dict(label=label, value=v["p99"], lo=v["p99_min"], hi=v["p99_max"], color=color,
                                       text=fmt_us(v["p99"])))
            summary_json[f"{exp}|{key[1]}|{key[2]}"] = {k: v[k] for k in ("n", "rps_median", "rps_min", "rps_max", "rps_cv", "p50", "p95", "p99", "cpu_cores")}
        if any(v.get("diag") for _, v in items):
            names = ["encode", "append_wait", "append_locked", "shard_wait", "shard_hold", "flush_write", "flush_sync", "flush_swap"]
            maxes = ["append_wait", "append_locked", "shard_wait", "shard_hold", "flush_write"]
            md += ["", "Instrumentation (diagnostics build; median over runs). `avg` = nanoseconds per call:", "",
                   "| variant | " + " | ".join(f"{n} avg ns" for n in names) + " |", "|---|" + "---:|" * len(names)]
            for key, v in items:
                if v.get("n") and v.get("diag"):
                    md.append(f"| {key[1]} | " + " | ".join(
                        f"{v['diag'].get('diag_' + n + '_avg_ns', 0):.0f}" for n in names) + " |")
            md += ["", "Worst single observation (median over runs of each run's max), microseconds, and log-buffer behaviour:", "",
                   "| variant | " + " | ".join(f"max {n}" for n in maxes) + " | buffer reallocations | peak pending log MiB |",
                   "|---|" + "---:|" * (len(maxes) + 2)]
            for key, v in items:
                if v.get("n") and v.get("diag"):
                    d = v["diag"]
                    md.append(f"| {key[1]} | " + " | ".join(f"{d.get('diag_' + n + '_max_ns', 0) / 1000:,.0f}" for n in maxes) +
                              f" | {d.get('diag_buf_grow_events', 0):,.0f} | {d.get('diag_buf_peak_bytes', 0) / 1048576:.1f} |")
        md.append("")
        hbar_svg(f"{exp} {title}: throughput", rows_chart_rps, "requests per second", os.path.join(args.out, "charts", f"{exp}-throughput.svg"))
        hbar_svg(f"{exp} {title}: p99 latency", rows_chart_p99, "microseconds (lower is better)", os.path.join(args.out, "charts", f"{exp}-p99.svg"))
        md.append(f"Charts: `charts/{exp}-throughput.svg`, `charts/{exp}-p99.svg`")
        md.append("")

    # same variant, different binaries: is the difference real?
    bins = []
    for key in S:
        if key[2] not in bins:
            bins.append(key[2])
    if len(bins) > 1:
        md += ["## Binary comparison (same variant, same session, interleaved)", "",
               f"Each other binary against the primary (`{primary}`). p = two-sided Mann-Whitney U on the per-run "
               "requests/s; with n < 8 per side it is only a rough guide. 'diff' is the change in the median.", "",
               "| variant | binary | n | median req/s | primary median | diff | p |", "|---|---|---:|---:|---:|---:|---:|"]
        for key, v in S.items():
            if key[2] == primary or not v.get("n"):
                continue
            ref = S.get((key[0], key[1], primary))
            if not ref or not ref.get("n"):
                continue
            p = mann_whitney_p(v["rps"], ref["rps"])
            md.append(f"| {key[0]} {key[1]} | {key[2]} | {v['n']} / {ref['n']} | {fmt_rps(v['rps_median'])} | "
                      f"{fmt_rps(ref['rps_median'])} | {pct(v['rps_median'], ref['rps_median']):+.1f}% | "
                      f"{'n/a' if p != p else f'{p:.3f}'} |")
        md.append("")

    # cross-run reproducibility
    nfiles = len(args.raw)
    if nfiles > 1:
        md += ["## Reproducibility between independent suite runs", "",
               "Median requests/s of the same configuration in each independent invocation of the suite, and the",
               "relative difference between the largest and smallest of those medians.", "",
               "| config | " + " | ".join(f"run {i+1}" for i in range(nfiles)) + " | max diff |",
               "|---|" + "---:|" * nfiles + "---:|"]
        diffs = []
        for key, v in S.items():
            if v.get("n", 0) and len(v["by_file"]) == nfiles:
                vals = [v["by_file"][i] for i in range(nfiles)]
                d = (max(vals) - min(vals)) / statistics.mean(vals) * 100
                diffs.append((d, key, vals))
        for d, key, vals in sorted(diffs, reverse=True):
            md.append(f"| {key[0]} {key[1]}" + (f" [{key[2]}]" if key[2] != primary else "") + " | " + " | ".join(fmt_rps(x) for x in vals) + f" | {d:.1f}% |")
        if diffs:
            ds = [d for d, _, _ in diffs]
            md += ["", f"Across {len(ds)} configurations: median difference {statistics.median(ds):.1f}%, "
                       f"90th percentile {sorted(ds)[int(0.9*(len(ds)-1))]:.1f}%, worst {max(ds):.1f}%.", ""]

    md += ["## Method notes", "",
           "- Latency = time from the start of writing a request's batch to reading that request's reply (see `bench/loadgen.cpp`).",
           "- Clients are closed-loop: a stalled server makes clients wait instead of queueing more work, so tail latency during",
           "  stalls is understated relative to an open-loop arrival process.",
           "- Server CPU (cores) = CPU seconds consumed by all server threads divided by the wall time of the load run; it",
           "  includes preload and warm-up, so it is an upper-ish estimate of steady-state use.",
           "- CV = sample standard deviation / mean of the per-run requests/s across all repeats of that configuration.", ""]
    with open(os.path.join(args.out, "report.md"), "w") as f:
        f.write("\n".join(md))
    with open(os.path.join(args.out, "summary.json"), "w") as f:
        json.dump(summary_json, f, indent=1)
    print("wrote", os.path.join(args.out, "report.md"))


if __name__ == "__main__":
    main()
