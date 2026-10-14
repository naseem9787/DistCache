#!/usr/bin/env python3
"""Cluster benchmarks for Phase 7 (consistent hashing).

    bench/harness/bench_cluster.py --suite scale     --repeats 5 --out results/cluster-scale
    bench/harness/bench_cluster.py --suite rebalance --repeats 3 --out results/cluster-rebalance
    bench/harness/bench_cluster.py --suite all

scale      Does adding nodes add throughput, and what does the router hop cost?
           Every node is a 1-worker server pinned to its own physical core (so "N nodes" means
           N cores of server), the router has a core to itself, the load generator has two.
             node-1        one node, direct                      (the reference)
             smart-N       N nodes, the client routes with the ring (no extra hop)
             router-N      N nodes behind distcache_router (CPUs 4-5), clients see one address
           x pipeline 1 and 16, 80 % GET, 50 clients, 100k uniform keys, preloaded.

rebalance  What does adding a node cost a running cache, with and without key migration?
           3 nodes behind the router, GET-only load; mid-run a 4th node is added with
           RING ADD (plain) or RING ADD ... MIGRATE. Hit rate and throughput per second are
           recorded; the migration command's own report (keys moved) and duration are kept.

Methodology is the same as run_suite.py: repeats are rounds (every variant once per round, random
order), servers are started fresh for every run, CPUs are pinned, the machine health probe runs
at the start and the end, raw results go to <out>/raw.jsonl and environment to <out>/env.json.
"""
import argparse, json, os, random, signal, socket, statistics, subprocess, sys, time, types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import run_suite as rs  # noqa: E402  (shared helpers: wait_ready, thread_cpu, env capture)

ROOT = rs.ROOT
# This machine has 6 physical cores, each with two hyperthreads (cpu 2k and 2k+1 are siblings).
# One node per physical core (cpus 0, 2, 4), the router on its own core (6-7), the load generator
# on the last two cores (8-11). The 4th node of the rebalance experiment only receives traffic
# after it is added and shares core 0 with node n0 (cpu 1): noted where it matters.
NODE_CPUS = [0, 2, 4, 1]
ROUTER_CPUS = "6-7"


def proc_cpu_seconds(pid):
    """CPU seconds (user + system) of the WHOLE process, including threads that already exited.
    (Summing /proc/<pid>/task/* misses finished threads, which matters for the router: its
    per-client threads are gone by the time the load generator has finished.)"""
    try:
        with open(f"/proc/{pid}/stat") as f:
            parts = f.read().rsplit(")", 1)[1].split()
        return (int(parts[11]) + int(parts[12])) / os.sysconf("SC_CLK_TCK")
    except (FileNotFoundError, ProcessLookupError, IndexError):
        return 0.0


def resp(port, *args, timeout=60):
    """One command, one reply, as text: simple strings/errors/integers verbatim, bulk payload."""
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        out = f"*{len(args)}\r\n" + "".join(f"${len(str(a))}\r\n{a}\r\n" for a in args)
        s.sendall(out.encode())
        buf = b""
        while b"\r\n" not in buf:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
        if buf[:1] == b"$":
            n = int(buf.split(b"\r\n", 1)[0][1:])
            body = buf.split(b"\r\n", 1)[1]
            while len(body) < n + 2:
                body += s.recv(65536)
            return body[:n].decode(errors="replace")
        return buf.split(b"\r\n", 1)[0].decode(errors="replace")


class Cluster:
    def __init__(self, args, nodes, router, extra_nodes=0, base_port=19000):
        self.args = args
        self.procs = []
        self.node_ports = []
        self.router_port = None
        self.router_proc = None
        port = base_port
        for i in range(nodes + extra_nodes):
            port += 1
            cmd = ["taskset", "-c", str(NODE_CPUS[i]), os.path.join(ROOT, "build", "distcache"), "--port", str(port),
                   "--threads", "1", "--mode", "sharded", "--shards", "64"]
            p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.procs.append(p)
            self.node_ports.append(port)
        for p, port in zip(self.procs, self.node_ports):
            rs.wait_ready(port, p)
        self.ring_nodes = nodes
        if router:
            port += 1
            cmd = ["taskset", "-c", ROUTER_CPUS, os.path.join(ROOT, "build", "distcache_router"), "--port", str(port)]
            for i in range(nodes):
                cmd += ["--node", f"n{i}=127.0.0.1:{self.node_ports[i]}"]
            self.router_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.router_port = port
            rs.wait_ready(port, self.router_proc)

    def node_names(self):
        return ",".join(f"n{i}=127.0.0.1:{p}" for i, p in enumerate(self.node_ports[:self.ring_nodes]))

    def key_counts(self):
        return [int(resp(p, "DBSIZE").lstrip(":")) for p in self.node_ports]

    def cpu(self):
        node = sum(proc_cpu_seconds(p.pid) for p in self.procs)
        router = proc_cpu_seconds(self.router_proc.pid) if self.router_proc else 0.0
        return node, router

    def stop(self):
        for p in ([self.router_proc] if self.router_proc else []) + self.procs:
            p.send_signal(signal.SIGTERM)
        for p in ([self.router_proc] if self.router_proc else []) + self.procs:
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()


def loadgen_cmd(args, target_args, clients, pipeline, duration, warmup, get_ratio, preload, label):
    cmd = ["taskset", "-c", args.loadgen_cpus, args.loadgen, *target_args, "--clients", str(clients),
           "--threads", str(args.loadgen_threads), "--pipeline", str(pipeline), "--duration", str(duration),
           "--warmup", str(warmup), "--keyspace", str(args.keyspace), "--value-size", "64",
           "--get-ratio", str(get_ratio), "--label", label, "--seed", str(args.seed)]
    if preload:
        cmd.append("--preload")
    return cmd


def run_loadgen(cmd, timeout):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode not in (0, 3) or not r.stdout.strip():
        raise RuntimeError(f"loadgen failed rc={r.returncode}: {r.stderr[-300:]}")
    return json.loads(r.stdout.strip().splitlines()[-1])


# ---- scale ------------------------------------------------------------------------------------

def scale_variants():
    v = []
    for p in (1, 16):
        v.append(dict(name=f"node-1, P{p}", topo="direct", nodes=1, pipeline=p))
        for n in (2, 3):
            v.append(dict(name=f"smart-{n}, P{p}", topo="smart", nodes=n, pipeline=p))
        for n in (1, 2, 3):
            v.append(dict(name=f"router-{n}, P{p}", topo="router", nodes=n, pipeline=p))
    # deep pipelines: does the sub-linear smart-client scaling at P16 come from each batch being
    # split over several nodes (smaller batches per node)?
    v.append(dict(name="node-1, P64", topo="direct", nodes=1, pipeline=64))
    v.append(dict(name="smart-3, P64", topo="smart", nodes=3, pipeline=64))
    v.append(dict(name="router-3, P64", topo="router", nodes=3, pipeline=64))
    return v


def run_scale(args, v):
    cl = Cluster(args, v["nodes"], router=(v["topo"] == "router"))
    try:
        if v["topo"] == "direct":
            target = ["--port", str(cl.node_ports[0])]
        elif v["topo"] == "smart":
            target = ["--nodes", cl.node_names()]
        else:
            target = ["--port", str(cl.router_port)]
        cmd = loadgen_cmd(args, target, args.clients, v["pipeline"], args.duration, args.warmup, 0.8, True, v["name"])
        c0 = cl.cpu()
        t0 = time.time()
        res = run_loadgen(cmd, args.duration + args.warmup + 600)
        wall = time.time() - t0
        c1 = cl.cpu()
        counts = cl.key_counts()
        return dict(variant=v["name"], topo=v["topo"], nodes=v["nodes"], pipeline=v["pipeline"], wall_s=round(wall, 2),
                    node_cpu_s=round(c1[0] - c0[0], 2), router_cpu_s=round(c1[1] - c0[1], 2), node_keys=counts[:v["nodes"]],
                    load_cmd=" ".join(cmd), result=res)
    finally:
        cl.stop()
        os.sync()
        time.sleep(args.settle)


# ---- rebalance --------------------------------------------------------------------------------

def run_rebalance(args, v):
    cl = Cluster(args, 3, router=True, extra_nodes=1)
    try:
        # load the data through the router so every key sits on the node the ring chose
        pre = loadgen_cmd(args, ["--port", str(cl.router_port)], args.clients, 16, 0.5, 0.0, 1.0, True, "preload")
        run_loadgen(pre, 600)
        before_counts = cl.key_counts()
        dur = args.rebalance_duration
        cmd = loadgen_cmd(args, ["--port", str(cl.router_port)], args.clients, 16, dur, args.warmup, 1.0, False, v["name"])
        launched = time.time()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(args.warmup + args.add_at)
        t_add = time.time()
        add = ["RING", "ADD", "n3", "127.0.0.1", str(cl.node_ports[3])] + (["MIGRATE"] if v["migrate"] else [])
        reply = resp(cl.router_port, *add, timeout=600)
        t_done = time.time()
        out, err = proc.communicate(timeout=dur + 120)
        if proc.returncode not in (0, 3) or not out.strip():
            raise RuntimeError(f"loadgen failed rc={proc.returncode}: {err[-300:]}")
        res = json.loads(out.strip().splitlines()[-1])
        return dict(variant=v["name"], migrate=v["migrate"], add_reply=reply,
                    add_started_s=round(t_add - launched - args.warmup, 2),
                    add_duration_s=round(t_done - t_add, 3), keys_before=before_counts, keys_after=cl.key_counts(),
                    result=res)
    finally:
        cl.stop()
        os.sync()
        time.sleep(args.settle)


# ---- reporting --------------------------------------------------------------------------------

def med(xs):
    return statistics.median(xs) if xs else float("nan")


def cv(xs):
    return (statistics.pstdev(xs) / statistics.mean(xs) * 100) if len(xs) > 1 and statistics.mean(xs) else 0.0


def fmt_rps(x):
    return f"{x / 1000:,.1f}k"


def svg_bars(path, title, labels, values, unit="req/s"):
    w, h, left, top = 760, 360, 70, 40
    mx = (max(values) * 1.15 if values else 1) or 1
    bw = (w - left - 20) / max(len(values), 1)
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" font-family="sans-serif" font-size="11">',
             f'<rect width="{w}" height="{h}" fill="#fff"/>',
             f'<text x="{w / 2}" y="22" text-anchor="middle" font-size="14" font-weight="bold">{title}</text>']
    for i in range(5):
        y = top + (h - top - 70) * (1 - i / 4)
        parts.append(f'<line x1="{left}" y1="{y:.0f}" x2="{w - 10}" y2="{y:.0f}" stroke="#ddd"/>')
        parts.append(f'<text x="{left - 6}" y="{y + 4:.0f}" text-anchor="end">{mx * i / 4 / 1000:,.0f}k</text>')
    for i, (lab, val) in enumerate(zip(labels, values)):
        bh = (h - top - 70) * val / mx
        x = left + i * bw + bw * 0.15
        color = "#2a7fba" if lab.startswith("smart") else ("#d9822b" if lab.startswith("router") else "#666")
        parts.append(f'<rect x="{x:.0f}" y="{h - 70 - bh:.0f}" width="{bw * 0.7:.0f}" height="{bh:.0f}" fill="{color}"/>')
        parts.append(f'<text x="{x + bw * 0.35:.0f}" y="{h - 72 - bh:.0f}" text-anchor="middle">{val / 1000:,.0f}k</text>')
        parts.append(f'<text transform="translate({x + bw * 0.35:.0f},{h - 60}) rotate(40)" text-anchor="start">{lab}</text>')
    parts.append(f'<text x="14" y="{top + 120}" transform="rotate(-90 14 {top + 120})">{unit}</text></svg>')
    open(path, "w").write("\n".join(parts))


def svg_lines(path, title, series, ylabel, ymax=None):
    """series: {name: [(x, y), ...]}"""
    w, h, left, top, bottom = 760, 340, 60, 40, 50
    xs = [x for s in series.values() for x, _ in s]
    ys = [y for s in series.values() for _, y in s]
    x0, x1 = min(xs), max(xs)
    y1 = ymax if ymax is not None else max(ys) * 1.1
    colors = ["#2a7fba", "#d9822b", "#3a9d5d", "#9b59b6"]
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" font-family="sans-serif" font-size="11">',
             f'<rect width="{w}" height="{h}" fill="#fff"/>',
             f'<text x="{w / 2}" y="22" text-anchor="middle" font-size="14" font-weight="bold">{title}</text>']
    def px(x): return left + (x - x0) / max(x1 - x0, 1e-9) * (w - left - 20)
    def py(y): return h - bottom - (y / y1) * (h - top - bottom)
    for i in range(6):
        yy = y1 * i / 5
        parts.append(f'<line x1="{left}" y1="{py(yy):.0f}" x2="{w - 20}" y2="{py(yy):.0f}" stroke="#ddd"/>')
        parts.append(f'<text x="{left - 6}" y="{py(yy) + 4:.0f}" text-anchor="end">{yy:g}</text>')
    for i, (name, pts) in enumerate(series.items()):
        c = colors[i % len(colors)]
        d = " ".join(f"{'M' if j == 0 else 'L'}{px(x):.1f},{py(y):.1f}" for j, (x, y) in enumerate(pts))
        parts.append(f'<path d="{d}" fill="none" stroke="{c}" stroke-width="2"/>')
        parts.append(f'<rect x="{left + 10 + i * 190}" y="{h - 22}" width="12" height="12" fill="{c}"/>')
        parts.append(f'<text x="{left + 28 + i * 190}" y="{h - 12}">{name}</text>')
    parts.append(f'<text x="{w / 2}" y="{h - 34}" text-anchor="middle">seconds into the measured window</text>')
    parts.append(f'<text x="14" y="{top + 100}" transform="rotate(-90 14 {top + 100})">{ylabel}</text></svg>')
    open(path, "w").write("\n".join(parts))


def report_scale(rows, out):
    by = {}
    for r in rows:
        by.setdefault(r["variant"], []).append(r)
    lines = ["## Scale-out: nodes x topology\n",
             "| variant | runs | req/s (median) | min..max | CV | p50 | p99 | hit rate | node CPU (cores) | router CPU (cores) | keys per node (last run) |",
             "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---|"]
    order = [v["name"] for v in scale_variants()]
    meds = {}
    for name in order:
        rs_ = by.get(name)
        if not rs_:
            continue
        rps = [r["result"]["result"]["rps"] for r in rs_]
        p50 = [r["result"]["result"]["latency_us"]["p50"] for r in rs_]
        p99 = [r["result"]["result"]["latency_us"]["p99"] for r in rs_]
        hits = [r["result"]["result"]["hits"] / max(1, r["result"]["result"]["gets"]) * 100 for r in rs_]
        wall = [r["wall_s"] for r in rs_]
        node_cpu = [r["node_cpu_s"] / w for r, w in zip(rs_, wall)]
        router_cpu = [r["router_cpu_s"] / w for r, w in zip(rs_, wall)]
        meds[name] = med(rps)
        lines.append(f"| {name} | {len(rs_)} | {fmt_rps(med(rps))} | {fmt_rps(min(rps))}..{fmt_rps(max(rps))} | {cv(rps):.1f}% | "
                     f"{med(p50):,.0f} us | {med(p99):,.0f} us | {med(hits):.1f}% | {med(node_cpu):.2f} | {med(router_cpu):.2f} | "
                     f"{rs_[-1]['node_keys']} |")
    # throughput relative to one node, same pipeline
    lines += ["", "Relative to `node-1` with the same pipeline depth (median req/s):", "",
              "| variant | vs node-1 |", "|---|---:|"]
    for name in order:
        if name in meds and not name.startswith("node-1"):
            base = meds.get(f"node-1, {name.split(', ')[1]}")
            if base:
                lines.append(f"| {name} | {meds[name] / base:.2f}x |")
    for p in (1, 16, 64):
        labs = [f"node-1" ] + [f"smart-{n}" for n in (2, 3)] + [f"router-{n}" for n in (1, 2, 3)]
        if p == 64:
            labs = ["node-1", "smart-3", "router-3"]
        vals = [meds.get(f"{l}, P{p}", 0) for l in labs]
        svg_bars(os.path.join(out, f"scale-P{p}.svg"), f"Throughput by topology, pipeline {p} (80% GET, 50 clients)", labs, vals)
    return "\n".join(lines) + "\n"


def report_rebalance(rows, out):
    lines = ["## Adding a node to a running cache (3 -> 4 nodes), GET-only load\n",
             "| variant | runs | keys moved (reply) | RING ADD duration | hit rate before | lowest hit rate during | hit rate after | req/s before | req/s during | req/s after |",
             "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|"]
    series = {}
    for name in ("RING ADD", "RING ADD MIGRATE"):
        rs_ = [r for r in rows if r["variant"] == name]
        if not rs_:
            continue
        b_hit, d_low, a_hit, b_rps, d_rps, a_rps, durs = [], [], [], [], [], [], []
        for r in rs_:
            ps = r["result"]["result"]["per_second"]
            ms = r["result"]["result"]["per_second_misses"]
            n = len(ps)
            hit = [(1 - ms[i] / ps[i]) * 100 if ps[i] else float("nan") for i in range(n)]
            t_add = r["add_started_s"]
            t_end = t_add + r["add_duration_s"]
            i_add, i_end = int(t_add), int(t_end) + 1
            before = list(range(0, max(1, i_add)))
            during = list(range(i_add, min(n, max(i_add + 1, i_end))))
            after = list(range(min(n - 1, i_end + 1), n))
            b_hit.append(med([hit[i] for i in before]))
            d_low.append(min(hit[i] for i in during))
            a_hit.append(med([hit[i] for i in after]))
            b_rps.append(med([ps[i] for i in before]))
            d_rps.append(med([ps[i] for i in during]))
            a_rps.append(med([ps[i] for i in after]))
            durs.append(r["add_duration_s"])
        last = rs_[-1]
        moved = last["add_reply"].replace("+OK", "").strip() or "-"
        lines.append(f"| {name} | {len(rs_)} | {moved} | {med(durs):.2f} s | {med(b_hit):.1f}% | {med(d_low):.1f}% | {med(a_hit):.1f}% | "
                     f"{fmt_rps(med(b_rps))} | {fmt_rps(med(d_rps))} | {fmt_rps(med(a_rps))} |")
        # timeline of the last run for the chart
        ps = last["result"]["result"]["per_second"]
        ms = last["result"]["result"]["per_second_misses"]
        series[name] = [(i, (1 - ms[i] / ps[i]) * 100 if ps[i] else 0.0) for i in range(len(ps))]
    if series:
        svg_lines(os.path.join(out, "rebalance-hit-rate.svg"), "Hit rate while a 4th node is added (GET-only, last run of each)",
                  series, "hit rate %", ymax=100)
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--suite", default="all", choices=["scale", "rebalance", "all"])
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--duration", type=float, default=8)
    ap.add_argument("--warmup", type=float, default=1.5)
    ap.add_argument("--rebalance-duration", type=float, default=20)
    ap.add_argument("--add-at", type=float, default=6, help="seconds into the measured window when the node is added")
    ap.add_argument("--clients", type=int, default=50)
    ap.add_argument("--keyspace", type=int, default=100000)
    ap.add_argument("--loadgen-cpus", default="8-11")
    ap.add_argument("--loadgen-threads", type=int, default=4)
    ap.add_argument("--loadgen", default=os.path.join(ROOT, "build", "distcache_loadgen"))
    ap.add_argument("--settle", type=float, default=2.0)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--variant-filter", default="", help="only scale variants containing one of these ';'-separated texts")
    ap.add_argument("--out", default=os.path.join(ROOT, "results", "cluster"))
    ap.add_argument("--no-health-probe", action="store_true")
    args = ap.parse_args()

    for p in (os.path.join(ROOT, "build", "distcache"), os.path.join(ROOT, "build", "distcache_router"), args.loadgen):
        if not os.path.exists(p):
            sys.exit(f"missing {p} (build first)")
    os.makedirs(args.out, exist_ok=True)

    ns = types.SimpleNamespace(data_root=os.path.expanduser("~/.distcache-data"), server_cpus="0-5", loadgen_cpus=args.loadgen_cpus,
                               duration=args.duration, warmup=args.warmup, repeats=args.repeats)
    os.makedirs(ns.data_root, exist_ok=True)
    env = rs.capture_env(ns, {"distcache": os.path.join(ROOT, "build", "distcache"),
                              "router": os.path.join(ROOT, "build", "distcache_router")}, args.loadgen)
    env["node_cpus"] = NODE_CPUS
    env["router_cpus"] = ROUTER_CPUS
    health = {}
    probe = os.path.join(ROOT, "scripts", "health_probe.sh")
    if os.path.exists(probe) and not args.no_health_probe:
        r = subprocess.run([probe], capture_output=True, text=True)
        health["start"] = dict(exit_code=r.returncode, output=r.stdout.strip())
        print(r.stdout.strip(), flush=True)
    env["health"] = health
    json.dump(env, open(os.path.join(args.out, "env.json"), "w"), indent=2)

    work = []
    if args.suite in ("scale", "all"):
        wanted = [t for t in args.variant_filter.split(";") if t]
        for v in scale_variants():
            if not wanted or any(t in v["name"] for t in wanted):
                work.append(("scale", v))
    if args.suite in ("rebalance", "all"):
        work.append(("rebalance", dict(name="RING ADD", migrate=False)))
        work.append(("rebalance", dict(name="RING ADD MIGRATE", migrate=True)))

    rng = random.Random(args.seed)
    raw_path = os.path.join(args.out, "raw.jsonl")
    total = len(work) * args.repeats
    done = 0
    with open(raw_path, "a") as raw:
        for rep in range(1, args.repeats + 1):
            order = work[:]
            rng.shuffle(order)
            for kind, v in order:
                try:
                    rec = run_scale(args, v) if kind == "scale" else run_rebalance(args, v)
                    rec.update(kind=kind, repeat=rep, ts=time.strftime("%Y-%m-%dT%H:%M:%S"))
                except Exception as ex:  # keep going; the failure is recorded
                    rec = dict(kind=kind, variant=v["name"], repeat=rep, error=str(ex))
                raw.write(json.dumps(rec) + "\n")
                raw.flush()
                done += 1
                r = rec.get("result", {}).get("result", {}) if isinstance(rec.get("result"), dict) else {}
                print(f"[{done}/{total}] {kind:9} {v['name']:22} rep{rep} " +
                      (f"{r.get('rps', 0):,.0f} req/s" if r else f"ERROR {rec.get('error')}"), flush=True)

    if not args.no_health_probe and os.path.exists(probe):
        r = subprocess.run([probe], capture_output=True, text=True)
        health["end"] = dict(exit_code=r.returncode, output=r.stdout.strip())
        env["health"] = health
        json.dump(env, open(os.path.join(args.out, "env.json"), "w"), indent=2)

    rows = [json.loads(l) for l in open(raw_path) if l.strip()]
    rows = [r for r in rows if "error" not in r]
    md = ["# Cluster benchmark report\n",
          f"Environment: {env['cpu_model']}, {env['logical_cpus']} logical CPUs, kernel {env['kernel']}; nodes pinned to CPUs "
          f"{NODE_CPUS} (one each), router on CPUs {ROUTER_CPUS}, load generator on CPUs {args.loadgen_cpus}. "
          f"{args.duration:g} s measured (+{args.warmup:g} s warm-up) per run, {args.repeats} repeats, commit {env['git_commit']}"
          f"{' (dirty)' if env['git_dirty'] else ''}.\n"]
    scale_rows = [r for r in rows if r["kind"] == "scale"]
    reb_rows = [r for r in rows if r["kind"] == "rebalance"]
    if scale_rows:
        md.append(report_scale(scale_rows, args.out))
    if reb_rows:
        md.append(report_rebalance(reb_rows, args.out))
    open(os.path.join(args.out, "report.md"), "w").write("\n".join(md))
    print("\n".join(md))


if __name__ == "__main__":
    main()
