#!/usr/bin/env python3
"""Rate limiter benchmarks (Phase 9).

    bench/harness/bench_ratelimit.py --suite throughput|sweep|failover|all [--repeats N] --out DIR

throughput  RL.ACQUIRE throughput and latency, by topology and by workload:
              allow   100,000 keys, huge bucket: every call is ALLOWED, so every call is a write
              deny    100,000 keys, tiny bucket: after 5 calls per key almost every call is DENIED (no write)
              hot     ONE key, capacity 100, refill 1000/s: saturated; the safety bound is checked on every run
            topologies: single node (2 threads, 1 core), single node (4 threads, 2 cores), 3-node cluster with a
            smart client, 3-node cluster behind the router, 1 primary + 1 replica (async), same with --repl-sync 1.
sweep       client-count sweep (pipeline 1) for single node, smart cluster and router cluster.
failover    hot bucket (capacity 200,000, no meaningful refill) drained through the router; the primary of a
            replicated shard is kill -9'd after 40 % of the tokens are gone. Over-admission = tokens the
            clients were told "allowed" for, beyond the capacity. async vs sync 1.

Same method as Phase 6-8: fresh processes per run, pinned CPUs, repeats in shuffled order, health probe at the
start and end, raw results in <out>/raw.jsonl, environment in env.json. Compare rows within one session only.
"""
import argparse, json, os, random, signal, socket, statistics, subprocess, sys, threading, time, types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bench_cluster as bc  # noqa: E402
import bench_repl as br     # noqa: E402
import run_suite as rs      # noqa: E402

ROOT, BUILD = rs.ROOT, os.path.join(rs.ROOT, "build")
LOADGEN_CPUS = "8-11"
NODE_CPUS = ["0-1", "2-3", "4-5"]   # one physical core (two hyper-threads) per node
ROUTER_CPUS = "6-7"

WORKLOADS = {
    "allow": dict(keyspace=100000, cap=1_000_000, rate="1000000", desc="many keys, always allowed (every call writes)"),
    "deny": dict(keyspace=100000, cap=5, rate="0.001", desc="many keys, almost always denied (no write)"),
    "hot": dict(keyspace=1, cap=100, rate="1000", desc="one key, capacity 100, 1000 tokens/s, saturated"),
}
TOPOS = ["single (2 thr, 1 core)", "single (4 thr, 2 cores)", "cluster-3 smart client", "cluster-3 router",
         "async 1 replica", "sync 1, 1 replica"]


class Topo:
    def __init__(self, kind):
        self.kind, self.nodes, self.router, self.target = kind, [], None, []
        if kind == "single (2 thr, 1 core)":
            n = self._n(0, NODE_CPUS[0], 2)
            self.target = ["--port", str(n.port)]
        elif kind == "single (4 thr, 2 cores)":
            n = self._n(0, "0-3", 4)
            self.target = ["--port", str(n.port)]
        elif kind in ("cluster-3 smart client", "cluster-3 router"):
            ns = [self._n(0, NODE_CPUS[i], 2) for i in range(3)]
            if kind.endswith("client"):
                self.target = ["--nodes", ",".join(f"n{i}=127.0.0.1:{n.port}" for i, n in enumerate(ns))]
            else:
                (rport,) = br.ports(1)
                cmd = ["taskset", "-c", ROUTER_CPUS, os.path.join(BUILD, "distcache_router"), "--port", str(rport)]
                for i, n in enumerate(ns):
                    cmd += ["--node", f"n{i}=127.0.0.1:{n.port}"]
                self.router = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                rs.wait_ready(rport, self.router)
                self.target = ["--port", str(rport)]
        elif kind in ("async 1 replica", "sync 1, 1 replica"):
            pp, prp = br.ports(2)
            extra = ["--repl-sync", "1", "--repl-sync-timeout-ms", "5000"] if kind.startswith("sync") else []
            p = br.Node(pp, prp, NODE_CPUS[0], extra=extra, threads=2).wait_ready()
            rp, rrp = br.ports(2)
            r = br.Node(rp, rrp, NODE_CPUS[1], replicaof=prp, threads=2).wait_ready()
            self.nodes += [p, r]
            assert br.wait_for(lambda: br.num(br.info(pp), "in_sync_replicas") >= 1, 30)
            self.target = ["--port", str(pp)]
        else:
            raise ValueError(kind)

    def _n(self, i, cpus, threads):
        (p,) = br.ports(1)
        n = br.Node(p, 0, cpus, threads=threads).wait_ready()
        self.nodes.append(n)
        return n

    def cpu(self):
        return sum(n.cpu() for n in self.nodes)

    def stop(self):
        if self.router:
            self.router.send_signal(signal.SIGTERM)
            try:
                self.router.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.router.kill()
        for n in self.nodes:
            n.stop()
        os.sync()


def rl_args(args, wl, pipeline, clients, duration=None):
    a = types.SimpleNamespace(**vars(args))
    a.keyspace = wl["keyspace"]
    d = duration or args.duration
    cmd = bc.loadgen_cmd(a, [], clients, pipeline, d, args.warmup, 0.0, False, "rl")
    return cmd + ["--rl-capacity", str(wl["cap"]), "--rl-rate", wl["rate"]]


def run_one(args, kind, wname, pipeline, clients):
    wl = WORKLOADS[wname]
    t = Topo(kind)
    try:
        cmd = rl_args(args, wl, pipeline, clients)
        # insert the target arguments right after the loadgen binary (after taskset -c CPUS BIN)
        i = cmd.index(args.loadgen) + 1
        cmd[i:i] = t.target
        c0, t0 = t.cpu(), time.time()
        res = bc.run_loadgen(cmd, args.duration + args.warmup + 600)
        wall = time.time() - t0
        rec = dict(suite="throughput", topo=kind, workload=wname, pipeline=pipeline, clients=clients,
                   server_cpu=round((t.cpu() - c0) / wall, 2), result=res)
        rl = res["result"]["rl"]
        # safety bound: tokens ever granted <= keys_touched_max * (capacity + rate * total_time); +0.25 s slack
        rate = float(wl["rate"])
        bound = wl["keyspace"] * (wl["cap"] + rate * (rl["total_s"] + 0.25))
        rec["bound"] = bound
        rec["bound_ok"] = rl["allowed_all"] <= bound
        if wname == "hot":
            rec["bound_ratio"] = rl["allowed_all"] / bound   # vs the bound with 0.25 s of slack
            rec["fluid_ratio"] = rl["allowed_all"] / (wl["cap"] + rate * rl["total_s"])   # vs the exact fluid limit cap + rate*T
        return rec
    finally:
        t.stop()
        time.sleep(args.settle)


def run_failover(args, sync):
    nodes, router = [], None
    try:
        pp, prp, r1p, r1rp, r2p, r2rp, rport = br.ports(7)
        sa = ["--repl-sync", str(sync), "--repl-sync-timeout-ms", "3000"] if sync else []
        P = br.Node(pp, prp, NODE_CPUS[0], extra=sa, require_lease=True, threads=2).wait_ready()
        R1 = br.Node(r1p, r1rp, NODE_CPUS[1], extra=sa, replicaof=prp, require_lease=True, threads=2).wait_ready()
        R2 = br.Node(r2p, r2rp, NODE_CPUS[2], extra=sa, replicaof=prp, require_lease=True, threads=2).wait_ready()
        nodes = [P, R1, R2]
        cmd = ["taskset", "-c", ROUTER_CPUS, os.path.join(BUILD, "distcache_router"), "--port", str(rport),
               "--node", f"a=127.0.0.1:{pp}", "--replica", f"a=127.0.0.1:{r1p}", "--replica", f"a=127.0.0.1:{r2p}",
               "--lease-ms", "500", "--probe-ms", "100", "--probe-timeout-ms", "200", "--guard-ms", "100",
               "--fail-after", "3", "--sync-replicas", str(sync)]
        router = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        rs.wait_ready(rport, router)
        assert br.wait_for(lambda: "state=healthy" in bc.resp(rport, "SHARD", "STATUS"), 30)
        CAP = 200_000
        wl = dict(keyspace=1, cap=CAP, rate="0.001")
        lg = rl_args(args, wl, 1, 8, duration=args.failover_duration)
        i = lg.index(args.loadgen) + 1
        lg[i:i] = ["--port", str(rport)]
        proc = subprocess.Popen(lg, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        # kill the primary once 40 % of the tokens have been granted (the primary's own counter)
        t_kill = None
        deadline = time.time() + 60
        while time.time() < deadline and proc.poll() is None:
            try:
                if br.num(br.info(pp), "rl_allowed") >= CAP * 0.4:
                    t_kill = time.time()
                    P.kill9()
                    break
            except Exception:
                pass
            time.sleep(0.003)
        out, err = proc.communicate()
        res = json.loads(out.strip().splitlines()[-1])
        rl = res["result"]["rl"]
        return dict(suite="failover", policy=f"sync {sync}" if sync else "async", capacity=CAP, killed=t_kill is not None,
                    told_allowed=rl["allowed_all"], over_admission=rl["allowed_all"] - CAP, errors=res["result"]["errors"],
                    drained=rl["allowed_all"] >= CAP * 0.9, result=res)
    finally:
        if router:
            router.send_signal(signal.SIGTERM)
            try:
                router.wait(timeout=10)
            except subprocess.TimeoutExpired:
                router.kill()
        for n in nodes:
            n.stop()
        os.sync()
        time.sleep(args.settle)


def fmt_k(x):
    return f"{x / 1000:,.1f}k"


def report(rows, out, env, args):
    md = ["# Rate limiter benchmark report\n",
          f"Machine: {env['cpu_model']}, {env['logical_cpus']} logical CPUs, kernel {env['kernel']}. Nodes pinned to one physical "
          f"core each (CPUs 0-1, 2-3, 4-5; the 4-thread single node uses 0-3), router on {ROUTER_CPUS}, load generators on {LOADGEN_CPUS}. "
          f"{args.duration:g} s measured (+{args.warmup:g} s warm-up) per run, 50 clients unless stated, commit {env['git_commit']}"
          f"{' (dirty)' if env['git_dirty'] else ''}.\n"]
    th = [r for r in rows if r.get("suite") == "throughput" and r.get("sweep") is None]
    if th:
        md.append("## Throughput and latency by topology and workload\n")
        md.append("Workloads: **allow** = " + WORKLOADS["allow"]["desc"] + "; **deny** = " + WORKLOADS["deny"]["desc"] +
                  "; **hot** = " + WORKLOADS["hot"]["desc"] + ". The `denied` column is the measured share of calls refused: "
                  "with 100,000 keys and capacity 5, **deny at pipeline 1 is only partly denied** (few requests per key), at pipeline 16 it is >90 % denied.\n")
        md.append("| workload | pipeline | topology | runs | req/s median (min..max) | CV | p50 | p95 | p99 | vs single (2 thr) | denied | server CPU | safety bound held |")
        md.append("|---|---:|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---|")
        for wn in WORKLOADS:
            for pl in (1, 16):
                base = [r["result"]["result"]["rps"] for r in th if r["topo"] == TOPOS[0] and r["workload"] == wn and r["pipeline"] == pl]
                bmed = statistics.median(base) if base else None
                for tp in TOPOS:
                    g = [r for r in th if r["topo"] == tp and r["workload"] == wn and r["pipeline"] == pl]
                    if not g:
                        continue
                    rps = [r["result"]["result"]["rps"] for r in g]
                    st = br.stats(rps)
                    lat = lambda k: statistics.median([r["result"]["result"]["latency_us"][k] for r in g])
                    rel = f"{st['median'] / bmed * 100 - 100:+.0f}%" if bmed else ""
                    held = f"{sum(1 for r in g if r['bound_ok'])}/{len(g)}"
                    den = statistics.median([r["result"]["result"]["rl"]["denied"] / max(1, r["result"]["result"]["rl"]["allowed"] + r["result"]["result"]["rl"]["denied"]) for r in g])
                    md.append(f"| {wn} | {pl} | {tp} | {len(g)} | {fmt_k(st['median'])} ({fmt_k(st['min'])}..{fmt_k(st['max'])}) | {st['cv']:.1f}% | "
                              f"{lat('p50'):,.0f} us | {lat('p95'):,.0f} us | {lat('p99'):,.0f} us | {rel} | "
                              f"{den * 100:.0f}% | {statistics.median([r['server_cpu'] for r in g]):.1f} | {held} |")
        md.append("")
        hot = [r for r in th if r["workload"] == "hot"]
        if hot:
            md.append("### Accuracy of the hot key\n")
            md.append("One bucket, capacity 100, refill 1000/s, hammered by 50 clients. `granted / bound` where bound = capacity + rate x "
                      "(warm-up + duration + 0.25 s): **it must be <= 1 (safety) and should be close to 1 (the limiter is not merely "
                      "refusing everything).**\n")
            md.append("`fluid` = granted / (capacity + rate x total time) with no slack; a value a hair above 1 only means the bucket "
                      "started a few ms before the load generator's own clock did.\n")
            md.append("| topology | pipeline | runs | granted / bound: min..max | granted / fluid limit: min..max | any run over the bound |")
            md.append("|---|---:|---:|---|---|---|")
            for tp in TOPOS:
                for pl in (1, 16):
                    g = [r for r in hot if r["topo"] == tp and r["pipeline"] == pl]
                    if g:
                        ratios = [r["bound_ratio"] for r in g]
                        fl = [r["fluid_ratio"] for r in g]
                        md.append(f"| {tp} | {pl} | {len(g)} | {min(ratios):.4f}..{max(ratios):.4f} | {min(fl):.4f}..{max(fl):.4f} | {'**YES**' if any(not r['bound_ok'] for r in g) else 'no'} |")
            md.append("")
        for wn in WORKLOADS:
            labs, vals = [], []
            for tp in TOPOS:
                g = [r["result"]["result"]["rps"] for r in th if r["topo"] == tp and r["workload"] == wn and r["pipeline"] == 16]
                if g:
                    labs.append(tp.split(" (")[0] if tp.startswith("single") else tp)
                    vals.append(statistics.median(g))
            if vals:
                bc.svg_bars(os.path.join(out, f"rl-{wn}-p16.svg"), f"RL.ACQUIRE throughput, {wn}, pipeline 16", labs, vals)
    sw = [r for r in rows if r.get("suite") == "throughput" and r.get("sweep")]
    if sw:
        md.append("## Client-count sweep (pipeline 1)\n")
        for wn in ("allow", "hot"):
            md.append(f"### workload: {wn}\n")
            cl = sorted({r["clients"] for r in sw})
            md.append("| topology | " + " | ".join(f"{c} clients" for c in cl) + " |")
            md.append("|---|" + "---:|" * len(cl))
            for tp in ("single (2 thr, 1 core)", "cluster-3 smart client", "cluster-3 router"):
                cells = []
                for c in cl:
                    g = [r for r in sw if r["topo"] == tp and r["workload"] == wn and r["clients"] == c]
                    if g:
                        rps = statistics.median([r["result"]["result"]["rps"] for r in g])
                        p99 = statistics.median([r["result"]["result"]["latency_us"]["p99"] for r in g])
                        cells.append(f"{fmt_k(rps)} (p99 {p99:,.0f} us)")
                    else:
                        cells.append("")
                md.append(f"| {tp} | " + " | ".join(cells) + " |")
            md.append("")
    fo = [r for r in rows if r.get("suite") == "failover"]
    if fo:
        md.append("## Over-admission across a failover (kill -9 of the primary after 40 % of the tokens are granted)\n")
        md.append("Bucket capacity 200,000, refill 0.001/s (none that matters), 8 clients through the router, shard = 1 primary + 2 replicas, "
                  "lease 500 ms / probe 100 ms. `told allowed` counts only answers the clients actually received.\n")
        md.append("| policy | kills | told allowed: each run | over-admission (told allowed - capacity): each run | bucket drained |")
        md.append("|---|---:|---|---|---|")
        for pol in ("async", "sync 1"):
            g = [r for r in fo if r["policy"] == pol]
            if g:
                told = ", ".join("{:,}".format(r["told_allowed"]) for r in g)
                over = ", ".join("{:+,}".format(r["over_admission"]) for r in g)
                md.append(f"| {pol} | {len(g)} ({sum(1 for r in g if r['killed'])} killed) | {told} | "
                          f"{over} | {sum(1 for r in g if r['drained'])}/{len(g)} |")
        md.append("")
    open(os.path.join(out, "report.md"), "w").write("\n".join(md))
    return "\n".join(md)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--suite", default="all", choices=["throughput", "sweep", "failover", "all"])
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--duration", type=float, default=8)
    ap.add_argument("--warmup", type=float, default=1.5)
    ap.add_argument("--failover-duration", type=float, default=12)
    ap.add_argument("--clients", type=int, default=50)
    ap.add_argument("--settle", type=float, default=2.0)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--loadgen-cpus", default=LOADGEN_CPUS)
    ap.add_argument("--loadgen-threads", type=int, default=4)
    ap.add_argument("--loadgen", default=os.path.join(BUILD, "distcache_loadgen"))
    ap.add_argument("--only", default="", help="substring filter on topology names")
    ap.add_argument("--out", default=os.path.join(ROOT, "results", "ratelimit"))
    ap.add_argument("--no-health-probe", action="store_true")
    args = ap.parse_args()
    args.keyspace = 1
    for p in (os.path.join(BUILD, "distcache"), os.path.join(BUILD, "distcache_router"), args.loadgen):
        if not os.path.exists(p):
            sys.exit(f"missing {p} (build first)")
    os.makedirs(args.out, exist_ok=True)
    ns = types.SimpleNamespace(data_root=os.path.expanduser("~/.distcache-data"), server_cpus="0-5", loadgen_cpus=args.loadgen_cpus,
                               duration=args.duration, warmup=args.warmup, repeats=args.repeats)
    env = rs.capture_env(ns, {"distcache": os.path.join(BUILD, "distcache"), "router": os.path.join(BUILD, "distcache_router")}, args.loadgen)
    health = {}
    probe = os.path.join(ROOT, "scripts", "health_probe.sh")
    if os.path.exists(probe) and not args.no_health_probe:
        r = subprocess.run([probe], capture_output=True, text=True)
        health["start"] = dict(exit_code=r.returncode, output=r.stdout.strip())
        print(r.stdout.strip(), flush=True)
    env["health"] = health
    json.dump(env, open(os.path.join(args.out, "env.json"), "w"), indent=2)

    work = []
    suites = ["throughput", "sweep", "failover"] if args.suite == "all" else [args.suite]
    for s in suites:
        if s == "throughput":
            for tp in TOPOS:
                if args.only and args.only not in tp:
                    continue
                for wn in WORKLOADS:
                    for pl in (1, 16):
                        work.append((lambda tp=tp, wn=wn, pl=pl: run_one(args, tp, wn, pl, args.clients), f"{tp} | {wn} P{pl}"))
        elif s == "sweep":
            for tp in ("single (2 thr, 1 core)", "cluster-3 smart client", "cluster-3 router"):
                if args.only and args.only not in tp:
                    continue
                for wn in ("allow", "hot"):
                    for c in (1, 4, 16, 50, 256):
                        def f(tp=tp, wn=wn, c=c):
                            r = run_one(args, tp, wn, 1, c)
                            r["sweep"] = True
                            return r
                        work.append((f, f"sweep {tp} | {wn} c{c}"))
        elif s == "failover":
            for sync in (0, 1):
                work.append((lambda sync=sync: run_failover(args, sync), f"failover sync={sync}"))

    rng = random.Random(args.seed)
    raw_path = os.path.join(args.out, "raw.jsonl")
    total, done = len(work) * args.repeats, 0
    with open(raw_path, "a") as raw:
        for rep in range(1, args.repeats + 1):
            order = work[:]
            rng.shuffle(order)
            for fn, label in order:
                try:
                    rec = fn()
                    rec.update(repeat=rep, ts=time.strftime("%Y-%m-%dT%H:%M:%S"))
                except Exception as ex:
                    rec = dict(suite="error", label=label, repeat=rep, error=repr(ex))
                raw.write(json.dumps(rec) + "\n")
                raw.flush()
                done += 1
                rps = rec.get("result", {}).get("result", {}).get("rps") if isinstance(rec.get("result"), dict) else None
                print(f"[{done}/{total}] {label:60} rep{rep} " + (f"{rps:,.0f} req/s" if rps else ("ERROR " + rec["error"] if "error" in rec else "ok")), flush=True)
    if not args.no_health_probe and os.path.exists(probe):
        r = subprocess.run([probe], capture_output=True, text=True)
        health["end"] = dict(exit_code=r.returncode, output=r.stdout.strip())
        env["health"] = health
        json.dump(env, open(os.path.join(args.out, "env.json"), "w"), indent=2)
    allrows = [json.loads(l) for l in open(raw_path) if l.strip()]
    errors = [r for r in allrows if r.get("suite") == "error"]
    text = report([r for r in allrows if r.get("suite") != "error"], args.out, env, args)
    if errors:
        text += "\n\n## Failed runs\n" + "\n".join(f"- {e['label']} rep{e['repeat']}: {e['error']}" for e in errors)
        open(os.path.join(args.out, "report.md"), "w").write(text)
    print(text)


if __name__ == "__main__":
    main()
