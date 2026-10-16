#!/usr/bin/env python3
"""Replication and failover benchmarks (Phase 8).

    bench/harness/bench_repl.py --suite overhead|sweeps|lag|failover|resync|all  [--repeats N] --out DIR

overhead   What does each replication topology / acknowledgement policy cost on the write path?
           single node, replication log only, async x1/x2, sync 1 (1 or 2 replicas), sync 2.
           SET-only pipeline 1 and 16, and an 80 % GET mix at pipeline 16; 50 clients; direct to the primary.
sweeps     Throughput and latency of single / async-1 / sync-1 across client counts (pipeline 1) and
           pipeline depths (50 clients), SET only.
lag        Replication lag under sustained write load (primary-side lag_records sampled every 200 ms),
           and what a replica serves to readers while the primary is being written.
failover   kill -9 of the primary of a 3-node shard behind the router under steady write traffic:
           the unavailability window, the coordinator's own timeline, acknowledged writes lost, and
           the time for the old primary to rejoin as a synchronised replica.
resync     Full resynchronisation time and rate by dataset size, and catch-up after a link drop under load.

Method (same as the earlier phases): every process is a fresh `distcache`, CPUs are pinned (primary
0-3, replicas and router 4-5, load generators 6-11), repeats are rounds in seeded random order, the
machine health probe runs first and last, raw results go to <out>/raw.jsonl, environment to env.json.
A "single node" control row is part of every overhead session: compare within a session only.
"""
import argparse, json, os, random, signal, socket, statistics, subprocess, sys, tempfile, threading, time, types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bench_cluster as bc  # noqa: E402  (resp(), proc_cpu_seconds(), loadgen_cmd(), run_loadgen(), svg helpers)
import run_suite as rs      # noqa: E402

ROOT = rs.ROOT
BUILD = os.path.join(ROOT, "build")
PRIMARY_CPUS, REPLICA_CPUS, ROUTER_CPUS, LOADGEN_CPUS = "0-3", "4-5", "4-5", "6-11"


# ---- processes --------------------------------------------------------------------------------------

class Node:
    """A distcache process. `kind`: 'primary' | 'replica'."""

    def __init__(self, port, repl_port, cpus, extra=(), replicaof=None, threads=4, require_lease=False, data_dir=None):
        self.port, self.repl_port = port, repl_port
        cmd = ["taskset", "-c", cpus, os.path.join(BUILD, "distcache"), "--port", str(port), "--threads", str(threads)]
        if repl_port:
            cmd += ["--repl-port", str(repl_port)]
        if replicaof:
            cmd += ["--replicaof", f"127.0.0.1:{replicaof}"]
        if require_lease:
            cmd += ["--repl-require-lease"]
        if data_dir:
            cmd += ["--dir", data_dir, "--fsync", "everysec"]
        cmd += list(extra)
        self.cmd = cmd
        self.log = tempfile.TemporaryFile()
        self.p = subprocess.Popen(cmd, stdout=self.log, stderr=self.log)

    def wait_ready(self):
        rs.wait_ready(self.port, self.p)
        return self

    @property
    def pid(self):
        return self.p.pid

    def cpu(self):
        return bc.proc_cpu_seconds(self.p.pid)

    def kill9(self):
        self.p.send_signal(signal.SIGKILL)
        self.p.wait()

    def stop(self):
        if self.p.poll() is None:
            self.p.send_signal(signal.SIGTERM)
            try:
                self.p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait()
        self.log.close()


_ports = [20000]


def ports(n):
    out = []
    for _ in range(n):
        _ports[0] += 1
        out.append(_ports[0])
    return out


def info(port):
    """INFO as {field: value}; replicaN lines kept as their raw text."""
    d = {}
    for line in bc.resp(port, "INFO").splitlines():
        if ":" in line and not line.startswith("#"):
            k, _, v = line.partition(":")
            d[k] = v
    return d


def num(d, k, default=0):
    try:
        return int(d.get(k, default))
    except ValueError:
        return default


def replica_field(info_d, i, key):
    line = info_d.get(f"replica{i}", "")
    for part in line.split(","):
        if part.startswith(key + "="):
            return int(part.split("=", 1)[1])
    return None


def wait_for(pred, timeout=60, step=0.02):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(step)
    return False


def role(port):
    """ROLE -> list of strings: role, epoch, replid, offset, fenced, lease_ms, link, repl_port, lag, in_sync."""
    with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
        s.sendall(b"*1\r\n$4\r\nROLE\r\n")
        buf = b""
        while True:
            buf += s.recv(65536)
            if buf.startswith(b"*") and buf.count(b"\r\n") >= 21:
                break
        parts = buf.split(b"\r\n")
        return [parts[i].decode() for i in range(2, len(parts) - 1, 2)][:10]


class Cluster:
    """One primary and R replicas (all direct, no router)."""

    def __init__(self, replicas, sync=0, extra=()):
        pp, prp = ports(2)
        ex = list(extra) + (["--repl-sync", str(sync), "--repl-sync-timeout-ms", "5000"] if sync else [])
        self.primary = Node(pp, prp if replicas else (prp if sync is None else 0), PRIMARY_CPUS, extra=ex)
        self.replicas = []
        self.primary.wait_ready()
        for _ in range(replicas):
            rp, rrp = ports(2)
            self.replicas.append(Node(rp, rrp, REPLICA_CPUS, replicaof=prp, threads=2).wait_ready())
        if replicas:
            assert wait_for(lambda: num(info(pp), "in_sync_replicas") >= replicas, 30), "replicas never became in sync"

    def procs(self):
        return [self.primary] + self.replicas

    def cpu(self):
        return self.primary.cpu(), sum(r.cpu() for r in self.replicas)

    def caught_up(self, timeout=30):
        pp = self.primary.port
        target = num(info(pp), "repl_offset")
        return wait_for(lambda: all(num(info(r.port), "repl_offset") >= target for r in self.replicas), timeout)

    def stop(self):
        for r in self.replicas:
            r.stop()
        self.primary.stop()
        os.sync()


def stats(xs):
    xs = sorted(xs)
    if not xs:
        return dict(n=0)
    pick = lambda q: xs[min(len(xs) - 1, int(q * len(xs)))]
    return dict(n=len(xs), median=statistics.median(xs), min=xs[0], max=xs[-1], p95=pick(0.95), p99=pick(0.99),
                mean=statistics.mean(xs), cv=(statistics.pstdev(xs) / statistics.mean(xs) * 100) if len(xs) > 1 and statistics.mean(xs) else 0.0)


# ---- overhead -----------------------------------------------------------------------------------------------

TOPOLOGIES = [
    dict(name="single node (no replication flags)", replicas=0, sync=0, repl=False),
    dict(name="replication log only (0 replicas)", replicas=0, sync=0, repl=True),
    dict(name="async, 1 replica", replicas=1, sync=0, repl=True),
    dict(name="async, 2 replicas", replicas=2, sync=0, repl=True),
    dict(name="sync 1, 1 replica", replicas=1, sync=1, repl=True),
    dict(name="sync 1, 2 replicas", replicas=2, sync=1, repl=True),
    dict(name="sync 2, 2 replicas", replicas=2, sync=2, repl=True),
]
WORKLOADS = [("SET only, P1", 0.0, 1), ("SET only, P16", 0.0, 16), ("80% GET, P16", 0.8, 16)]


def make_cluster(topo):
    if topo["repl"] and topo["replicas"] == 0:
        pp, prp = ports(2)
        c = Cluster.__new__(Cluster)
        c.primary = Node(pp, prp, PRIMARY_CPUS).wait_ready()
        c.replicas = []
        return c
    if not topo["repl"]:
        pp = ports(1)[0]
        c = Cluster.__new__(Cluster)
        c.primary = Node(pp, 0, PRIMARY_CPUS).wait_ready()
        c.replicas = []
        return c
    return Cluster(topo["replicas"], topo["sync"])


def run_load(args, cl, get_ratio, pipeline, clients, label, preload=False):
    cmd = bc.loadgen_cmd(args, ["--port", str(cl.primary.port)], clients, pipeline, args.duration, args.warmup, get_ratio, preload, label)
    c0 = cl.cpu()
    t0 = time.time()
    res = bc.run_loadgen(cmd, args.duration + args.warmup + 600)
    wall = time.time() - t0
    c1 = cl.cpu()
    return res, wall, (c1[0] - c0[0]) / wall, (c1[1] - c0[1]) / wall


def run_overhead_one(args, topo, wl):
    name, get_ratio, pipeline = wl
    cl = make_cluster(topo)
    try:
        res, wall, pcpu, rcpu = run_load(args, cl, get_ratio, pipeline, args.clients, f"{topo['name']} | {name}", preload=get_ratio > 0)
        inf = info(cl.primary.port)
        end_lag = None
        if cl.replicas:
            time.sleep(1.0)
            inf2 = info(cl.primary.port)
            end_lag = [replica_field(inf2, i, "lag_records") or 0 for i in range(len(cl.replicas))]
        return dict(suite="overhead", topo=topo["name"], workload=name, wall_s=round(wall, 2), primary_cpu=round(pcpu, 2),
                    replica_cpu=round(rcpu, 2), end_lag_records=end_lag, backlog_trimmed=num(inf, "backlog_trimmed_records"),
                    cuts=num(inf, "replicas_cut_too_old") + num(inf, "replicas_cut_send_failed"), result=res)
    finally:
        cl.stop()
        time.sleep(args.settle)


# ---- sweeps --------------------------------------------------------------------------------------------------------

SWEEP_TOPOS = [t for t in TOPOLOGIES if t["name"] in ("single node (no replication flags)", "async, 1 replica", "sync 1, 1 replica")]


def run_sweep_one(args, topo, clients, pipeline):
    cl = make_cluster(topo)
    try:
        res, wall, pcpu, rcpu = run_load(args, cl, 0.0, pipeline, clients, f"{topo['name']} | c{clients} P{pipeline}")
        return dict(suite="sweeps", topo=topo["name"], clients=clients, pipeline=pipeline, wall_s=round(wall, 2),
                    primary_cpu=round(pcpu, 2), replica_cpu=round(rcpu, 2), result=res)
    finally:
        cl.stop()
        time.sleep(args.settle)


# ---- lag --------------------------------------------------------------------------------------------------------------

def run_lag_one(args, pipeline, replica_reads):
    cl = Cluster(1, 0)
    try:
        pp, rp = cl.primary.port, cl.replicas[0].port
        procs = []
        if replica_reads:
            # data first (through the primary), then writers on the primary and readers on the replica together
            bc.run_loadgen(bc.loadgen_cmd(args, ["--port", str(pp)], 20, 16, 1, 0, 0.5, True, "preload"), 600)
            cl.caught_up()
        dur = args.lag_duration
        wcmd = bc.loadgen_cmd(args, ["--port", str(pp)], args.clients, pipeline, dur, args.warmup, 0.0, False, f"writers P{pipeline}")
        wp = subprocess.Popen(wcmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        rp_proc = None
        if replica_reads:
            rcmd = bc.loadgen_cmd(args, ["--port", str(rp)], 20, 16, dur, args.warmup, 1.0, False, "replica readers")
            rp_proc = subprocess.Popen(rcmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        lags, lag_ms, rlag = [], [], []
        t0 = time.time()
        while wp.poll() is None:
            try:
                pi = info(pp)
                lag0 = replica_field(pi, 0, "lag_records")
                if lag0 is not None and time.time() - t0 > args.warmup:
                    lags.append(lag0)
                ri = info(rp)
                if time.time() - t0 > args.warmup:
                    rlag.append(num(ri, "lag_records"))
                    lag_ms.append(num(ri, "lag_ms"))
            except Exception:
                pass
            time.sleep(0.2)
        out, err = wp.communicate()
        res = json.loads(out.strip().splitlines()[-1])
        rres = None
        if rp_proc:
            o2, _ = rp_proc.communicate()
            rres = json.loads(o2.strip().splitlines()[-1])
        # how long to drain once the writers stop
        t1 = time.time()
        drained = cl.caught_up(30)
        drain_s = time.time() - t1
        pi, ri = info(pp), info(rp)
        cuts = num(pi, "replicas_cut_too_old") + num(pi, "replicas_cut_send_failed")
        return dict(suite="lag", pipeline=pipeline, replica_reads=replica_reads, cuts=cuts,
                    replica_full_syncs=num(ri, "full_syncs"), backlog_trimmed=num(pi, "backlog_trimmed_records"), primary_lag_records=stats(lags),
                    replica_seen_lag_records=stats(rlag), replica_lag_ms=stats(lag_ms), drain_s=round(drain_s, 3), drained=drained,
                    writers=res, readers=rres)
    finally:
        cl.stop()
        time.sleep(args.settle)


# ---- failover ----------------------------------------------------------------------------------------------------------

FAILOVER_CONFIGS = [
    dict(name="default timings (lease 2000, probe 500), async", lease=2000, probe=500, timeout=500, guard=500, sync=0),
    dict(name="default timings (lease 2000, probe 500), sync 1", lease=2000, probe=500, timeout=500, guard=500, sync=1),
    dict(name="fast timings (lease 500, probe 100), async", lease=500, probe=100, timeout=200, guard=100, sync=0),
    dict(name="fast timings (lease 500, probe 100), sync 1", lease=500, probe=100, timeout=200, guard=100, sync=1),
]


def run_failover_one(args, cfg):
    nodes = []
    router = None
    try:
        pp, prp, r1p, r1rp, r2p, r2rp = ports(6)
        sync_args = ["--repl-sync", str(cfg["sync"]), "--repl-sync-timeout-ms", "3000"] if cfg["sync"] else []
        P = Node(pp, prp, PRIMARY_CPUS, extra=sync_args, require_lease=True, threads=2).wait_ready()
        R1 = Node(r1p, r1rp, REPLICA_CPUS, extra=sync_args, replicaof=prp, require_lease=True, threads=2).wait_ready()
        R2 = Node(r2p, r2rp, REPLICA_CPUS, extra=sync_args, replicaof=prp, require_lease=True, threads=2).wait_ready()
        nodes = [P, R1, R2]
        (rport,) = ports(1)
        rcmd = ["taskset", "-c", ROUTER_CPUS, os.path.join(BUILD, "distcache_router"), "--port", str(rport),
                "--node", f"a=127.0.0.1:{pp}", "--replica", f"a=127.0.0.1:{r1p}", "--replica", f"a=127.0.0.1:{r2p}",
                "--lease-ms", str(cfg["lease"]), "--probe-ms", str(cfg["probe"]), "--probe-timeout-ms", str(cfg["timeout"]),
                "--guard-ms", str(cfg["guard"]), "--fail-after", "3", "--sync-replicas", str(cfg["sync"])]
        router = subprocess.Popen(rcmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        rs.wait_ready(rport, router)
        assert wait_for(lambda: "state=healthy" in bc.resp(rport, "SHARD", "STATUS"), 30), "shard never became healthy"
        # preload a dataset so the old primary's rejoin includes a real snapshot
        pre = bc.loadgen_cmd(args, ["--port", str(rport)], 20, 16, 1, 0, 0.5, True, "preload")
        bc.run_loadgen(pre, 600)
        assert wait_for(lambda: all(num(info(n.port), "repl_offset") >= num(info(pp), "repl_offset") for n in (R1, R2)), 30)

        # steady writer through the router; records (t, ok) for every attempt
        events, acked = [], []
        stop = threading.Event()

        def writer():
            s = None
            i = 0
            while not stop.is_set():
                try:
                    if s is None:
                        s = socket.create_connection(("127.0.0.1", rport), timeout=3)
                    key = f"w{i}"
                    msg = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n$1\r\nv\r\n".encode()
                    t = time.time()
                    s.sendall(msg)
                    r = s.recv(256)
                    ok = r.startswith(b"+OK")
                    events.append((t, ok))
                    if ok:
                        acked.append(key)
                    else:
                        time.sleep(0.005)
                    i += 1
                except Exception:
                    events.append((time.time(), False))
                    s = None
                    time.sleep(0.01)
                    i += 1
                    continue
                time.sleep(0.002)

        th = threading.Thread(target=writer)
        th.start()
        time.sleep(1.5)
        t_kill = time.time()
        P.kill9()
        ok_after = wait_for(lambda: any(ok for (t, ok) in events if t > t_kill + 0.05) and
                            "failovers=1" in bc.resp(rport, "SHARD", "STATUS") and "state=healthy" in bc.resp(rport, "SHARD", "STATUS"), 60, 0.05)
        time.sleep(1.5)
        stop.set()
        th.join()
        fails = [t for (t, ok) in events if t > t_kill and not ok]
        succ_after = [t for (t, ok) in events if t > t_kill and ok]
        first_fail = min(fails) - t_kill if fails else None
        # first success AFTER the unavailability window opened
        window_end = None
        if fails:
            last_fail = max(fails)
            nxt = [t for t in succ_after if t > last_fail]
            window_end = (min(nxt) - t_kill) if nxt else None
        timeline = bc.resp(rport, "SHARD", "EVENTS")
        # acknowledged writes that are missing now
        missing = 0
        chk = socket.create_connection(("127.0.0.1", rport), timeout=5)
        for key in acked:
            chk.sendall(f"*2\r\n$3\r\nGET\r\n${len(key)}\r\n{key}\r\n".encode())
            r = chk.recv(256)
            if r.startswith(b"$-1"):
                missing += 1
        chk.close()
        # the old primary comes back (empty process, same ports): time until it is a synchronised replica
        t_back = time.time()
        P2 = Node(pp, prp, PRIMARY_CPUS, extra=sync_args, require_lease=True, threads=2).wait_ready()
        nodes.append(P2)
        rejoined = wait_for(lambda: role(pp)[0] == "slave" and role(pp)[6] == "synced", 90, 0.05)
        rejoin_s = time.time() - t_back
        return dict(suite="failover", config=cfg["name"], promoted_ok=ok_after, total_attempts=len(events), acked=len(acked),
                    acked_missing=missing, first_error_after_kill_s=first_fail, writes_possible_again_s=window_end,
                    old_primary_rejoin_s=round(rejoin_s, 3) if rejoined else None, events=timeline.splitlines())
    finally:
        if router:
            router.send_signal(signal.SIGTERM)
            router.wait(timeout=10)
        for n in nodes:
            n.stop()
        time.sleep(args.settle)


# ---- resync --------------------------------------------------------------------------------------------------------------

def run_resync_full(args, keys):
    pp, prp = ports(2)
    P = Node(pp, prp, PRIMARY_CPUS).wait_ready()
    R = None
    try:
        cmd = bc.loadgen_cmd(args, ["--port", str(pp)], 20, 16, 1, 0, 0.2, True, "load")
        cmd[cmd.index("--keyspace") + 1] = str(keys)
        bc.run_loadgen(cmd, 3600)
        rp, rrp = ports(2)
        t0 = time.time()
        R = Node(rp, rrp, REPLICA_CPUS, replicaof=prp, threads=2)
        R.wait_ready()
        ok = wait_for(lambda: info(rp).get("replica_state") == "online" and num(info(rp), "repl_offset") >= num(info(pp), "repl_offset"), 600, 0.02)
        took = time.time() - t0
        sz = bc.resp(rp, "DBSIZE")
        return dict(suite="resync", kind="full", keys=keys, seconds=round(took, 3), synced=ok, replica_dbsize=sz,
                    keys_per_s=round(keys / took) if took > 0 else None)
    finally:
        if R:
            R.stop()
        P.stop()
        time.sleep(args.settle)


def run_resync_partial(args):
    cl = Cluster(1, 0)
    try:
        pp, rp = cl.primary.port, cl.replicas[0].port
        cmd = bc.loadgen_cmd(args, ["--port", str(pp)], args.clients, 16, 12, 1.0, 0.0, False, "writers")
        wp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(1.0 + 4.0)
        before = info(rp)
        t0 = time.time()
        # re-point the replica at its own primary: its link is dropped and re-established (a partial resync)
        epoch = role(rp)[1]
        bc.resp(rp, "REPLICAOF", "127.0.0.1", str(cl.primary.repl_port), epoch)
        # sample until its lag is small again
        samples = []
        recovered = None
        while wp.poll() is None:
            ri = info(rp)
            lag = num(ri, "lag_records")
            samples.append((round(time.time() - t0, 3), lag, ri.get("replica_state", "")))
            if recovered is None and ri.get("replica_state") == "online" and time.time() - t0 > 0.05:
                recovered = time.time() - t0
            time.sleep(0.05)
        out, _ = wp.communicate()
        after = info(rp)
        cuts = num(info(cl.primary.port), "replicas_cut_too_old") + num(info(cl.primary.port), "replicas_cut_send_failed")
        return dict(suite="resync", kind="partial-under-load", cuts=cuts, recovered_online_s=round(recovered, 3) if recovered else None,
                    partial_syncs=num(after, "partial_syncs"), full_syncs=num(after, "full_syncs"),
                    max_lag_records=max([s[1] for s in samples] or [0]), samples=samples[:200])
    finally:
        cl.stop()
        time.sleep(args.settle)


# ---- reporting ---------------------------------------------------------------------------------------------------------------

def fmt_k(x):
    return f"{x / 1000:,.1f}k"


def report(rows, out, env, args):
    md = ["# Replication benchmark report\n",
          f"Machine: {env['cpu_model']}, {env['logical_cpus']} logical CPUs, kernel {env['kernel']}; primary on CPUs {PRIMARY_CPUS}, replicas/router on {REPLICA_CPUS}, "
          f"load generators on {LOADGEN_CPUS}. {args.duration:g} s measured (+{args.warmup:g} s warm-up) per run, {args.clients} clients unless stated, commit {env['git_commit']}"
          f"{' (dirty)' if env['git_dirty'] else ''}.\n"]
    ov = [r for r in rows if r.get("suite") == "overhead"]
    if ov:
        md.append("## Write-path cost by topology and acknowledgement policy\n")
        md.append("| topology | workload | runs | req/s (median) | min..max | CV | p50 | p95 | p99 | vs single | primary CPU | replica CPU | end lag (records) |")
        md.append("|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---|")
        base = {}
        for wl in [w[0] for w in WORKLOADS]:
            rr = [r["result"]["result"]["rps"] for r in ov if r["topo"] == TOPOLOGIES[0]["name"] and r["workload"] == wl]
            base[wl] = statistics.median(rr) if rr else None
        meds = {}
        for t in TOPOLOGIES:
            for wl in [w[0] for w in WORKLOADS]:
                rs_ = [r for r in ov if r["topo"] == t["name"] and r["workload"] == wl]
                if not rs_:
                    continue
                rps = [r["result"]["result"]["rps"] for r in rs_]
                lat = lambda k: statistics.median([r["result"]["result"]["latency_us"][k] for r in rs_])
                st = stats(rps)
                meds[(t["name"], wl)] = st["median"]
                rel = f"{st['median'] / base[wl] * 100 - 100:+.0f}%" if base[wl] else ""
                lag = [x for r in rs_ if r["end_lag_records"] for x in r["end_lag_records"]]
                md.append(f"| {t['name']} | {wl} | {len(rs_)} | {fmt_k(st['median'])} | {fmt_k(st['min'])}..{fmt_k(st['max'])} | {st['cv']:.1f}% | "
                          f"{lat('p50'):,.0f} us | {lat('p95'):,.0f} us | {lat('p99'):,.0f} us | {rel} | "
                          f"{statistics.median([r['primary_cpu'] for r in rs_]):.2f} | {statistics.median([r['replica_cpu'] for r in rs_]):.2f} | "
                          f"{'max ' + str(max(lag)) if lag else '-'} |")
        md.append("")
        for wl in [w[0] for w in WORKLOADS]:
            labs = [t["name"].replace("single node (no replication flags)", "single").replace("replication log only (0 replicas)", "log only") for t in TOPOLOGIES]
            vals = [meds.get((t["name"], wl), 0) for t in TOPOLOGIES]
            fn = "overhead-" + wl.replace(" ", "").replace(",", "").replace("%", "") + ".svg"
            bc.svg_bars(os.path.join(out, fn), f"Throughput by topology: {wl}", labs, vals)
    sw = [r for r in rows if r.get("suite") == "sweeps"]
    if sw:
        md.append("## Client-count and pipeline-depth sweeps (SET only)\n")
        for title, key, vals in (("Clients (pipeline 1)", "clients", sorted({r["clients"] for r in sw if r["pipeline"] == 1})),
                                 ("Pipeline depth (50 clients)", "pipeline", sorted({r["pipeline"] for r in sw if r["clients"] == args.clients}))):
            md.append(f"### {title}\n")
            md.append("| topology | " + " | ".join(f"{key}={v}" for v in vals) + " |")
            md.append("|---|" + "---:|" * len(vals))
            for t in SWEEP_TOPOS:
                cells = []
                for v in vals:
                    rs_ = [r for r in sw if r["topo"] == t["name"] and ((r["clients"] == v and r["pipeline"] == 1) if key == "clients" else (r["pipeline"] == v and r["clients"] == args.clients))]
                    if rs_:
                        rps = statistics.median([r["result"]["result"]["rps"] for r in rs_])
                        p99 = statistics.median([r["result"]["result"]["latency_us"]["p99"] for r in rs_])
                        cells.append(f"{fmt_k(rps)} (p99 {p99:,.0f} us)")
                    else:
                        cells.append("")
                md.append(f"| {t['name']} | " + " | ".join(cells) + " |")
            md.append("")
    lg = [r for r in rows if r.get("suite") == "lag"]
    if lg:
        md.append("## Replication lag under sustained write load (async, 1 replica)\n")
        md.append("| pipeline | replica readers | runs | writer req/s | primary-side lag (records): median / p95 / p99 / max | replica-seen lag_ms (p99 / max) | drain after load (s) | replica read req/s | replica read hit rate | runs where the replica was cut off (backlog overflow) |")
        md.append("|---:|---|---:|---:|---|---|---:|---:|---:|---:|")
        for pl in (1, 16, 64):
            for rr_ in (False, True):
                g = [r for r in lg if r["pipeline"] == pl and r["replica_reads"] == rr_]
                if not g:
                    continue
                allv = [x for r in g for x in [r["primary_lag_records"].get("median", 0)]]
                med = statistics.median([r["primary_lag_records"].get("median", 0) for r in g])
                p95 = statistics.median([r["primary_lag_records"].get("p95", 0) for r in g])
                p99 = statistics.median([r["primary_lag_records"].get("p99", 0) for r in g])
                mx = max(r["primary_lag_records"].get("max", 0) for r in g)
                lms99 = statistics.median([r["replica_lag_ms"].get("p99", 0) for r in g])
                lmsmax = max(r["replica_lag_ms"].get("max", 0) for r in g)
                wrps = statistics.median([r["writers"]["result"]["rps"] for r in g])
                drain = statistics.median([r["drain_s"] for r in g])
                if rr_:
                    rrps = statistics.median([r["readers"]["result"]["rps"] for r in g])
                    hr = statistics.median([r["readers"]["result"]["hits"] / max(1, r["readers"]["result"]["gets"]) * 100 for r in g])
                    rcell, hcell = fmt_k(rrps), f"{hr:.1f}%"
                else:
                    rcell = hcell = "-"
                md.append(f"| {pl} | {'yes' if rr_ else 'no'} | {len(g)} | {fmt_k(wrps)} | {med:,.0f} / {p95:,.0f} / {p99:,.0f} / {mx:,} | {lms99:,.0f} / {lmsmax:,.0f} | {drain:.2f} | {rcell} | {hcell} | {sum(1 for r in g if r.get('cuts'))} of {len(g)} |")
        md.append("")
    fo = [r for r in rows if r.get("suite") == "failover"]
    if fo:
        md.append("## Failover: kill -9 of the primary under write traffic through the router\n")
        md.append("| configuration | kills | first error after kill (s) | writes possible again (s), median [min..max] | acknowledged writes | acknowledged writes MISSING | old primary rejoined as synced replica (s) |")
        md.append("|---|---:|---:|---|---:|---:|---:|")
        for cfg in FAILOVER_CONFIGS:
            g = [r for r in fo if r["config"] == cfg["name"]]
            if not g:
                continue
            w = [r["writes_possible_again_s"] for r in g if r["writes_possible_again_s"] is not None]
            fe = [r["first_error_after_kill_s"] for r in g if r["first_error_after_kill_s"] is not None]
            rj = [r["old_primary_rejoin_s"] for r in g if r["old_primary_rejoin_s"] is not None]
            md.append(f"| {cfg['name']} | {len(g)} ({sum(1 for r in g if r['promoted_ok'])} promoted) | {statistics.median(fe):.2f} | "
                      f"{statistics.median(w):.2f} [{min(w):.2f}..{max(w):.2f}] | {sum(r['acked'] for r in g)} | **{sum(r['acked_missing'] for r in g)}** | "
                      f"{statistics.median(rj) if rj else float('nan'):.2f} |")
        md.append("")
    rz = [r for r in rows if r.get("suite") == "resync"]
    if rz:
        md.append("## Resynchronisation\n")
        md.append("| kind | detail | runs | seconds, median [min..max] | rate |")
        md.append("|---|---|---:|---|---|")
        for keys in sorted({r["keys"] for r in rz if r["kind"] == "full"}):
            g = [r for r in rz if r["kind"] == "full" and r["keys"] == keys]
            sec = [r["seconds"] for r in g]
            md.append(f"| full resync | {keys:,} keys (64 B values) | {len(g)} | {statistics.median(sec):.2f} [{min(sec):.2f}..{max(sec):.2f}] | {keys / statistics.median(sec):,.0f} keys/s |")
        g = [r for r in rz if r["kind"] == "partial-under-load"]
        if g:
            rec = [r["recovered_online_s"] for r in g if r["recovered_online_s"] is not None]
            md.append(f"| link drop under sustained writes (REPLICAOF to the same primary) | partial resyncs {sorted({r['partial_syncs'] for r in g})}, full {sorted({r['full_syncs'] for r in g})}, max lag {max(r['max_lag_records'] for r in g):,} records | {len(g)} | "
                      f"{statistics.median(rec):.2f} [{min(rec):.2f}..{max(rec):.2f}] to ONLINE again | - |")
        md.append("")
    open(os.path.join(out, "report.md"), "w").write("\n".join(md))
    return "\n".join(md)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--suite", default="all", choices=["overhead", "sweeps", "lag", "failover", "resync", "all"])
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--duration", type=float, default=8)
    ap.add_argument("--warmup", type=float, default=1.5)
    ap.add_argument("--lag-duration", type=float, default=15)
    ap.add_argument("--clients", type=int, default=50)
    ap.add_argument("--keyspace", type=int, default=100000)
    ap.add_argument("--settle", type=float, default=2.0)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--loadgen-cpus", default=LOADGEN_CPUS)
    ap.add_argument("--loadgen-threads", type=int, default=4)
    ap.add_argument("--loadgen", default=os.path.join(BUILD, "distcache_loadgen"))
    ap.add_argument("--only", default="", help="substring filter on topology names (overhead/sweeps)")
    ap.add_argument("--resync-sizes", default="100000,500000,1000000")
    ap.add_argument("--out", default=os.path.join(ROOT, "results", "repl"))
    ap.add_argument("--no-health-probe", action="store_true")
    args = ap.parse_args()

    for p in (os.path.join(BUILD, "distcache"), os.path.join(BUILD, "distcache_router"), args.loadgen):
        if not os.path.exists(p):
            sys.exit(f"missing {p} (build first)")
    os.makedirs(args.out, exist_ok=True)
    ns = types.SimpleNamespace(data_root=os.path.expanduser("~/.distcache-data"), server_cpus=PRIMARY_CPUS, loadgen_cpus=args.loadgen_cpus,
                               duration=args.duration, warmup=args.warmup, repeats=args.repeats)
    env = rs.capture_env(ns, {"distcache": os.path.join(BUILD, "distcache"), "router": os.path.join(BUILD, "distcache_router")}, args.loadgen)
    env["cpus"] = dict(primary=PRIMARY_CPUS, replica=REPLICA_CPUS, router=ROUTER_CPUS, loadgen=args.loadgen_cpus)
    health = {}
    probe = os.path.join(ROOT, "scripts", "health_probe.sh")
    if os.path.exists(probe) and not args.no_health_probe:
        r = subprocess.run([probe], capture_output=True, text=True)
        health["start"] = dict(exit_code=r.returncode, output=r.stdout.strip())
        print(r.stdout.strip(), flush=True)
    env["health"] = health
    json.dump(env, open(os.path.join(args.out, "env.json"), "w"), indent=2)

    work = []
    suites = ["overhead", "sweeps", "lag", "failover", "resync"] if args.suite == "all" else [args.suite]
    for s in suites:
        if s == "overhead":
            for t in TOPOLOGIES:
                if args.only and args.only not in t["name"]:
                    continue
                for wl in WORKLOADS:
                    work.append(("overhead", lambda t=t, wl=wl: run_overhead_one(args, t, wl), f"{t['name']} | {wl[0]}"))
        elif s == "sweeps":
            for t in SWEEP_TOPOS:
                if args.only and args.only not in t["name"]:
                    continue
                for c in (1, 4, 16, 50, 256):
                    work.append(("sweeps", lambda t=t, c=c: run_sweep_one(args, t, c, 1), f"{t['name']} | c{c} P1"))
                for p in (4, 16, 64):
                    work.append(("sweeps", lambda t=t, p=p: run_sweep_one(args, t, args.clients, p), f"{t['name']} | c{args.clients} P{p}"))
        elif s == "lag":
            for pl in (1, 16, 64):
                for rr_ in (False, True):
                    work.append(("lag", lambda pl=pl, rr_=rr_: run_lag_one(args, pl, rr_), f"lag P{pl} reads={rr_}"))
        elif s == "failover":
            for cfg in FAILOVER_CONFIGS:
                work.append(("failover", lambda cfg=cfg: run_failover_one(args, cfg), cfg["name"]))
        elif s == "resync":
            for n in [int(x) for x in args.resync_sizes.split(",") if x]:
                work.append(("resync", lambda n=n: run_resync_full(args, n), f"full resync {n}"))
            work.append(("resync", lambda: run_resync_partial(args), "partial resync under load"))

    rng = random.Random(args.seed)
    raw_path = os.path.join(args.out, "raw.jsonl")
    total = len(work) * args.repeats
    done = 0
    with open(raw_path, "a") as raw:
        for rep in range(1, args.repeats + 1):
            order = work[:]
            rng.shuffle(order)
            for kind, fn, label in order:
                try:
                    rec = fn()
                    rec.update(repeat=rep, ts=time.strftime("%Y-%m-%dT%H:%M:%S"))
                except Exception as ex:  # keep going; the failure is recorded
                    rec = dict(suite=kind, label=label, repeat=rep, error=repr(ex))
                raw.write(json.dumps(rec) + "\n")
                raw.flush()
                done += 1
                rps = rec.get("result", {}).get("result", {}).get("rps") if isinstance(rec.get("result"), dict) else None
                print(f"[{done}/{total}] {kind:9} {label:55} rep{rep} " + (f"{rps:,.0f} req/s" if rps else ("ERROR " + rec["error"] if "error" in rec else "ok")), flush=True)

    if not args.no_health_probe and os.path.exists(probe):
        r = subprocess.run([probe], capture_output=True, text=True)
        health["end"] = dict(exit_code=r.returncode, output=r.stdout.strip())
        env["health"] = health
        json.dump(env, open(os.path.join(args.out, "env.json"), "w"), indent=2)
    rows = [json.loads(l) for l in open(raw_path) if l.strip()]
    errors = [r for r in rows if "error" in r]
    rows = [r for r in rows if "error" not in r]
    text = report(rows, args.out, env, args)
    if errors:
        text += "\n\n## Failed runs\n" + "\n".join(f"- {e.get('suite')} {e.get('label')} rep{e.get('repeat')}: {e['error']}" for e in errors)
        open(os.path.join(args.out, "report.md"), "w").write(text)
    print(text)


if __name__ == "__main__":
    main()
