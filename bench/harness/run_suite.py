#!/usr/bin/env python3
"""DistCache benchmark orchestrator.

    bench/harness/run_suite.py --suite core --repeats 3 --out results/run-a
    bench/harness/run_suite.py --list

What it does, per measurement:
  1. starts a FRESH server (pinned to --server-cpus) with the variant's flags and, if the
     variant is persistent, an empty data directory on a real disk;
  2. runs distcache_loadgen (pinned to --loadgen-cpus) for warmup + duration seconds;
  3. records the loadgen JSON, the server's per-thread CPU use over the run, its RSS and the
     exact command lines, then stops the server and deletes its data.

Design rules that make results comparable:
  * "One change at a time": every experiment varies exactly ONE factor against a fixed baseline.
  * Repeats are INTERLEAVED (repeat 1 of every variant, then repeat 2, ...) so slow drift in
    the machine hits all variants alike instead of biasing whichever ran last.
  * Variants may point at different server binaries, so before/after comparisons of a code
    change run side by side in the same session (see --binary-baseline).
  * Everything needed to reproduce a number is stored next to it in raw.jsonl; env.json holds
    the machine and build description.
"""
import argparse, datetime, hashlib, json, os, platform, random, shutil, signal, socket, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))

# ---- experiment definitions -------------------------------------------------------------
# A variant is (name, server overrides, load overrides). Baseline server: sharded engine, 4
# workers, 64 shards, no persistence, unlimited capacity. Baseline load: 50 clients, pipeline
# 1, 100k uniform keys, 64-byte values, 80% GET / 20% SET, no TTL.
SERVER_BASE = dict(mode="sharded", threads=4, shards=64, max_keys=0, persist=False, fsync="everysec",
                   snapshot_mb=0, binary="current", env={}, data_root=None)
LOAD_BASE = dict(clients=50, pipeline=1, keyspace=100000, value_size=64, get_ratio=0.8, ttl_ms=0,
                 key_dist="uniform")


def V(name, server=None, load=None, group="", base=False):
    """group: variants compared with each other; base: the reference variant of its group."""
    return dict(name=name, server=server or {}, load=load or {}, group=group, base=base)


def experiments(only_wal_binaries=False):
    E = []
    # E1: what a request costs by type
    E.append(dict(id="E1", title="Workload mix", factor="GET/SET ratio",
                  variants=[V(f"{n}, P{p}", load=dict(get_ratio=r, pipeline=p), group=f"P{p}", base=(n == "80% GET"))
                            for p in (1, 16)
                            for n, r in (("SET only", 0.0), ("80% GET", 0.8), ("50% GET", 0.5), ("GET only", 1.0))]))
    # E2: client concurrency
    E.append(dict(id="E2", title="Client count", factor="connections",
                  variants=[V(f"{c} clients, P{p}", load=dict(clients=c, pipeline=p), group=f"P{p}", base=(c == 50))
                            for p in (1, 16) for c in (1, 4, 16, 50, 64, 256)]))
    # E3: pipeline depth
    E.append(dict(id="E3", title="Pipeline depth", factor="commands per batch",
                  variants=[V(f"P{p}", load=dict(pipeline=p), group="depth", base=(p == 1)) for p in (1, 2, 4, 16, 64)]))
    # E4: concurrency design
    E.append(dict(id="E4", title="Concurrency design", factor="engine mode / worker threads",
                  variants=[V("single (1 thread, no locks)", server=dict(mode="single", threads=1), load=dict(pipeline=16), group="design"),
                            V("global mutex, 4 workers", server=dict(mode="global", threads=4), load=dict(pipeline=16), group="design"),
                            V("sharded, 1 worker", server=dict(threads=1), load=dict(pipeline=16), group="design"),
                            V("sharded, 2 workers", server=dict(threads=2), load=dict(pipeline=16), group="design"),
                            V("sharded, 4 workers", server=dict(threads=4), load=dict(pipeline=16), group="design", base=True),
                            V("sharded, 8 workers", server=dict(threads=8), load=dict(pipeline=16), group="design")]))
    # E5: TTL
    E.append(dict(id="E5", title="TTL cost", factor="SET carries PX",
                  variants=[V("no TTL", load=dict(pipeline=16, get_ratio=0.5), group="ttl", base=True),
                            V("TTL 60 s (never fires)", load=dict(pipeline=16, get_ratio=0.5, ttl_ms=60000), group="ttl"),
                            V("TTL 200 ms (constant expiry churn)", load=dict(pipeline=16, get_ratio=0.5, ttl_ms=200), group="ttl")]))
    # E6: LRU eviction
    E.append(dict(id="E6", title="LRU eviction cost", factor="capacity vs 100k keyspace",
                  variants=[V("unlimited", load=dict(pipeline=16, get_ratio=0.5), group="lru", base=True),
                            V("capacity 200k (no eviction)", server=dict(max_keys=200000), load=dict(pipeline=16, get_ratio=0.5), group="lru"),
                            V("capacity 50k, uniform", server=dict(max_keys=50000), load=dict(pipeline=16, get_ratio=0.5), group="lru"),
                            V("capacity 10k, uniform", server=dict(max_keys=10000), load=dict(pipeline=16, get_ratio=0.5), group="lru"),
                            V("capacity 50k, zipf 0.99", server=dict(max_keys=50000), load=dict(pipeline=16, get_ratio=0.5, key_dist="zipf"), group="lru")]))
    # E7: WAL durability policy (the central persistence experiment)
    wal = []
    for mix_name, ratio in (("SET only", 0.0), ("80% GET", 0.8)):
        for p in (1, 16):
            grp = f"{mix_name}, P{p}"
            wal.append(V(f"{grp}: no persistence", load=dict(get_ratio=ratio, pipeline=p), group=grp, base=True))
            for pol in ("no", "everysec", "always"):
                wal.append(V(f"{grp}: fsync {pol}", server=dict(persist=True, fsync=pol),
                             load=dict(get_ratio=ratio, pipeline=p), group=grp))
    E.append(dict(id="E7", title="WAL durability policy", factor="--fsync", variants=wal))

    # W1: where does the logging overhead go? Uses the DIAGNOSTICS build (timers + profiler), so
    # these runs attribute time; they are not headline throughput numbers. Run with:
    #   --suite W1 --binary diag=build/distcache_diag
    g = "SET only, P16, fsync no"
    ld = dict(get_ratio=0.0, pipeline=16)
    E.append(dict(id="W1", title="WAL overhead attribution (diagnostics build)", factor="what the log writes to / does",
                  variants=[V("no persistence", load=ld, group=g, base=True),
                            V("log to the real disk", server=dict(persist=True, fsync="no"), load=ld, group=g),
                            V("log, flusher discards data (sink=null)", server=dict(persist=True, fsync="no", env={"DISTCACHE_WAL_SINK": "null"}),
                              load=ld, group=g),
                            V("log to tmpfs (/tmp)", server=dict(persist=True, fsync="no", data_root="/tmp"), load=ld, group=g)]))
    return E


def apply_baseline(exp_variant):
    s = dict(SERVER_BASE); s.update(exp_variant["server"])
    l = dict(LOAD_BASE); l.update(exp_variant["load"])
    return s, l


# ---- helpers ------------------------------------------------------------------------------

def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=20).stdout.strip()
    except Exception as e:  # pragma: no cover
        return f"<error: {e}>"


def capture_env(args, binaries, loadgen):
    flags = sh(f"grep -m1 'FLAGS = ' {ROOT}/build/build.ninja | head -1")
    return {
        "captured_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "git_commit": sh(f"git -C {ROOT} rev-parse --short HEAD"),
        "git_dirty": bool(sh(f"git -C {ROOT} status --porcelain")),
        "kernel": platform.release(),
        "cpu_model": sh("lscpu | awk -F: '/Model name/{gsub(/^ +/,\"\",$2); print $2}'"),
        "logical_cpus": os.cpu_count(),
        "cpu_topology_siblings": sh("cat /sys/devices/system/cpu/cpu0/topology/thread_siblings_list"),
        "mem_total_mb": sh("awk '/MemTotal/{print int($2/1024)}' /proc/meminfo"),
        "hypervisor": sh("lscpu | awk -F: '/Hypervisor vendor/{gsub(/^ +/,\"\",$2); print $2}'"),
        "compiler": sh("g++ --version | head -1"),
        "cmake_build_type": sh(f"grep CMAKE_BUILD_TYPE {ROOT}/build/CMakeCache.txt | head -1") + " (CMakeLists defaults to Release)",
        "compile_flags_sample": sh(f"ninja -C {ROOT}/build -t commands distcache 2>/dev/null | grep -m1 'src/main.cpp' | grep -o '\\-O[0-3s] \\|\\-DNDEBUG\\|-std=[a-z+0-9]*' | tr '\\n' ' '"),
        "data_dir_filesystem": sh(f"stat -f -c %T {args.data_root}"),
        "dirty_ratio": sh("cat /proc/sys/vm/dirty_ratio"),
        "dirty_background_ratio": sh("cat /proc/sys/vm/dirty_background_ratio"),
        "server_cpus": args.server_cpus,
        "loadgen_cpus": args.loadgen_cpus,
        "duration_s": args.duration,
        "warmup_s": args.warmup,
        "repeats": args.repeats,
        "binaries": {k: {"path": v, "sha256_16": sha256(v)} for k, v in binaries.items()},
        "loadgen": {"path": loadgen, "sha256_16": sha256(loadgen)},
        "command_line": " ".join(sys.argv),
    }


def wait_ready(port, proc, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        if proc.poll() is not None:
            raise RuntimeError("server exited during startup")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5) as s:
                s.sendall(b"*1\r\n$4\r\nPING\r\n")
                if s.recv(16).startswith(b"+PONG"):
                    return
        except OSError:
            pass
        time.sleep(0.02)
    raise RuntimeError("server did not become ready")


def thread_cpu(pid):
    """{thread-name: cpu_seconds} summed over threads with that name, from /proc."""
    hz = os.sysconf("SC_CLK_TCK")
    out = {}
    try:
        for tid in os.listdir(f"/proc/{pid}/task"):
            try:
                with open(f"/proc/{pid}/task/{tid}/comm") as f:
                    name = f.read().strip()
                with open(f"/proc/{pid}/task/{tid}/stat") as f:
                    parts = f.read().rsplit(")", 1)[1].split()
                ticks = int(parts[11]) + int(parts[12])  # utime + stime
                out[name] = out.get(name, 0.0) + ticks / hz
            except (FileNotFoundError, ProcessLookupError, IndexError):
                continue
    except FileNotFoundError:
        pass
    return out


def rss_mb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except FileNotFoundError:
        pass
    return None


class Runner:
    def __init__(self, args, binaries, loadgen):
        self.a = args
        self.bins = binaries
        self.loadgen = loadgen
        self.port = args.base_port

    def run_once(self, server, load, label):
        self.port += 1
        port = self.port
        binary = self.bins[server["binary"]]
        data_dir = None
        scmd = []
        if self.a.server_cpus:
            scmd += ["taskset", "-c", self.a.server_cpus]
        scmd += [binary, "--port", str(port), "--mode", server["mode"], "--threads", str(server["threads"]),
                 "--shards", str(server["shards"])]
        if server["max_keys"]:
            scmd += ["--max-keys", str(server["max_keys"])]
        if server["persist"]:
            data_dir = tempfile.mkdtemp(prefix="bench-", dir=server.get("data_root") or self.a.data_root)
            scmd += ["--dir", data_dir, "--fsync", server["fsync"], "--snapshot-mb", str(server["snapshot_mb"])]
        log = tempfile.TemporaryFile()
        env = dict(os.environ)
        env.update(server.get("env") or {})
        if self.a.profile_dir:
            os.makedirs(self.a.profile_dir, exist_ok=True)
            safe = "".join(ch if ch.isalnum() else "_" for ch in label)[:80]
            env["DISTCACHE_PROFILE"] = os.path.join(self.a.profile_dir, f"{safe}-{self.port}.txt")
        proc = subprocess.Popen(scmd, stdout=log, stderr=log, env=env)
        try:
            wait_ready(port, proc)
            lcmd = []
            if self.a.loadgen_cpus:
                lcmd += ["taskset", "-c", self.a.loadgen_cpus]
            lcmd += [self.loadgen, "--port", str(port), "--clients", str(load["clients"]), "--threads", str(self.a.loadgen_threads),
                     "--pipeline", str(load["pipeline"]), "--duration", str(self.a.duration), "--warmup", str(self.a.warmup),
                     "--keyspace", str(load["keyspace"]), "--value-size", str(load["value_size"]),
                     "--get-ratio", str(load["get_ratio"]), "--ttl-ms", str(load["ttl_ms"]),
                     "--key-dist", load["key_dist"], "--label", label, "--seed", str(self.a.seed)]
            if load["get_ratio"] > 0 or server["max_keys"]:
                lcmd.append("--preload")
            cpu0 = thread_cpu(proc.pid)
            t0 = time.time()
            r = subprocess.run(lcmd, capture_output=True, text=True, timeout=self.a.duration + self.a.warmup + 600)
            wall = time.time() - t0
            cpu1 = thread_cpu(proc.pid)
            rss = rss_mb(proc.pid)
            if r.returncode not in (0, 3) or not r.stdout.strip():
                raise RuntimeError(f"loadgen failed rc={r.returncode}: {r.stderr[-300:]}")
            res = json.loads(r.stdout.strip().splitlines()[-1])
            cpu = {k: round(cpu1.get(k, 0) - cpu0.get(k, 0), 3) for k in cpu1}
            return dict(server_cmd=" ".join(scmd), load_cmd=" ".join(lcmd), loadgen_rc=r.returncode, wall_s=round(wall, 2),
                        server_cpu_seconds=cpu, server_rss_mb=round(rss, 1) if rss else None, result=res)
        finally:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            log.close()
            if data_dir:
                shutil.rmtree(data_dir, ignore_errors=True)
            # Settle: flush whatever the kernel still has dirty and let background writeback finish, so
            # the next measurement does not inherit this run's disk activity (kernel flusher threads
            # are not pinned and can steal CPU from a pinned server).
            if self.a.settle > 0:
                os.sync()
                time.sleep(self.a.settle)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--suite", default="all", help="comma list of experiment ids (E1..E7) or 'all'")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--duration", type=float, default=8)
    ap.add_argument("--warmup", type=float, default=1.5)
    ap.add_argument("--out", default=os.path.join(ROOT, "results", "run"))
    ap.add_argument("--server-cpus", default="0-5", help="taskset list for the server ('' = no pinning)")
    ap.add_argument("--loadgen-cpus", default="6-11")
    ap.add_argument("--loadgen-threads", type=int, default=4)
    ap.add_argument("--data-root", default=os.path.expanduser("~/.distcache-data"))
    ap.add_argument("--binary", action="append", default=[], metavar="NAME=PATH",
                    help="server binary; repeatable. The FIRST is the primary and runs every variant; the others run only "
                         "the variants that use persistence, interleaved in the same session. Default: current=build/distcache")
    ap.add_argument("--loadgen", default=os.path.join(ROOT, "build", "distcache_loadgen"))
    ap.add_argument("--base-port", type=int, default=18000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--settle", type=float, default=2.0,
                    help="seconds to wait after a sync() between runs, so writeback from one run does not bleed into the next (0 = off)")
    ap.add_argument("--variant-filter", default="", help="only variants whose name contains this text (';' separates alternatives)")
    ap.add_argument("--profile-dir", default="", help="diagnostics build only: write sampling-profiler output here")
    ap.add_argument("--no-health-probe", action="store_true", help="skip the machine health probe at start/end")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()

    exps = experiments()
    if args.list:
        for e in exps:
            print(f"{e['id']}  {e['title']}  (varies: {e['factor']}, {len(e['variants'])} variants)")
            for v in e["variants"]:
                print(f"      - {v['name']}")
        return
    wanted = None if args.suite == "all" else set(args.suite.split(","))
    # "all" means the standard experiments (E*); investigation experiments (W*) are opt-in
    exps = [e for e in exps if (wanted is None and e["id"].startswith("E")) or (wanted and e["id"] in wanted)]

    binaries = {}
    for spec in args.binary or ["current=" + os.path.join(ROOT, "build", "distcache")]:
        name, _, path = spec.partition("=")
        if not path:
            name, path = "current", spec
        binaries[name] = path
    primary = next(iter(binaries))
    for p in list(binaries.values()) + [args.loadgen]:
        if not os.path.exists(p):
            sys.exit(f"missing binary: {p} (build first)")
    os.makedirs(args.data_root, exist_ok=True)
    if sh(f"stat -f -c %T {args.data_root}") in ("tmpfs", "ramfs"):
        sys.exit(f"--data-root {args.data_root} is RAM-backed: fsync would be a no-op. Use a real disk.")
    os.makedirs(args.out, exist_ok=True)
    env = capture_env(args, binaries, args.loadgen)
    # Machine health at the start and end of the session (see scripts/health_probe.sh). A degraded
    # machine makes persistent-configuration results incomparable with healthy-state results.
    probe = os.path.join(ROOT, "scripts", "health_probe.sh")
    health = {}
    if os.path.exists(probe) and not args.no_health_probe:
        r = subprocess.run([probe], capture_output=True, text=True)
        health["start"] = dict(exit_code=r.returncode, output=r.stdout.strip())
        print(r.stdout.strip(), flush=True)
        if r.returncode == 3:
            print("WARNING: machine is in a degraded state; results are tagged and not comparable with healthy sessions", flush=True)
    env["health"] = health
    with open(os.path.join(args.out, "env.json"), "w") as f:
        json.dump(env, f, indent=2)

    # expand: with a baseline binary, every variant is also run on it (so a code change is
    # compared side by side in the same session)
    work = []
    wanted_names = [t for t in args.variant_filter.split(";") if t]  # ';' because variant names contain ','
    for e in exps:
        for v in e["variants"]:
            if wanted_names and not any(t in v["name"] for t in wanted_names):
                continue
            work.append((e, v, primary))
            # the other binaries are only interesting where persistence is involved
            if apply_baseline(v)[0]["persist"]:
                for other in list(binaries)[1:]:
                    work.append((e, v, other))
    runner = Runner(args, binaries, args.loadgen)
    rng = random.Random(args.seed)
    total = len(work) * args.repeats
    done = 0
    t_start = time.time()
    raw_path = os.path.join(args.out, "raw.jsonl")
    with open(raw_path, "a") as raw:
        for rep in range(1, args.repeats + 1):
            order = work[:]
            rng.shuffle(order)           # random order inside a repeat, every variant once per repeat
            for e, v, which in order:
                server, load = apply_baseline(v)
                server["binary"] = which
                label = f"{e['id']}|{v['name']}|{which}"
                try:
                    rec = runner.run_once(server, load, label)
                    rec.update(primary_binary=primary, exp=e["id"], exp_title=e["title"], variant=v["name"], group=v.get("group", ""), base=v.get("base", False),
                               binary=which, repeat=rep,
                               server_cfg=server, load_cfg=load, ts=datetime.datetime.now().isoformat(timespec="seconds"))
                except Exception as ex:  # keep going; the failure is recorded
                    rec = dict(exp=e["id"], variant=v["name"], binary=which, repeat=rep, error=str(ex), server_cfg=server, load_cfg=load)
                raw.write(json.dumps(rec) + "\n")
                raw.flush()
                done += 1
                r = rec.get("result", {}).get("result", {})
                eta = (time.time() - t_start) / done * (total - done)
                print(f"[{done:3d}/{total}] rep{rep} {label:<70} "
                      f"{r.get('rps', float('nan')):>10.0f} rps  p99 {r.get('latency_us', {}).get('p99', float('nan')):>9.0f} us"
                      f"  (eta {eta/60:4.1f} min){'  ERROR ' + rec['error'] if 'error' in rec else ''}", flush=True)
    if os.path.exists(probe) and not args.no_health_probe:
        r = subprocess.run([probe], capture_output=True, text=True)
        health["end"] = dict(exit_code=r.returncode, output=r.stdout.strip())
        env["health"] = health
        with open(os.path.join(args.out, "env.json"), "w") as f:
            json.dump(env, f, indent=2)
        print(r.stdout.strip(), flush=True)
    print("wrote", raw_path)


if __name__ == "__main__":
    main()
