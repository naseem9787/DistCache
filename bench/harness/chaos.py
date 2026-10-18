#!/usr/bin/env python3
"""Chaos test for the whole system (Phase 10): real processes, persistence on, sync replication.

    bench/harness/chaos.py [--seconds 60] [--seed 1] [--out FILE.json]

Starts scripts/cluster.sh (2 shards x 1 primary + 2 replicas, --repl-sync 1, WAL on, one router), then runs
concurrent clients through the router while a chaos loop injects, at random:
    kill -9 of a primary, kill -9 of a replica (restarted a little later),
    SIGSTOP/SIGCONT of a primary (a frozen process that wakes up after it was replaced),
    kill -9 + restart of the router (it must rediscover epochs from the nodes and its state file).
At most ONE node of a shard is down at a time, because with --repl-sync 1 and two replicas the coordinator
promotes only when both replicas are reachable (R-N+1 = 2); more simultaneous failures make a shard
unavailable until a node returns, which is safe but would only stall the test.

The clients keep a model of everything the cluster ACKNOWLEDGED:
    SET (some with EX), DEL, and RL.ACQUIRE on 10 buckets (capacity 500, no meaningful refill).
After the chaos stops and every node is back, it VERIFIES:
    1. every acknowledged SET that was not later deleted is readable with the right value,
    2. every acknowledged DEL is still deleted (nothing resurrected by a WAL restart or an old primary),
    3. for each bucket the clients were told "allowed" at most `capacity` times (+ the refill the elapsed time allows),
    4. each shard has exactly one primary and its replicas converge to the same DBSIZE,
    5. a TTL key written before the chaos still has a sane TTL.
Exit status 0 = all hold. A violated check prints the evidence and exits 1. Errors during the chaos are expected
(unavailability windows, closed connections) and are counted, not failures: an unacknowledged write makes no promise.
"""
import argparse, json, os, random, re, signal, socket, subprocess, sys, threading, time

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
BASE = 7300
DC_HOME = os.path.expanduser("~/.distcache-chaos")
ENV = dict(os.environ, DC_HOME=DC_HOME, BASE_PORT=str(BASE))
NODES = {"a": ["a1", "a2", "a3"], "b": ["b1", "b2", "b3"]}
PORT = {"a1": BASE + 101, "a2": BASE + 102, "a3": BASE + 103, "b1": BASE + 201, "b2": BASE + 202, "b3": BASE + 203, "router": BASE}


def sh(*args, check=False):
    return subprocess.run([os.path.join(ROOT, "scripts", "cluster.sh"), *args], env=ENV, capture_output=True, text=True, check=check)


class RC:
    """A tiny RESP client with one persistent connection (reconnects on demand)."""

    def __init__(self, port, timeout=4.0):
        self.port, self.timeout, self.s, self.buf = port, timeout, None, b""

    def _fill(self):
        d = self.s.recv(65536)
        if not d:
            raise ConnectionError("closed")
        self.buf += d

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, _, self.buf = self.buf.partition(b"\r\n")
        return line

    def _read(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            return ("ERR", rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self._fill()
            v, self.buf = self.buf[:n], self.buf[n + 2:]
            return v.decode()
        if t == b"*":
            return [self._read() for _ in range(int(rest))]
        raise ValueError(line)

    def call(self, *args):
        try:
            if self.s is None:
                self.s = socket.create_connection(("127.0.0.1", self.port), timeout=self.timeout)
                self.buf = b""
            msg = f"*{len(args)}\r\n".encode() + b"".join(f"${len(str(a).encode())}\r\n{a}\r\n".encode() for a in args)
            self.s.sendall(msg)
            return self._read()
        except (OSError, ConnectionError):
            self.close()
            raise ConnectionError("connection failed")

    def close(self):
        if self.s:
            try:
                self.s.close()
            except OSError:
                pass
        self.s, self.buf = None, b""


def info(port):
    c = RC(port)
    try:
        txt = c.call("INFO")
    finally:
        c.close()
    d = {}
    for line in (txt or "").splitlines():
        if ":" in line and not line.startswith("#"):
            k, _, v = line.partition(":")
            d[k] = v
    return d


def shard_status():
    c = RC(PORT["router"])
    try:
        return c.call("SHARD", "STATUS") or ""
    except ConnectionError:
        return ""
    finally:
        c.close()


def primary_of(shard):
    m = re.search(rf"^shard:{shard} .*primary={shard}#(\d+)", shard_status(), re.M)
    return f"{shard}{int(m.group(1)) + 1}" if m else None


def pid_of(node):
    try:
        return int(open(os.path.join(DC_HOME, "pids", f"{node}.pid")).read())
    except (OSError, ValueError):
        return None


def all_healthy():
    return sh("health").returncode == 0


def fully_synced():
    """Every shard healthy AND every member up and synchronised. A node that just restarted is effectively
    still DOWN for failover purposes until it has resynchronised (an unsynced replica is not eligible), so the
    chaos loop only injects the next fault once this holds: one failure at a time, as documented."""
    if not all_healthy():
        return False
    st = shard_status()
    members = [l for l in st.splitlines() if l.strip().startswith("member:")]
    return len(members) == 6 and all("role=down" not in l and "synced=yes" in l for l in members)


def wait_healthy(timeout):
    end = time.time() + timeout
    while time.time() < end:
        if all_healthy():
            return True
        time.sleep(0.3)
    return False


# ---- clients -----------------------------------------------------------------------------------------------

class Model:
    def __init__(self):
        self.lock = threading.Lock()
        self.set_acked = {}      # key -> value, acknowledged and not later deleted
        self.del_acked = set()   # keys whose DEL was acknowledged (and not re-set afterwards)
        self.ttl_keys = {}       # key -> ttl seconds requested
        self.bucket_allowed = {f"chaos:rl:{i}": 0 for i in range(10)}
        self.errors = 0
        self.ops = 0


def writer(tid, model, stop, rng):
    c = RC(PORT["router"])
    i = 0
    mine = []
    while not stop.is_set():
        i += 1
        key, val = f"chaos:{tid}:{i}", f"v{i}-{rng.randrange(10**6)}"
        try:
            if i % 7 == 0:
                r = c.call("SET", key, val, "EX", 3600)
                ttl = 3600
            else:
                r = c.call("SET", key, val)
                ttl = None
            if r == "OK":
                with model.lock:
                    model.set_acked[key] = val
                    model.del_acked.discard(key)
                    if ttl:
                        model.ttl_keys[key] = ttl
                    model.ops += 1
                mine.append(key)
            else:
                with model.lock:
                    model.errors += 1
            if i % 11 == 0 and mine:
                victim = mine.pop(rng.randrange(len(mine)))
                r = c.call("DEL", victim)
                if isinstance(r, int):  # acknowledged: gone for good
                    with model.lock:
                        model.set_acked.pop(victim, None)
                        model.ttl_keys.pop(victim, None)
                        model.del_acked.add(victim)
                else:
                    with model.lock:
                        model.errors += 1
        except ConnectionError:
            with model.lock:
                model.errors += 1
            time.sleep(0.02)
        time.sleep(0.002)
    c.close()


def limiter(tid, model, stop, rng):
    c = RC(PORT["router"])
    keys = list(model.bucket_allowed)
    while not stop.is_set():
        k = rng.choice(keys)
        try:
            r = c.call("RL.ACQUIRE", k, 500, "0.001")
            if isinstance(r, list) and r and r[0] == 1:
                with model.lock:
                    model.bucket_allowed[k] += 1
            elif not isinstance(r, list):
                with model.lock:
                    model.errors += 1
        except ConnectionError:
            with model.lock:
                model.errors += 1
            time.sleep(0.02)
        time.sleep(0.001)
    c.close()


# ---- chaos -----------------------------------------------------------------------------------------------------------

def down_nodes():
    return [n for sh_nodes in NODES.values() for n in sh_nodes if pid_of(n) is None or not os.path.exists(f"/proc/{pid_of(n)}")]


def chaos(seconds, rng, log):
    end = time.time() + seconds
    pending_restart = []   # (when, node or 'router')
    frozen = []            # (when, pid)
    counts = {"kill_primary": 0, "kill_replica": 0, "freeze_primary": 0, "restart_router": 0}
    while time.time() < end:
        now = time.time()
        for item in list(pending_restart):
            if now >= item[0]:
                pending_restart.remove(item)
                sh("start", item[1])
                log(f"  restarted {item[1]}")
        for item in list(frozen):
            if now >= item[0]:
                frozen.remove(item)
                try:
                    os.kill(item[1], signal.SIGCONT)
                    log(f"  resumed pid {item[1]}")
                except OSError:
                    pass
        busy_shards = {n[0] for _, n in pending_restart if n != "router"} | {s for s in NODES if any(f for f in frozen)}
        ev = rng.choices(["kill_primary", "kill_replica", "freeze_primary", "restart_router", "idle"], [30, 30, 12, 12, 16])[0]
        shard = rng.choice(list(NODES))
        if ev in ("kill_primary", "kill_replica", "freeze_primary") and (shard in busy_shards or not fully_synced()):
            ev = "idle"
        if ev == "kill_primary" or ev == "freeze_primary":
            p = primary_of(shard)
            if p is None or not all_healthy():
                ev = "idle"
            elif ev == "kill_primary":
                sh("kill", p)
                pending_restart.append((time.time() + rng.uniform(1.0, 3.0), p))
                counts[ev] += 1
                log(f"kill -9 primary {p}")
            else:
                pid = pid_of(p)
                if pid:
                    os.kill(pid, signal.SIGSTOP)
                    frozen.append((time.time() + rng.uniform(2.0, 4.0), pid))
                    counts[ev] += 1
                    log(f"SIGSTOP primary {p} (pid {pid})")
        elif ev == "kill_replica":
            p = primary_of(shard)
            reps = [n for n in NODES[shard] if n != p]
            if p is None or not all_healthy():
                ev = "idle"
            else:
                n = rng.choice(reps)
                sh("kill", n)
                pending_restart.append((time.time() + rng.uniform(1.0, 3.0), n))
                counts[ev] += 1
                log(f"kill -9 replica {n}")
        elif ev == "restart_router":
            sh("kill", "router")
            time.sleep(rng.uniform(0.2, 1.0))
            sh("start", "router")
            counts[ev] += 1
            log("restarted the router")
        # let the cluster settle between events: failover takes ~1.5 s with the cluster.sh timings
        time.sleep(rng.uniform(2.5, 5.0))
    # finish: wake frozen processes and restart everything that is down
    for _, pid in frozen:
        try:
            os.kill(pid, signal.SIGCONT)
        except OSError:
            pass
    for _, n in pending_restart:
        sh("start", n)
    return counts


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seconds", type=float, default=60)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    rng = random.Random(args.seed)
    log = lambda m: print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

    sh("clean")
    r = sh("up")
    print(r.stdout.strip(), r.stderr.strip())
    if r.returncode != 0:
        sys.exit("cluster did not start")
    model = Model()
    c0 = RC(PORT["router"])
    assert c0.call("SET", "chaos:ttl-probe", "x", "EX", 7200) == "OK"
    c0.close()
    stop = threading.Event()
    threads = [threading.Thread(target=writer, args=(t, model, stop, random.Random(args.seed * 100 + t))) for t in range(4)]
    threads += [threading.Thread(target=limiter, args=(t, model, stop, random.Random(args.seed * 1000 + t))) for t in range(2)]
    t_start = time.time()
    for t in threads:
        t.start()
    counts = chaos(args.seconds, rng, log)
    log("chaos over; restarting everything and waiting for the cluster to heal")
    for n in [x for s in NODES.values() for x in s] + ["router"]:
        pid = pid_of(n)
        if pid is None or not os.path.exists(f"/proc/{pid}"):
            sh("start", n)
    healed = wait_healthy(90)
    time.sleep(2)
    stop.set()
    for t in threads:
        t.join()
    elapsed = time.time() - t_start
    log(f"cluster healthy: {healed}; {len(model.set_acked)} live acknowledged keys, {len(model.del_acked)} acknowledged deletes, "
        f"{model.errors} client errors during the chaos")

    problems = []
    if not healed:
        problems.append("cluster did not become healthy within 90 s after the chaos")
    # let replicas finish catching up before comparing them
    time.sleep(3)
    c = RC(PORT["router"])
    missing, wrong = [], []
    for k, v in list(model.set_acked.items()):
        got = None
        for _ in range(5):
            try:
                got = c.call("GET", k)
                if not isinstance(got, tuple):
                    break
            except ConnectionError:
                time.sleep(0.2)
        if got != v:
            (missing if got is None else wrong).append((k, v, got))
    resurrected = []
    for k in model.del_acked:
        got = "error"
        for _ in range(5):
            try:
                got = c.call("GET", k)
                if not isinstance(got, tuple):
                    break
            except ConnectionError:
                time.sleep(0.2)
        if isinstance(got, tuple) or got == "error":
            problems.append(f"could not read back {k}: {got!r}")
            break
        if got is not None:
            resurrected.append(k)
    if missing:
        problems.append(f"{len(missing)} acknowledged writes are MISSING, e.g. {missing[:3]}")
    if wrong:
        problems.append(f"{len(wrong)} acknowledged writes have a WRONG value, e.g. {wrong[:3]}")
    if resurrected:
        problems.append(f"{len(resurrected)} acknowledged deletes were RESURRECTED, e.g. {resurrected[:3]}")
    over = {k: n for k, n in model.bucket_allowed.items() if n > 500}
    if over:
        problems.append(f"rate-limit buckets granted more than their capacity (500): {over}")
    ttl = c.call("TTL", "chaos:ttl-probe")
    if not isinstance(ttl, int) or not (0 < ttl <= 7200):
        problems.append(f"TTL probe has TTL {ttl!r}")
    c.close()
    # topology: exactly one primary per shard, replicas converge
    topo = {}
    for shard, ns in NODES.items():
        roles = {}
        for n in ns:
            try:
                roles[n] = info(PORT[n])
            except OSError:
                roles[n] = {}
        masters = [n for n, i in roles.items() if i.get("role") == "master"]
        if len(masters) != 1:
            problems.append(f"shard {shard}: {len(masters)} nodes claim to be primary: {masters}")
        sizes = {}
        for n in ns:
            try:
                cc = RC(PORT[n])
                sizes[n] = cc.call("DBSIZE")
                cc.close()
            except ConnectionError:
                sizes[n] = None
        # replicas serve reads; give them a moment to apply the tail
        if len(set(sizes.values())) != 1:
            time.sleep(2)
            for n in ns:
                try:
                    cc = RC(PORT[n])
                    sizes[n] = cc.call("DBSIZE")
                    cc.close()
                except ConnectionError:
                    sizes[n] = None
        if len(set(sizes.values())) != 1:
            problems.append(f"shard {shard}: replicas have diverged: DBSIZE {sizes}")
        topo[shard] = dict(primaries=masters, dbsize=sizes)
    result = dict(seconds=args.seconds, seed=args.seed, elapsed_s=round(elapsed, 1), events=counts,
                  acked_live_keys=len(model.set_acked), acked_deletes=len(model.del_acked), ttl_keys=len(model.ttl_keys),
                  client_ops_acknowledged=model.ops, client_errors=model.errors,
                  bucket_allowed=model.bucket_allowed, topology=topo, healed=healed, problems=problems)
    print(json.dumps(result, indent=2))
    if args.out:
        json.dump(result, open(args.out, "w"), indent=2)
    sh("down")
    if problems:
        print("\nFAILED:\n  " + "\n  ".join(problems))
        sys.exit(1)
    print("\nOK: every acknowledged write survived, nothing was resurrected, no limit was exceeded, one primary per shard.")


if __name__ == "__main__":
    main()
