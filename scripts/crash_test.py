#!/usr/bin/env python3
"""Crash test: kill -9 the real server mid-write, restart it, and audit what survived.

  scripts/crash_test.py [--policy always|everysec|no] [--rounds N] [--writers N] [--snapshot-mb N]

Each round:
  1. start the server on a data directory that persists across rounds
  2. N writer connections SET unique keys sequentially, recording every ACKNOWLEDGED write
  3. after a random 0.3-1.2 s, SIGKILL the server (no shutdown code runs at all)
  4. restart it (recovery) and look up every acknowledged key

Reported per policy:
  lost_acked   acknowledged writes that did not survive. fsync=always must be 0.
  holes        a missing key with a LATER key present. Recovery yields a prefix of the log,
               so this must always be 0 for every policy.
  corrupt      a recovered key whose value is wrong (must always be 0)

HONEST LIMIT: SIGKILL kills the process but the operating system survives, so data already
handed to write() is not lost. This test therefore measures the PROCESS-crash window (data
still inside the server's own buffers). It cannot simulate power loss; surviving that is what
fdatasync is for, and it is covered by the unit tests, not by this script.
"""
import argparse, os, random, shutil, signal, socket, subprocess, sys, tempfile, threading, time


def encode(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            d = self.s.recv(65536)
            if not d:
                raise ConnectionError("closed")
            self.buf += d
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def reply(self):
        line = self._line()
        if line[:1] == b"$":
            n = int(line[1:])
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                d = self.s.recv(65536)
                if not d:
                    raise ConnectionError("closed")
                self.buf += d
            v, self.buf = self.buf[:n], self.buf[n + 2:]
            return v
        return line

    def cmd(self, *args):
        self.s.sendall(encode(*args))
        return self.reply()

    def pipeline_get(self, keys):
        self.s.sendall(b"".join(encode("GET", k) for k in keys))
        return [self.reply() for _ in keys]


def start_server(binary, port, data_dir, policy, snapshot_mb, log):
    p = subprocess.Popen(
        [binary, "--port", str(port), "--threads", "4", "--mode", "sharded", "--dir", data_dir,
         "--fsync", policy, "--snapshot-mb", str(snapshot_mb)],
        stdout=log, stderr=log)
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return p
        except OSError:
            if p.poll() is not None:
                raise RuntimeError("server exited during startup; see log")
            time.sleep(0.05)
    raise RuntimeError("server did not start")


def run_policy(binary, policy, rounds, writers, snapshot_mb, seed, data_root):
    rnd = random.Random(seed)
    os.makedirs(data_root, exist_ok=True)
    data_dir = tempfile.mkdtemp(prefix="crash-", dir=data_root)  # a real disk, not a RAM-backed /tmp
    log_path = data_dir + "-server.log"
    log = open(log_path, "wb")
    port = 17000 + rnd.randrange(2000)
    total_acked = total_lost = max_lost = holes = rounds_with_loss = corrupt = 0
    try:
        for r in range(rounds):
            srv = start_server(binary, port, data_dir, policy, snapshot_mb, log)
            acked = [0] * writers
            stop = threading.Event()

            def writer(t):
                try:
                    c = Conn(port)
                    i = 0
                    while not stop.is_set():
                        if c.cmd("SET", f"r{r}w{t}:{i}", f"value-{r}-{t}-{i}") == b"+OK":
                            acked[t] = i + 1
                        i += 1
                except Exception:
                    pass  # the server was killed under us: expected

            ts = [threading.Thread(target=writer, args=(t,)) for t in range(writers)]
            for th in ts:
                th.start()
            time.sleep(rnd.uniform(0.3, 1.2))
            srv.send_signal(signal.SIGKILL)   # no flush, no cleanup, no destructor
            srv.wait()
            stop.set()
            for th in ts:
                th.join()

            srv = start_server(binary, port, data_dir, policy, snapshot_mb, log)
            c = Conn(port)
            round_lost = 0
            for t in range(writers):
                n = acked[t]
                keys = [f"r{r}w{t}:{i}" for i in range(n + 20)]  # + a few in-flight keys beyond the last ack
                vals = []
                for off in range(0, len(keys), 500):
                    vals += c.pipeline_get(keys[off:off + 500])
                present = [v is not None for v in vals]
                corrupt += sum(1 for i, v in enumerate(vals) if v is not None and v != f"value-{r}-{t}-{i}".encode())
                lost = sum(1 for i in range(n) if not present[i])
                round_lost += lost
                first_missing = next((i for i, p in enumerate(present) if not p), None)
                if first_missing is not None and any(present[first_missing:]):
                    holes += 1  # a later write survived while an earlier one did not: log not a prefix
                total_acked += n
            total_lost += round_lost
            max_lost = max(max_lost, round_lost)
            rounds_with_loss += 1 if round_lost else 0
            srv.send_signal(signal.SIGTERM)  # clean stop before the next round's kill
            srv.wait()
    finally:
        log.close()
        shutil.rmtree(data_dir, ignore_errors=True)
        try:
            os.unlink(log_path)
        except OSError:
            pass
    return dict(policy=policy, rounds=rounds, acked=total_acked, lost=total_lost, max_lost=max_lost,
                rounds_with_loss=rounds_with_loss, holes=holes, corrupt=corrupt)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default="./build/distcache")
    ap.add_argument("--policy", choices=["always", "everysec", "no", "all"], default="all")
    ap.add_argument("--rounds", type=int, default=20)
    ap.add_argument("--writers", type=int, default=4)
    ap.add_argument("--snapshot-mb", type=int, default=64)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--data-root", default=os.path.expanduser("~/.distcache-data"),
                    help="directory (on a real disk, not tmpfs) for the server's data dirs")
    a = ap.parse_args()
    policies = ["always", "everysec", "no"] if a.policy == "all" else [a.policy]
    print(f"{'policy':<10} {'snap_mb':>7} {'rounds':>6} {'acked writes':>13} {'lost acked':>11} {'worst round':>12} "
          f"{'rounds w/ loss':>14} {'holes':>6} {'corrupt':>8}")
    bad = False
    for p in policies:
        res = run_policy(a.binary, p, a.rounds, a.writers, a.snapshot_mb, a.seed, a.data_root)
        print(f"{res['policy']:<10} {a.snapshot_mb:>7} {res['rounds']:>6} {res['acked']:>13} {res['lost']:>11} "
              f"{res['max_lost']:>12} {res['rounds_with_loss']:>14} {res['holes']:>6} {res['corrupt']:>8}", flush=True)
        if res["holes"] or res["corrupt"] or (p == "always" and res["lost"]):
            bad = True
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
