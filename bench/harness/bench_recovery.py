#!/usr/bin/env python3
"""Recovery-time benchmark: log replay vs snapshot load.

    bench/harness/bench_recovery.py --sizes 100000,500000,1000000 --repeats 5 --out results/recovery

For each dataset size and each of two history shapes, per repeat:
  1. start a fresh persistent server (fsync no, auto-snapshot off) and load the data with
     distcache_loadgen;
  2. "unique":  every key written exactly once        -> log records == keys
     "churn":   keys overwritten ~repeatedly for 5 s   -> log records >> keys
  3. stop it cleanly, then START IT AGAIN and time recovery two ways:
        external:  process start -> first PING answered (what a client waiting on a restart sees)
        reported:  INFO recovery_ms (the server's own timer around recovery only)
     this is "log only";
  4. SAVE (snapshot + compact the log), stop, start again, time recovery again: "snapshot".
  5. after every restart the key count is checked against the count before shutdown.

Limits: the page cache is WARM (nothing here drops it), so file reads are memory speed; a cold
start from a real disk would add read time proportional to the file sizes reported.
"""
import argparse, json, os, shutil, statistics, subprocess, sys, tempfile, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run_suite as rs  # noqa: E402  (shared helpers: env capture, readiness, process control)

ROOT = rs.ROOT


def redis_cli(port, *cmd):
    import socket
    s = socket.create_connection(("127.0.0.1", port), timeout=30)
    payload = f"*{len(cmd)}\r\n" + "".join(f"${len(c)}\r\n{c}\r\n" for c in cmd)
    s.sendall(payload.encode())
    buf = b""
    while b"\r\n" not in buf:
        buf += s.recv(65536)
    if buf[:1] == b"$":
        n = int(buf.split(b"\r\n", 1)[0][1:])
        body = buf.split(b"\r\n", 1)[1]
        while len(body) < n + 2:
            body += s.recv(65536)
        s.close()
        return body[:n].decode()
    s.close()
    return buf.split(b"\r\n", 1)[0].decode()


def info(port):
    out = {}
    for line in redis_cli(port, "INFO").splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            out[k] = v
    return out


def dir_bytes(d, prefix):
    return sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d) if f.startswith(prefix))


class Server:
    def __init__(self, binary, port, data_dir, cpus):
        self.binary, self.port, self.data_dir, self.cpus = binary, port, data_dir, cpus
        self.proc = None

    def start(self):
        cmd = (["taskset", "-c", self.cpus] if self.cpus else []) + [
            self.binary, "--port", str(self.port), "--mode", "sharded", "--threads", "4", "--shards", "64",
            "--dir", self.data_dir, "--fsync", "no", "--snapshot-mb", "0"]
        t0 = time.perf_counter()
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        rs.wait_ready(self.port, self.proc, timeout=300)
        return time.perf_counter() - t0

    def stop(self):
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
            self.proc = None


def one(args, size, shape, port):
    data_dir = tempfile.mkdtemp(prefix="recovery-", dir=args.data_root)
    srv = Server(args.binary, port, data_dir, args.server_cpus)
    try:
        srv.start()
        lcmd = (["taskset", "-c", args.loadgen_cpus] if args.loadgen_cpus else []) + [
            args.loadgen, "--port", str(port), "--clients", "50", "--threads", "4", "--pipeline", "16",
            "--keyspace", str(size), "--value-size", str(args.value_size), "--preload", "--warmup", "0",
            "--duration", "0.5" if shape == "unique" else "5", "--get-ratio", "1.0" if shape == "unique" else "0.0"]
        subprocess.run(lcmd, check=True, capture_output=True, text=True)
        before = info(port)
        keys_before = int(before["keys"])
        records = int(before.get("wal_records", 0))
        srv.stop()
        rec = dict(size=size, shape=shape, keys=keys_before, wal_records=records, value_size=args.value_size)
        rec["wal_bytes"] = dir_bytes(data_dir, "wal-")

        # --- log only
        ext = srv.start()
        i = info(port)
        rec["log_only"] = dict(external_s=round(ext, 3), reported_ms=int(i.get("recovery_ms", -1)),
                               keys_after=int(i["keys"]))
        # --- snapshot
        t0 = time.perf_counter()
        assert redis_cli(port, "SAVE") == "+OK"
        rec["save_s"] = round(time.perf_counter() - t0, 3)
        srv.stop()
        rec["snapshot_bytes"] = dir_bytes(data_dir, "snapshot-")
        rec["wal_bytes_after_save"] = dir_bytes(data_dir, "wal-")
        ext = srv.start()
        i = info(port)
        rec["snapshot"] = dict(external_s=round(ext, 3), reported_ms=int(i.get("recovery_ms", -1)),
                               keys_after=int(i["keys"]))
        rec["keys_match"] = rec["log_only"]["keys_after"] == keys_before == rec["snapshot"]["keys_after"]
        return rec
    finally:
        srv.stop()
        shutil.rmtree(data_dir, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sizes", default="100000,500000,1000000")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--value-size", type=int, default=64)
    ap.add_argument("--binary", default=os.path.join(ROOT, "build", "distcache"))
    ap.add_argument("--loadgen", default=os.path.join(ROOT, "build", "distcache_loadgen"))
    ap.add_argument("--server-cpus", default="0-5")
    ap.add_argument("--loadgen-cpus", default="6-11")
    ap.add_argument("--data-root", default=os.path.expanduser("~/.distcache-data"))
    ap.add_argument("--out", default=os.path.join(ROOT, "results", "recovery"))
    args = ap.parse_args()
    if rs.sh(f"stat -f -c %T {args.data_root}") in ("tmpfs", "ramfs"):
        sys.exit("--data-root is RAM-backed")
    os.makedirs(args.data_root, exist_ok=True)
    os.makedirs(args.out, exist_ok=True)
    sizes = [int(x) for x in args.sizes.split(",")]
    port = 19000
    results = []
    with open(os.path.join(args.out, "recovery.jsonl"), "w") as raw:
        for rep in range(1, args.repeats + 1):
            for size in sizes:
                for shape in ("unique", "churn"):
                    port += 1
                    r = one(args, size, shape, port)
                    r["repeat"] = rep
                    results.append(r)
                    raw.write(json.dumps(r) + "\n")
                    raw.flush()
                    print(f"rep{rep} {size:>8} {shape:<6} keys={r['keys']:>8} records={r['wal_records']:>9} "
                          f"log {r['wal_bytes']/1e6:7.1f} MB -> {r['log_only']['external_s']:6.2f}s | "
                          f"snapshot {r['snapshot_bytes']/1e6:6.1f} MB -> {r['snapshot']['external_s']:6.2f}s "
                          f"{'OK' if r['keys_match'] else 'KEY COUNT MISMATCH'}", flush=True)

    # summary table
    lines = ["| keys | history | log records | log size | log-only recovery (median, min..max) | snapshot size | "
             "snapshot recovery (median, min..max) | speedup | SAVE time |", "|---:|---|---:|---:|---|---:|---|---:|---:|"]
    for size in sizes:
        for shape in ("unique", "churn"):
            rr = [r for r in results if r["size"] == size and r["shape"] == shape]
            if not rr:
                continue
            lo = [r["log_only"]["external_s"] for r in rr]
            sn = [r["snapshot"]["external_s"] for r in rr]
            lines.append(
                f"| {int(statistics.median([r['keys'] for r in rr])):,} | {shape} | "
                f"{int(statistics.median([r['wal_records'] for r in rr])):,} | "
                f"{statistics.median([r['wal_bytes'] for r in rr])/1e6:.0f} MB | "
                f"{statistics.median(lo):.2f} s ({min(lo):.2f}..{max(lo):.2f}) | "
                f"{statistics.median([r['snapshot_bytes'] for r in rr])/1e6:.0f} MB | "
                f"{statistics.median(sn):.2f} s ({min(sn):.2f}..{max(sn):.2f}) | "
                f"{statistics.median(lo)/statistics.median(sn):.1f}x | "
                f"{statistics.median([r['save_s'] for r in rr]):.2f} s |")
    ok = all(r["keys_match"] for r in results)
    text = "\n".join(lines) + f"\n\nKey count identical after every restart: {'yes' if ok else 'NO'} ({len(results)} recoveries pairs)\n"
    with open(os.path.join(args.out, "recovery.md"), "w") as f:
        f.write(text)
    print()
    print(text)


if __name__ == "__main__":
    main()
