#!/usr/bin/env python3
"""Aggregate a profile written by the diagnostics build (DISTCACHE_PROFILE=file) into
per-thread "where did the CPU go" tables.

    bench/harness/symbolize.py profile.txt build/distcache_diag [--top 14]

The profile is raw stack samples taken by a SIGPROF timer (process CPU time, user + system),
each tagged with the name of the interrupted thread. Addresses are resolved with addr2line.
Limits worth knowing:
  * samples count ON-CPU time only. A thread blocked on a lock or a disk does not appear; the
    nanosecond timers in `INFO` (diag_*) cover the waiting side.
  * time spent inside the kernel is attributed to the user-space function that made the
    system call (e.g. a write() shows up under Wal::write_all / write).
  * the sampler perturbs the program slightly; use it to compare shares, not absolute speed.
"""
import argparse, collections, subprocess, sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("profile")
    ap.add_argument("binary")
    ap.add_argument("--top", type=int, default=14)
    args = ap.parse_args()

    base = 0
    libsym = {}
    samples = []
    with open(args.profile) as f:
        for line in f:
            if line.startswith("# base="):
                base = int(line.split("base=")[1].split()[0], 16)
                continue
            if line.startswith("@ "):  # library symbol resolved in-process by dladdr: "@ addr module symbol"
                _, a, mod, sym = line.split(None, 3)
                libsym[int(a, 16)] = f"{sym.strip()} [{mod}]"
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            tag, n = parts[0], int(parts[1])
            frames = [int(x, 16) for x in parts[2:2 + n]]
            samples.append((tag, frames[2:] if len(frames) > 2 else frames))  # drop handler + sigreturn frames
    if not samples:
        sys.exit("no samples")

    addrs = sorted({a for _, fr in samples for a in fr})
    offs = [a - base for a in addrs]
    resolved = {}
    CHUNK = 2000
    for i in range(0, len(offs), CHUNK):
        chunk = offs[i:i + CHUNK]
        out = subprocess.run(["addr2line", "-a", "-f", "-C", "-i", "-e", args.binary] + [hex(o) for o in chunk],
                             capture_output=True, text=True).stdout.splitlines()
        cur = None
        funcs = []
        pending_fn = None
        for ln in out:
            if ln.startswith("0x"):  # addr2line -a prints the address; function names never start with 0x
                if cur is not None:
                    resolved[cur] = funcs
                cur = int(ln, 16)
                funcs = []
                pending_fn = None
            elif pending_fn is None:
                pending_fn = ln
            else:
                funcs.append(pending_fn)
                pending_fn = None
        if cur is not None:
            resolved[cur] = funcs
    by_off = {}
    for a, o in zip(addrs, offs):
        fns = [f for f in resolved.get(o, []) if f and f != "??"]
        by_off[a] = fns or [libsym.get(a, "[unresolved library/kernel]")]

    def short(fn):
        fn = fn.replace("std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >", "std::string")
        return fn if len(fn) < 90 else fn[:87] + "..."

    per_tag = collections.defaultdict(lambda: dict(n=0, self=collections.Counter(), incl=collections.Counter()))
    for tag, frames in samples:
        t = per_tag[tag]
        t["n"] += 1
        if frames:
            t["self"][short(by_off[frames[0]][0])] += 1
        seen = set()
        for a in frames:
            for fn in by_off[a]:
                seen.add(short(fn))
        for fn in seen:
            t["incl"][fn] += 1

    total = len(samples)
    print(f"{total} samples; breakdown by thread:")
    for tag, t in sorted(per_tag.items(), key=lambda kv: -kv[1]["n"]):
        print(f"  {tag:<10} {t['n']:>7}  {t['n'] / total * 100:5.1f}% of all CPU samples")
    for tag, t in sorted(per_tag.items(), key=lambda kv: -kv[1]["n"]):
        if t["n"] < total * 0.02:
            continue
        print(f"\n=== thread: {tag}  ({t['n']} samples)")
        print("  self time (function the CPU was executing):")
        for fn, c in t["self"].most_common(args.top):
            print(f"    {c / t['n'] * 100:5.1f}%  {fn}")
        print("  inclusive time (function anywhere on the stack):")
        for fn, c in t["incl"].most_common(args.top):
            print(f"    {c / t['n'] * 100:5.1f}%  {fn}")


if __name__ == "__main__":
    main()
