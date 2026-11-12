#!/usr/bin/env python3
"""Generates docs/media/demo.svg and docs/media/architecture.svg.

  python3 docs/media/make_media.py

demo.svg is an animated terminal replay (pure SVG + CSS, no scripts, so GitHub renders it in the README). Every line in it
is a line of docs/demo-output.txt, the saved output of scripts/demo_full.sh; the script only drops repetitive lines
(the node list, the early coordinator events, repeated status blocks) and times the rest. Nothing is typed that the
real run did not print. Re-run scripts/demo_full.sh and this script to refresh it.
"""
import html
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "demo-output.txt")

# ------------------------------------------------------------------------------------------------ demo.svg
raw = [l.rstrip("\n") for l in open(SRC, encoding="utf-8")]
keep, section = [], 0
for l in raw:
    if l.startswith("== "):
        section = int(re.match(r"== (\d)", l).group(1)) if re.match(r"== \d", l) else 99
    if re.match(r"^[ab]\d :7[12]0\d", l):                      # per-node role list
        continue
    if re.match(r"^(role_transitions|role:master|repl_offset)", l):   # replication INFO noise
        continue
    if re.search(r"several masters|primary is [ab]#0 \(epoch 0\)|demoting [ab]#\d to follow [ab]#0", l):
        continue                                               # start-up coordinator events
    if section == 6 and (re.match(r"^(shard:|  member:)", l) or l.startswith("$ scripts/cluster.sh status")):
        continue                                               # the second status block
    if section == 7 and re.match(r"^\$ redis-cli -p 7000 RL.ACQUIRE", l):
        pass
    if l.strip() == "" and (not keep or keep[-1] == ""):
        continue
    keep.append(l)
while keep and keep[-1] == "":
    keep.pop()

LH, FS, COLS = 17, 12, 118
W, VISIBLE = 920, 27                      # terminal width, visible text rows
TOP, LEFT = 44, 18
H = TOP + VISIBLE * LH + 18

def cls(l):
    if l.startswith("== "):
        return "h"
    if l.startswith("$ "):
        return "c"
    if re.search(r"PRIMARY FAILED|role=down|killed|unreachable", l):
        return "r"
    if re.search(r"PROMOTED|recovered|state=healthy|^OK|writes to shard|nothing acknowledged|errors=0", l):
        return "g"
    if re.search(r"READONLY|REJOIN|demoting|re-pointing", l):
        return "y"
    return "t"

# timing (seconds): headings pause, commands "type", output flows
t, times = 0.6, []
for i, l in enumerate(keep):
    c = cls(l)
    if c == "h":
        t += 1.4
    elif c == "c":
        t += 0.7
    elif l == "":
        t += 0.2
    else:
        t += 0.16
    times.append(t)
HOLD = 5.0
T = round(t + HOLD, 2)

css = [f"""
text{{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,"DejaVu Sans Mono",monospace;font-size:{FS}px;white-space:pre}}
.t{{fill:#c9d1d9}} .h{{fill:#79c0ff;font-weight:700}} .c{{fill:#7ee787;font-weight:700}}
.r{{fill:#ff7b72;font-weight:700}} .g{{fill:#7ee787}} .y{{fill:#e3b341}}
.ln{{opacity:0}}
"""]
for i, tm in enumerate(times):
    p = tm / T * 100
    css.append(f"@keyframes k{i}{{0%,{p:.3f}%{{opacity:0}}{p + 0.08:.3f}%,96%{{opacity:1}}99%,100%{{opacity:0}}}}")
    css.append(f".l{i}{{animation:k{i} {T}s linear infinite}}")
# smooth scroll: the content moves up as lines are added so the newest line stays on screen
kf = ["0%{transform:translateY(0)}"]
for i, tm in enumerate(times):
    y = max(0, (i + 1) * LH - VISIBLE * LH)
    kf.append(f"{tm / T * 100:.3f}%{{transform:translateY({-y}px)}}")
end_y = -max(0, len(keep) * LH - VISIBLE * LH)
kf.append(f"99%{{transform:translateY({end_y}px)}}100%{{transform:translateY(0)}}")
css.append("@keyframes scroll{" + "".join(kf) + "}")
css.append(f".scroll{{animation:scroll {T}s linear infinite}}")

body = []
for i, l in enumerate(keep):
    s = l if len(l) <= COLS else l[:COLS - 1] + "…"
    body.append(f'<text class="ln l{i} {cls(l)}" x="{LEFT}" y="{TOP + (i + 1) * LH - 4}" xml:space="preserve">{html.escape(s)}</text>')

svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" role="img" aria-label="Terminal replay of scripts/demo_full.sh: a 2-shard replicated DistCache cluster routes keys, rate-limits, survives kill -9 of a primary, rejoins it as a replica and rebalances live">
<title>DistCache end-to-end demo (real output of scripts/demo_full.sh)</title>
<defs><clipPath id="clip"><rect x="0" y="{TOP - 6}" width="{W}" height="{VISIBLE * LH + 12}"/></clipPath></defs>
<style>{"".join(css)}</style>
<rect width="{W}" height="{H}" rx="10" fill="#0d1117" stroke="#30363d"/>
<rect width="{W}" height="30" rx="10" fill="#161b22"/><rect y="20" width="{W}" height="10" fill="#161b22"/>
<circle cx="18" cy="15" r="5.5" fill="#ff5f56"/><circle cx="38" cy="15" r="5.5" fill="#ffbd2e"/><circle cx="58" cy="15" r="5.5" fill="#27c93f"/>
<text x="{W / 2}" y="19" text-anchor="middle" fill="#8b949e" style="font-size:12px">scripts/demo_full.sh: 2 shards x (1 primary + 2 replicas), sync replication, WAL, router (real output)</text>
<g clip-path="url(#clip)"><g class="scroll">
{chr(10).join(body)}
</g></g>
</svg>
"""
open(os.path.join(HERE, "demo.svg"), "w", encoding="utf-8").write(svg)
print(f"demo.svg: {len(keep)} lines from demo-output.txt, loop {T}s, {len(svg) // 1024} KB")

# debug aid: DEMO_FRAME=<n> also writes demo_frame.svg, a static picture of the terminal after n lines (no animation)
if os.environ.get("DEMO_FRAME"):
    n = min(int(os.environ["DEMO_FRAME"]), len(keep))
    off = max(0, n * LH - VISIBLE * LH)
    static = re.sub(r"<style>.*?</style>", f"<style>text{{font-family:ui-monospace,Consolas,monospace;font-size:{FS}px;white-space:pre}}"
                    ".t{fill:#c9d1d9}.h{fill:#79c0ff;font-weight:700}.c{fill:#7ee787;font-weight:700}.r{fill:#ff7b72;font-weight:700}"
                    ".g{fill:#7ee787}.y{fill:#e3b341}</style>", svg, flags=re.S)
    static = re.sub(r'class="ln l\d+ ', 'class="', static)
    lines_out = [m for m in re.findall(r"<text class=.*?</text>", static, flags=re.S) if 'y="' in m]
    shown = [ln for ln in re.findall(r'<text class="[a-z]+" x="\d+" y="\d+" xml:space="preserve">.*?</text>', static)][:n]
    head = static[: static.index('<g class="scroll">')]
    open(os.path.join(HERE, "demo_frame.svg"), "w", encoding="utf-8").write(
        head + f'<g transform="translate(0,{-off})">' + "\n".join(shown) + "</g></g></svg>")
    print(f"demo_frame.svg: first {n} lines, scrolled {off}px")

# ------------------------------------------------------------------------------------------ architecture.svg
AW, AH = 1000, 660
o = []
def box(x, y, w, h, fill, stroke, r=8, sw=1.5, dash=None):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    o.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{d}/>')
def text(x, y, s, size=13, weight=400, fill="#1f2328", anchor="start", style=""):
    o.append(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}" {style}>{html.escape(s)}</text>')
def arrow(x1, y1, x2, y2, color="#57606a", dash=None, label=None, lx=None, ly=None, anchor="middle"):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    o.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="1.8"{d} marker-end="url(#ah)"/>')
    if label:
        text(lx if lx is not None else (x1 + x2) / 2, ly if ly is not None else (y1 + y2) / 2 - 4, label, 11, 400, color, anchor)

box(0, 0, AW, AH, "#ffffff", "#d0d7de", 12)
# clients
box(330, 18, 340, 46, "#f6f8fa", "#8c959f")
text(500, 38, "Clients", 14, 700, anchor="middle")
text(500, 55, "redis-cli  ·  any RESP client  ·  smart client (same ring)", 11.5, 400, "#57606a", "middle")
arrow(500, 64, 500, 104, label="RESP over TCP", lx=560, ly=88)
# router
box(40, 104, 920, 156, "#ddf4ff", "#54aeff", 10, 2)
text(60, 128, "distcache_router", 15, 700, "#0969da")
text(940, 128, "one process; one coordinator thread per replicated shard", 11.5, 400, "#0969da", "end")
box(60, 142, 270, 100, "#ffffff", "#8c959f")
text(75, 162, "Consistent-hash ring", 13, 700); text(75, 180, "key (hash tags) → shard NAME", 12); text(75, 197, "160 virtual nodes per node", 12)
text(75, 214, "live add / remove with MIGRATE", 12); text(75, 231, "(resumable if a step fails)", 12, 400, "#57606a")
box(350, 142, 270, 100, "#ffffff", "#8c959f")
text(365, 162, "Route + availability", 13, 700); text(365, 180, "shard → current primary address", 12)
text(365, 197, "a shard that is ambiguous or", 12); text(365, 214, "has no safe primary is marked", 12); text(365, 231, "unavailable: reads AND writes refused", 12)
box(640, 142, 300, 100, "#fff8c5", "#bf8700")
text(655, 162, "Failover coordinator", 13, 700); text(655, 180, "probe ROLE · renew write LEASE (2 s)", 12)
text(655, 197, "detect failure · wait out the old lease", 12); text(655, 214, "PROMOTE best replica · bump epoch", 12)
text(655, 231, "demote + resync a returning old primary", 12)

def shard(x, name):
    box(x, 296, 440, 344, "#f6f8fa", "#d0d7de", 10)
    text(x + 16, 318, f"Shard {name}", 14, 700)
    # primary
    box(x + 16, 330, 408, 124, "#dafbe1", "#2da44e", 8, 2)
    text(x + 28, 350, "PRIMARY  (epoch e, holds a lease)", 13, 700, "#1a7f37")
    text(x + 28, 369, "execute() → shard lock → engine (LRU, TTL, RL.* update)", 11.5)
    text(x + 28, 386, "inside that same critical section, in order:", 11.5, 400, "#57606a")
    text(x + 40, 402, "1. WAL buffer (group commit, fsync policy)", 11.5)
    text(x + 40, 418, "2. replication log, offset k (idempotent records)", 11.5)
    text(x + 28, 440, "reply held until: WAL durable ∧ N replicas ACKed k ∧ lease valid", 11, 700, "#1a7f37")
    # replicas
    for i, rx in enumerate((x + 16, x + 224)):
        box(rx, 500, 200, 120, "#ffffff", "#8c959f", 8)
        text(rx + 12, 520, f"REPLICA {i + 1}", 13, 700)
        text(rx + 12, 538, "read-only (-READONLY on writes)", 11.5)
        text(rx + 12, 555, "applies  R offset record  in order", 11.5)
        text(rx + 12, 572, "duplicates skipped, gap → resync", 11.5)
        text(rx + 12, 589, "ACK offset  → primary", 11.5)
        text(rx + 12, 608, "serves (possibly stale) reads", 11, 400, "#57606a")
        arrow(rx + 100, 454, rx + 100, 500, "#2da44e", label="stream" if i == 0 else None, lx=rx + 112, ly=472, anchor="start")
    text(x + 220, 490, "full resync = snapshot + replay", 10, 400, "#57606a", "middle")
shard(40, "a")
shard(520, "b")
arrow(180, 260, 180, 296, label="forward by key", lx=192, ly=282, anchor="start")
arrow(760, 260, 760, 296, label="forward by key", lx=772, ly=282, anchor="start")
# the coordinator's control traffic reaches the primary of EVERY shard: one dashed L-shaped path per shard
for tx, label in ((420, "LEASE · PROMOTE · REPLICAOF"), (900, None)):
    pts = f"M{660 if tx == 420 else 900},242 " + (f"L660,270 L{tx},270 L{tx},296" if tx == 420 else "L900,296")
    o.append(f'<path d="{pts}" fill="none" stroke="#bf8700" stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#ah)"/>')
text(540, 288, "LEASE · PROMOTE · REPLICAOF to each shard's primary", 10.5, 400, "#9a6700", "middle")
text(500, 652, "Epochs fence stale primaries; with --repl-sync N and R replicas, promotion needs R-N+1 eligible replicas, so the promoted node holds every acknowledged write.", 11, 400, "#57606a", "middle")

arch = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {AW} {AH}" width="{AW}" role="img" aria-label="DistCache architecture: clients, the router with ring and failover coordinator, shards with a primary and two replicas">
<title>DistCache architecture</title>
<defs><marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#57606a"/></marker></defs>
<style>text{{font-family:-apple-system,"Segoe UI",Helvetica,Arial,sans-serif}}</style>
{chr(10).join(o)}
</svg>
"""
open(os.path.join(HERE, "architecture.svg"), "w", encoding="utf-8").write(arch)
print(f"architecture.svg: {len(arch) // 1024} KB")
