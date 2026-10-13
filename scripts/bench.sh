#!/bin/bash
# Reproduce the benchmark results end to end.
#
#   scripts/bench.sh                 full run: build, 2 independent suite runs x 3 repeats, recovery, report
#   scripts/bench.sh --quick         ~5 minute smoke run (1 repeat, short runs, E1+E7 only)
#   scripts/bench.sh --runs 3 --repeats 5
#
# Output goes to results/ (raw JSONL per run, env.json, report.md, SVG charts). Takes roughly
# 35 minutes per suite run on the reference machine. Do not run other CPU-heavy work meanwhile:
# the harness pins server and load generator to separate CPUs, but a busy machine still adds noise.
set -e
cd "$(dirname "$0")/.."
RUNS=2; REPEATS=3; DURATION=8; WARMUP=1.5; SUITE=all; QUICK=0; RECOVERY_SIZES=100000,500000,1000000
while [ $# -gt 0 ]; do
  case "$1" in
    --runs) RUNS=$2; shift 2;;
    --repeats) REPEATS=$2; shift 2;;
    --quick) QUICK=1; RUNS=1; REPEATS=1; DURATION=3; WARMUP=1; SUITE=E1,E7; RECOVERY_SIZES=100000; shift;;
    *) echo "unknown option $1" >&2; exit 2;;
  esac
done

echo "== build (Release)"
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release > /dev/null
cmake --build build > /dev/null
mkdir -p results

# The Phase 5 server, built from git, is the baseline the Phase 6 changes are compared against
# (E7 runs both binaries interleaved in the same session).
[ -x build/distcache_phase5 ] || scripts/build_baseline.sh
BINARIES="--binary final=build/distcache --binary baseline=build/distcache_phase5"

echo "== machine health"
scripts/health_probe.sh || echo "WARNING: the machine is not in a representative state; see docs/benchmarks/phase6 in the README"

echo "== environment"
python3 - <<'PY'
import os, platform, subprocess
print("kernel", platform.release(), "| cpus", os.cpu_count())
print(subprocess.run("lscpu | grep 'Model name'", shell=True, capture_output=True, text=True).stdout.strip())
PY

for i in $(seq 1 $RUNS); do
  echo "== suite run $i of $RUNS"
  rm -rf results/run-$i
  python3 bench/harness/run_suite.py --suite $SUITE --repeats $REPEATS --duration $DURATION --warmup $WARMUP \
      $BINARIES --out results/run-$i
done

echo "== report"
python3 bench/harness/report.py results/run-*/raw.jsonl --out results/report
echo "== recovery"
python3 bench/harness/bench_recovery.py --sizes $RECOVERY_SIZES --repeats $([ $QUICK = 1 ] && echo 1 || echo 5) --out results/recovery
echo "done: results/report/report.md, results/recovery/recovery.md"
