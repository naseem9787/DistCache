#!/bin/bash
# Build the Phase 5 server (the commit before Phase 6) as build/distcache_phase5, so the Phase 6
# changes can be compared against the code they replaced, under identical conditions.
#
# Usage: scripts/build_baseline.sh [commit]        default commit: cdd855d (Phase 5)
#
# It builds in a temporary git worktree, so your working tree is not touched.
set -e
cd "$(dirname "$0")/.."
COMMIT=${1:-cdd855d}
WT=$(mktemp -d /tmp/distcache-baseline-XXXXXX)
trap 'git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"' EXIT
git worktree add --detach "$WT" "$COMMIT" > /dev/null
cmake -S "$WT" -B "$WT/build" -G Ninja -DCMAKE_BUILD_TYPE=Release > /dev/null
cmake --build "$WT/build" --target distcache > /dev/null
mkdir -p build
cp "$WT/build/distcache" build/distcache_phase5
echo "built build/distcache_phase5 from $(git rev-parse --short "$COMMIT") ($(sha256sum build/distcache_phase5 | cut -c1-16))"
