// bench_ring: measures the consistent hash ring itself (no sockets).
//
//   bench_ring [keys]        default 200000 sampled keys
//
// Prints four tables:
//   1. balance      share of the hash circle per node vs number of virtual nodes
//   2. add a node   fraction of keys that move when the (N+1)th node joins, vs the ideal 1/(N+1)
//                   and vs hash(key) % N, the placement the ring replaces
//   3. remove node  same for a node leaving
//   4. cost         ring build time and lookup time
// Everything is deterministic (fixed key names, fixed hash) except the timings of table 4.
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "hash.hpp"
#include "hash_ring.hpp"

namespace {

std::vector<HashRing::Node> make_nodes(int n) {
    std::vector<HashRing::Node> v;
    for (int i = 0; i < n; ++i) v.push_back({"node-" + std::to_string(i), 1});
    return v;
}

std::string key_name(uint64_t i) { return "key:" + std::to_string(i); }

double now_s() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

}  // namespace

int main(int argc, char** argv) {
    const uint64_t keys = argc > 1 ? static_cast<uint64_t>(std::atoll(argv[1])) : 200000;
    std::vector<uint64_t> hashes(keys);
    for (uint64_t i = 0; i < keys; ++i) hashes[i] = key_hash(key_name(i));

    std::printf("# 1. balance: exact share of the circle owned by each node (ideal = 1/N)\n");
    std::printf("%6s %8s %9s %9s %9s %9s\n", "nodes", "vnodes", "min/ideal", "max/ideal", "stddev/mean", "points");
    for (int n : {3, 5, 10}) {
        for (uint32_t vn : {1u, 4u, 16u, 64u, 160u, 512u}) {
            HashRing r(make_nodes(n), vn);
            const auto share = r.ownership();
            double mn = 1e9, mx = 0, mean = 1.0 / n, var = 0;
            for (double s : share) { mn = std::min(mn, s); mx = std::max(mx, s); var += (s - mean) * (s - mean); }
            std::printf("%6d %8u %9.3f %9.3f %9.3f %9zu\n", n, vn, mn / mean, mx / mean,
                        std::sqrt(var / n) / mean, r.point_count());
        }
    }

    std::printf("\n# 2. adding one node: fraction of %llu sampled keys that change owner\n",
                static_cast<unsigned long long>(keys));
    std::printf("%6s %9s %9s %9s %11s %s\n", "N->N+1", "ideal", "ring", "modulo", "moved-to-old", "(keys that moved between two OLD nodes)");
    for (int n : {2, 3, 4, 5, 8, 16, 32}) {
        HashRing before(make_nodes(n));
        HashRing after = before.with_node({"node-new", 1});
        uint64_t moved = 0, to_old = 0, mod_moved = 0;
        for (uint64_t h : hashes) {
            const int a = before.owner_index(h), b = after.owner_index(h);
            if (before.nodes()[static_cast<size_t>(a)].name != after.nodes()[static_cast<size_t>(b)].name) {
                ++moved;
                if (after.nodes()[static_cast<size_t>(b)].name != "node-new") ++to_old;
            }
            if (h % static_cast<uint64_t>(n) != h % static_cast<uint64_t>(n + 1)) ++mod_moved;
        }
        std::printf("%3d->%-3d %9.4f %9.4f %9.4f %11llu\n", n, n + 1, 1.0 / (n + 1),
                    static_cast<double>(moved) / keys, static_cast<double>(mod_moved) / keys,
                    static_cast<unsigned long long>(to_old));
    }

    std::printf("\n# 3. removing one node: fraction of keys that change owner (ideal = 1/N)\n");
    std::printf("%6s %9s %9s %9s %11s\n", "N->N-1", "ideal", "ring", "modulo", "moved-others");
    for (int n : {3, 4, 5, 8, 16, 32}) {
        HashRing before(make_nodes(n));
        HashRing after = before.without_node("node-" + std::to_string(n / 2));
        uint64_t moved = 0, others = 0, mod_moved = 0;
        for (uint64_t h : hashes) {
            const std::string& a = before.nodes()[static_cast<size_t>(before.owner_index(h))].name;
            const std::string& b = after.nodes()[static_cast<size_t>(after.owner_index(h))].name;
            if (a != b) {
                ++moved;
                if (a != "node-" + std::to_string(n / 2)) ++others;
            }
            if (h % static_cast<uint64_t>(n) != h % static_cast<uint64_t>(n - 1)) ++mod_moved;
        }
        std::printf("%3d->%-3d %9.4f %9.4f %9.4f %11llu\n", n, n - 1, 1.0 / n, static_cast<double>(moved) / keys,
                    static_cast<double>(mod_moved) / keys, static_cast<unsigned long long>(others));
    }

    std::printf("\n# 4. cost (this machine, single thread)\n");
    std::printf("%6s %8s %9s %12s %12s\n", "nodes", "vnodes", "points", "build_ms", "lookup_ns");
    for (int n : {3, 10, 100}) {
        for (uint32_t vn : {16u, 160u}) {
            double t0 = now_s();
            HashRing r(make_nodes(n), vn);
            const double build_ms = (now_s() - t0) * 1000;
            // lookup of a precomputed hash (the ring part), median of 5 passes
            std::vector<double> per;
            volatile int sink = 0;
            for (int pass = 0; pass < 5; ++pass) {
                t0 = now_s();
                int acc = 0;
                for (uint64_t h : hashes) acc += r.owner_index(h);
                per.push_back((now_s() - t0) * 1e9 / static_cast<double>(keys));
                sink = sink + acc;
            }
            std::sort(per.begin(), per.end());
            std::printf("%6d %8u %9zu %12.2f %12.1f\n", n, vn, r.point_count(), build_ms, per[2]);
        }
    }
    {   // hashing a key string itself
        double t0 = now_s();
        volatile uint64_t sink = 0;
        std::vector<std::string> names;
        for (uint64_t i = 0; i < 100000; ++i) names.push_back(key_name(i));
        t0 = now_s();
        uint64_t acc = 0;
        for (const auto& s : names) acc += key_hash(s);
        sink = acc;
        std::printf("key_hash of a ~10 byte key: %.1f ns\n", (now_s() - t0) * 1e9 / 100000.0);
    }
    return 0;
}
