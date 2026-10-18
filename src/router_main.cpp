// distcache_router: command-line entry point.
//
//   distcache_router --port 6390 --node a=127.0.0.1:7001 --node b=127.0.0.1:7002 [--vnodes 160]
//
// Clients (redis-cli, the load generator) connect to the router as if it were one node.
// Topology can be changed at runtime with RING ADD / RING REMOVE (see README).
#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "net_util.hpp"
#include "version.hpp"
#include "router.hpp"

namespace {

volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

void usage() {
    std::puts(
        "usage: distcache_router [options]\n"
        "  --port N               listen port (default 6390)\n"
        "  --node NAME=HOST:PORT[@WEIGHT]   a backend node; repeat for each node\n"
        "                         NAME is the node's identity on the ring (placement depends on it)\n"
        "  --replica NAME=HOST:PORT   a replica (client port) of shard NAME; repeat per replica. A shard with\n"
        "                         replicas is watched by the failover coordinator (its nodes need --repl-port\n"
        "                         and --repl-require-lease; the primary one of --node, replicas --replicaof)\n"
        "  --lease-ms N           write lease length L (default 2000); renewed every --probe-ms\n"
        "  --probe-ms N           probe / renewal period (default 500)\n"
        "  --probe-timeout-ms N   per-request timeout towards nodes, D (default 500)\n"
        "  --fail-after N         failed probes before a primary is declared failed, F (default 3)\n"
        "  --guard-ms N           guard band G added to the lease bound (default 500)\n"
        "  --sync-replicas N      the --repl-sync the shard's nodes run with (default 0); sets the promotion rule\n"
        "  --state-file PATH      remember the highest epoch of every shard across router restarts\n"
        "  --vnodes N             ring points per unit of weight (default 160)\n"
        "  --timeout-ms N         connect/send/recv timeout towards nodes (default 3000)\n"
        "At runtime:  RING INFO | RING LOOKUP key | RING ADD name host port [weight] [MIGRATE]\n"
        "             RING REMOVE name [MIGRATE]");
}

// "a=127.0.0.1:7001@2"
bool parse_node(const std::string& s, RouterNode* n) {
    const size_t eq = s.find('=');
    if (eq == std::string::npos || eq == 0) return false;
    n->name = s.substr(0, eq);
    std::string rest = s.substr(eq + 1);
    const size_t at = rest.find('@');
    if (at != std::string::npos) {
        n->weight = static_cast<uint32_t>(std::max(1, std::atoi(rest.c_str() + at + 1)));
        rest.resize(at);
    }
    const size_t colon = rest.rfind(':');
    if (colon == std::string::npos || colon == 0) return false;
    n->host = rest.substr(0, colon);
    n->port = std::atoi(rest.c_str() + colon + 1);
    return n->port > 0 && n->port <= 65535;
}

}  // namespace

int main(int argc, char** argv) {
    RouterOptions opts;
    opts.bind = "127.0.0.1";
    std::vector<RouterNode> nodes;
    std::vector<std::pair<std::string, RouterReplica>> pending_replicas;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) { std::fprintf(stderr, "missing value for %s\n", a.c_str()); std::exit(2); }
            return argv[++i];
        };
        long long iv = 0;
        if (a == "--port") { if (!parse_int_arg("--port", next(), 0, 65535, &iv)) return 2; opts.port = static_cast<int>(iv); }
        else if (a == "--bind") { opts.bind = next(); in_addr_t tmp; if (!parse_bind_address(opts.bind, &tmp)) { std::fprintf(stderr, "--bind wants an IPv4 address, got '%s'\n", opts.bind.c_str()); return 2; } }
        else if (a == "--vnodes") { if (!parse_int_arg("--vnodes", next(), 1, 100000, &iv)) return 2; opts.vnodes = static_cast<uint32_t>(iv); }
        else if (a == "--timeout-ms") { if (!parse_int_arg("--timeout-ms", next(), 1, 3600000, &iv)) return 2; opts.io_timeout_ms = static_cast<int>(iv); }
        else if (a == "--lease-ms") { if (!parse_int_arg("--lease-ms", next(), 50, 3600000, &iv)) return 2; opts.failover.lease_ms = static_cast<int>(iv); }
        else if (a == "--probe-ms") { if (!parse_int_arg("--probe-ms", next(), 5, 3600000, &iv)) return 2; opts.failover.probe_ms = static_cast<int>(iv); }
        else if (a == "--probe-timeout-ms") { if (!parse_int_arg("--probe-timeout-ms", next(), 5, 3600000, &iv)) return 2; opts.failover.probe_timeout_ms = static_cast<int>(iv); }
        else if (a == "--fail-after") { if (!parse_int_arg("--fail-after", next(), 1, 1000, &iv)) return 2; opts.failover.fail_after = static_cast<int>(iv); }
        else if (a == "--guard-ms") { if (!parse_int_arg("--guard-ms", next(), 0, 3600000, &iv)) return 2; opts.failover.guard_ms = static_cast<int>(iv); }
        else if (a == "--sync-replicas") { if (!parse_int_arg("--sync-replicas", next(), 0, 16, &iv)) return 2; opts.failover.sync_replicas = static_cast<int>(iv); }
        else if (a == "--state-file") opts.failover.state_file = next();
        else if (a == "--replica") {
            const std::string spec = next();
            const size_t eq = spec.find('=');
            const size_t colon = spec.rfind(':');
            if (eq == std::string::npos || colon == std::string::npos || colon < eq) {
                std::fprintf(stderr, "bad --replica '%s' (want SHARD=HOST:PORT)\n", spec.c_str());
                return 2;
            }
            pending_replicas.push_back({spec.substr(0, eq), {spec.substr(eq + 1, colon - eq - 1), std::atoi(spec.c_str() + colon + 1)}});
        }
        else if (a == "--node") {
            RouterNode n;
            const std::string spec = next();
            if (!parse_node(spec, &n)) { std::fprintf(stderr, "bad --node '%s' (want NAME=HOST:PORT[@WEIGHT])\n", spec.c_str()); return 2; }
            nodes.push_back(std::move(n));
        } else if (a == "--help" || a == "-h") { usage(); return 0; }
        else { std::fprintf(stderr, "unknown option %s\n", a.c_str()); usage(); return 2; }
    }

    for (auto& pr : pending_replicas) {
        bool found = false;
        for (auto& n : nodes)
            if (n.name == pr.first) { n.replicas.push_back(pr.second); found = true; }
        if (!found) { std::fprintf(stderr, "--replica for unknown shard '%s' (declare it with --node first)\n", pr.first.c_str()); return 2; }
    }
    Router router(std::move(nodes), opts);
    std::string err;
    if (!router.start(&err)) { std::fprintf(stderr, "cannot start: %s\n", err.c_str()); return 1; }
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::signal(SIGPIPE, SIG_IGN);
    std::printf("distcache_router %s listening on %s:%d\n%s", DISTCACHE_VERSION, opts.bind.c_str(), router.port(), router.info().c_str());
    std::fflush(stdout);
    while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::printf("signal received, shutting down\n");
    std::fflush(stdout);
    router.stop();
    std::printf("stopped cleanly\n");
    return 0;
}
