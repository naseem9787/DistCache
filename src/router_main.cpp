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
    std::vector<RouterNode> nodes;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) { std::fprintf(stderr, "missing value for %s\n", a.c_str()); std::exit(2); }
            return argv[++i];
        };
        if (a == "--port") opts.port = std::atoi(next());
        else if (a == "--vnodes") opts.vnodes = static_cast<uint32_t>(std::max(1, std::atoi(next())));
        else if (a == "--timeout-ms") opts.io_timeout_ms = std::atoi(next());
        else if (a == "--node") {
            RouterNode n;
            const std::string spec = next();
            if (!parse_node(spec, &n)) { std::fprintf(stderr, "bad --node '%s' (want NAME=HOST:PORT[@WEIGHT])\n", spec.c_str()); return 2; }
            nodes.push_back(std::move(n));
        } else if (a == "--help" || a == "-h") { usage(); return 0; }
        else { std::fprintf(stderr, "unknown option %s\n", a.c_str()); usage(); return 2; }
    }

    Router router(std::move(nodes), opts);
    std::string err;
    if (!router.start(&err)) { std::fprintf(stderr, "cannot start: %s\n", err.c_str()); return 1; }
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::signal(SIGPIPE, SIG_IGN);
    std::printf("distcache_router listening on port %d\n%s", router.port(), router.info().c_str());
    std::fflush(stdout);
    while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    router.stop();
    return 0;
}
