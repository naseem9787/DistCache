#pragma once
// distcache_router: a RESP proxy that spreads keys over several DistCache nodes with a
// consistent hash ring.
//
//   client ──RESP──► router ──RESP──► node A   (owner of hash(key) on the ring)
//                           └──────► node B
//
// A client talks to the router exactly as it would to one node (redis-cli works). The router
// parses each command, finds the key, asks the ring which node owns it, forwards the command and
// relays the reply.
//
// Threading: one thread per client connection, blocking sockets. Each client thread owns its
// own connection to every node it has used, so there is no shared connection pool and no
// locking on the data path in steady state. A client's commands are executed in order. Within
// a pipelined batch the router sends to all nodes first and then reads the replies in command
// order, so a batch that touches several nodes costs about one round trip, not one per command.
// Thread-per-connection does not scale to thousands of clients; a real proxy would use an
// event loop (documented in the README).
//
// Topology changes (RING ADD / RING REMOVE) re-build the ring and publish it atomically. With
// MIGRATE the router also moves the affected keys, see migrate_keys() for the protocol and the
// guarantee it gives. Without MIGRATE the keys that changed owner simply become unreachable
// (misses) on the new owner: acceptable for a cache, and measured in the README.
//
// Single-router assumption: the migration protocol's atomicity comes from locks inside THIS
// process. Several routers sharing one set of nodes can serve traffic with a static ring, but
// must not migrate concurrently with each other or with unmigrated writers.
#include <array>
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "hash_ring.hpp"

struct RouterNode {
    std::string name;   // identity on the ring: placement depends on this, not on the address
    std::string host;
    int port = 0;
    uint32_t weight = 1;
};

struct RouterOptions {
    int port = 6390;                                  // 0 = let the OS pick (see Router::port())
    uint32_t vnodes = HashRing::kDefaultVnodes;       // ring points per unit of weight
    int io_timeout_ms = 3000;                         // connect / send / recv timeout towards nodes
};

struct MigrationReport {
    uint64_t scanned = 0;     // keys listed on the source node(s)
    uint64_t moved = 0;       // copied to the new owner and removed from the source
    uint64_t superseded = 0;  // new owner already held a newer value; the stale copy was dropped
    uint64_t vanished = 0;    // expired or deleted between listing and copying
    uint64_t errors = 0;      // node unreachable or unexpected reply (the key is left where it was)
};

class Router {
public:
    Router(std::vector<RouterNode> nodes, RouterOptions opts = {});
    ~Router();
    Router(const Router&) = delete;
    Router& operator=(const Router&) = delete;

    bool start(std::string* error = nullptr);  // bind, listen, spawn the acceptor
    void stop();                               // idempotent; closes every client connection
    int port() const { return port_; }

    // Topology changes. Serialized with each other (one at a time); safe to call while clients
    // are active. `report` is filled when migrate is true.
    bool add_node(const RouterNode& node, bool migrate, MigrationReport* report, std::string* error);
    bool remove_node(const std::string& name, bool migrate, MigrationReport* report, std::string* error);

    std::string info() const;                       // the text served by RING INFO / INFO
    std::string owner_of(const std::string& key) const;  // "" when the ring is empty

private:
    struct Route;
    class Session;
    struct SessionHandle;

    std::shared_ptr<const Route> route() const;
    void publish(std::shared_ptr<const Route> r);
    void accept_loop();
    MigrationReport migrate_keys(const Route& route, const std::vector<std::string>& sources);

    static constexpr size_t kStripes = 1024;

    RouterOptions opts_;
    int listen_fd_ = -1;
    int port_ = 0;
    std::atomic<bool> running_{false};
    std::thread acceptor_;

    std::shared_ptr<const Route> route_;   // accessed with std::atomic_load / atomic_store
    std::mutex admin_mu_;                  // one topology change at a time
    // Number of client batches currently running on the unlocked fast path. A migration
    // publishes its "migrating" route first and then waits for this to reach zero, so no
    // unlocked operation can overlap the first migration step. (A reader/writer lock would do
    // the same job but glibc's prefers readers: with clients always active the migration could
    // starve. Here new batches see the migrating route and leave the fast path at once.)
    std::atomic<int> fast_inflight_{0};
    // Per-key serialization while a migration runs (keys hash to a stripe). A client operation
    // on a moved key and a migration step on the same key exclude each other.
    std::array<std::mutex, kStripes> stripes_;

    std::mutex sessions_mu_;
    std::vector<std::unique_ptr<SessionHandle>> sessions_;

    std::atomic<uint64_t> forwarded_{0};
    std::atomic<uint64_t> backend_errors_{0};
};
