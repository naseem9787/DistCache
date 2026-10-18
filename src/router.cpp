#include "router.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <set>

#include "hash.hpp"
#include "resp.hpp"
#include "resp_client.hpp"

namespace {

using Cmd = std::vector<std::string>;

constexpr size_t kMaxInputBytes = 64u << 20;  // per client; larger than any sane pipeline
constexpr size_t kMaxBatchCommands = 1024;    // commands handled per round of the session loop

std::string upper(std::string s) {
    for (char& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return s;
}

std::string lower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

std::string wrong_args(const std::string& name) {
    return resp_error("ERR wrong number of arguments for '" + lower(name) + "' command");
}

std::string node_error(const std::string& node, const std::string& why) {
    return resp_error("ERR node '" + node + "' unreachable: " + why);
}

std::string no_nodes() { return resp_error("ERR no nodes in the ring"); }

std::string shard_unavailable(const std::string& shard) {
    return resp_error("ERR shard '" + shard + "' is unavailable (failover in progress, or no safe primary)");
}

bool parse_integer_reply(const std::string& raw, long long* v) {
    if (raw.size() < 4 || raw[0] != ':') return false;
    *v = std::strtoll(raw.c_str() + 1, nullptr, 10);
    return true;
}

bool send_all_fd(int fd, const std::string& s) {
    size_t off = 0;
    while (off < s.size()) {
        ssize_t n = ::send(fd, s.data() + off, s.size() - off, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        off += static_cast<size_t>(n);
    }
    return true;
}

}  // namespace

// ---- routing state ----------------------------------------------------------------------------

struct Router::Route {
    std::shared_ptr<const HashRing> ring;      // where keys live from now on
    std::shared_ptr<const HashRing> old_ring;  // set only while a migration is running
    std::map<std::string, RouterNode> nodes;   // every node that may need to be contacted
    uint64_t version = 0;
    bool any_replicated = false;               // some shard is watched by the coordinator (enables availability checks)
};

struct Router::SessionHandle {
    std::mutex mu;  // guards fd (stop() shuts it down from another thread)
    int fd = -1;
    std::thread th;
    std::atomic<bool> done{false};
};

std::shared_ptr<const Router::Route> Router::route() const { return std::atomic_load(&route_); }
void Router::publish(std::shared_ptr<const Route> r) { std::atomic_store(&route_, std::move(r)); }

void Router::update_route(const std::function<void(Route&)>& fn) {
    std::lock_guard<std::mutex> lock(publish_mu_);
    auto next = std::make_shared<Route>(*route());
    fn(*next);
    ++next->version;
    publish(std::move(next));
}

void Router::set_shard_available(const std::string& shard, bool available) {
    update_route([&](Route& r) {
        auto it = r.nodes.find(shard);
        if (it != r.nodes.end()) it->second.available = available;
    });
}

// Called by the coordinator after a promotion (and when it discovers who the primary is).
void Router::set_shard_primary(const std::string& shard, const std::string& host, int port) {
    update_route([&](Route& r) {
        auto it = r.nodes.find(shard);
        if (it != r.nodes.end()) { it->second.host = host; it->second.port = port; }
    });
}

// ---- one client connection ----------------------------------------------------------------------

class Router::Session {
public:
    Session(Router& r, int fd) : r_(r), fd_(fd) {}

    void run() {
        std::string in, out;
        char buf[65536];
        for (;;) {
            const ssize_t n = ::recv(fd_, buf, sizeof buf, 0);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return;
            in.append(buf, static_cast<size_t>(n));
            if (in.size() > kMaxInputBytes) return;
            for (;;) {  // handle every complete command that has arrived, a bounded number at a time
                std::vector<Cmd> cmds;
                size_t pos = 0;
                bool bad = false;
                std::string bad_msg;
                while (pos < in.size() && cmds.size() < kMaxBatchCommands) {
                    ParseResult pr = parse_command(std::string_view(in).substr(pos));
                    if (pr.status == ParseStatus::Incomplete) break;
                    if (pr.status == ParseStatus::Error) { bad = true; bad_msg = pr.error; break; }
                    pos += pr.consumed;
                    if (!pr.args.empty()) cmds.push_back(std::move(pr.args));
                }
                in.erase(0, pos);
                out.clear();
                process(cmds, out);
                if (bad) {
                    out += resp_error("ERR Protocol error: " + bad_msg);
                    send_all_fd(fd_, out);
                    return;
                }
                if (!out.empty() && !send_all_fd(fd_, out)) return;
                if (cmds.size() < kMaxBatchCommands) break;  // nothing more is buffered
            }
        }
    }

private:
    struct Backend {
        std::unique_ptr<RespConn> conn;
        std::string addr;
    };

    // A (lazily opened, reused) connection from THIS client's thread to a node.
    RespConn* backend(const Route& route, const std::string& name, std::string* why) {
        auto nit = route.nodes.find(name);
        if (nit == route.nodes.end()) { *why = "unknown node"; return nullptr; }
        const RouterNode& n = nit->second;
        const std::string addr = n.host + ":" + std::to_string(n.port);
        Backend& b = be_[name];
        if (b.conn && b.addr != addr) b.conn.reset();
        if (!b.conn) { b.conn = std::make_unique<RespConn>(); b.addr = addr; }
        if (!b.conn->is_open() && !b.conn->connect(n.host, n.port, r_.opts_.io_timeout_ms, why)) return nullptr;
        return b.conn.get();
    }

    // One blocking command/reply exchange with a node. Always returns bytes for the client:
    // the node's raw reply, or an error reply if the node could not be reached.
    std::string forward(const Route& route, const std::string& node, const Cmd& c) {
        r_.forwarded_.fetch_add(1, std::memory_order_relaxed);
        std::string why, raw;
        RespConn* conn = backend(route, node, &why);
        if (conn && conn->send_all(encode_command(c)) && conn->read_raw(raw)) return raw;
        r_.backend_errors_.fetch_add(1, std::memory_order_relaxed);
        return node_error(node, why.empty() ? "connection lost" : why);
    }

    // -- classification ---------------------------------------------------------------------
    // 0: answered locally (`local` holds the reply)   1: forward by key   2: needs several nodes
    enum Kind { kLocal = 0, kKey = 1, kBarrier = 2 };

    static bool shard_available(const Route& route, const std::string& shard) {
        auto it = route.nodes.find(shard);
        return it == route.nodes.end() || it->second.available;
    }

    static Kind classify(const std::string& name, const Cmd& c, std::string* local, const std::string** key) {
        auto need = [&](bool ok) { if (!ok) *local = wrong_args(name); return ok; };
        if (name == "PING") {
            if (c.size() == 1) *local = resp_simple("PONG");
            else if (c.size() == 2) *local = resp_bulk(c[1]);
            else *local = wrong_args(name);
            return kLocal;
        }
        if (name == "ECHO") { *local = c.size() == 2 ? resp_bulk(c[1]) : wrong_args(name); return kLocal; }
        if (name == "COMMAND") { *local = "*0\r\n"; return kLocal; }
        if (name == "GET" || name == "TTL" || name == "PTTL" || name == "PERSIST") {
            if (!need(c.size() == 2)) return kLocal;
            *key = &c[1];
            return kKey;
        }
        if (name == "SET") {
            if (!need(c.size() >= 3)) return kLocal;
            *key = &c[1];
            return kKey;
        }
        if (name == "EXPIRE" || name == "PEXPIRE") {
            if (!need(c.size() == 3)) return kLocal;
            *key = &c[1];
            return kKey;
        }
        if (name == "RL.ACQUIRE" || name == "RL.PEEK") {  // rate limiter: one bucket = one key = one shard
            if (!need(c.size() == 4 || c.size() == 5)) return kLocal;
            *key = &c[1];
            return kKey;
        }
        if (name == "DEL") {
            if (!need(c.size() >= 2)) return kLocal;
            if (c.size() == 2) { *key = &c[1]; return kKey; }
            return kBarrier;
        }
        if (name == "DBSIZE" || name == "KEYS" || name == "INFO" || name == "SAVE" || name == "RING" || name == "SHARD") return kBarrier;
        *local = resp_error("ERR unknown command '" + c[0] + "'");
        return kLocal;
    }

    static bool is_admin_mutation(const Cmd& c) {
        if (c.size() < 2 || upper(c[0]) != "RING") return false;
        const std::string sub = upper(c[1]);
        return sub == "ADD" || sub == "REMOVE";
    }

    // -- the batch driver -------------------------------------------------------------------

    void process(std::vector<Cmd>& cmds, std::string& out) {
        size_t i = 0;
        while (i < cmds.size()) {
            if (is_admin_mutation(cmds[i])) { out += admin(cmds[i]); ++i; continue; }
            size_t j = i;
            while (j < cmds.size() && !is_admin_mutation(cmds[j])) ++j;
            segment(cmds, i, j, out);
            i = j;
        }
    }

    void segment(std::vector<Cmd>& cmds, size_t from, size_t to, std::string& out) {
        std::shared_ptr<const Route> route = r_.route();
        if (!route->old_ring) {
            // Fast path: pipelined across nodes, no per-key locks. Announce ourselves, then
            // look at the route AGAIN: if a migration was published in between, leave. This
            // pairs with the migration's "publish, then wait for fast_inflight_ == 0".
            r_.fast_inflight_.fetch_add(1);
            route = r_.route();
            if (!route->old_ring) {
                fast_segment(*route, cmds, from, to, out);
                r_.fast_inflight_.fetch_sub(1);
                return;
            }
            r_.fast_inflight_.fetch_sub(1);
        }
        // Migration in progress: one command at a time, moved keys under their stripe lock.
        for (size_t k = from; k < to; ++k) handle_one(*route, cmds[k], out);
    }

    void handle_one(const Route& route, const Cmd& c, std::string& out) {
        const std::string name = upper(c[0]);
        std::string local;
        const std::string* key = nullptr;
        switch (classify(name, c, &local, &key)) {
            case kLocal: out += local; break;
            case kKey: out += key_command(route, name, c, *key); break;
            case kBarrier: out += barrier(route, name, c); break;
        }
    }

    void fast_segment(const Route& route, std::vector<Cmd>& cmds, size_t from, size_t to, std::string& out) {
        struct Item {
            bool forwarded;
            std::string node;
            std::string local;
        };
        std::vector<Item> items;
        std::map<std::string, std::string> sendbuf;  // node -> bytes queued for it

        // Send each node its queued commands (all nodes first), then read every reply in the
        // order the client sent the commands. Per-node replies come back in per-node order,
        // which is the order they were queued, so the k-th forwarded command of a node is
        // answered by the k-th reply read from that node.
        auto flush = [&] {
            std::set<std::string> failed;
            std::map<std::string, std::string> why;
            for (auto& kv : sendbuf) {
                std::string w;
                RespConn* conn = backend(route, kv.first, &w);
                if (!conn || !conn->send_all(kv.second)) {
                    failed.insert(kv.first);
                    why[kv.first] = w.empty() ? "connection lost" : w;
                }
            }
            sendbuf.clear();
            for (Item& it : items) {
                if (!it.forwarded) { out += it.local; continue; }
                if (failed.count(it.node)) {
                    r_.backend_errors_.fetch_add(1, std::memory_order_relaxed);
                    out += node_error(it.node, why[it.node]);
                    continue;
                }
                RespConn* conn = be_[it.node].conn.get();
                const size_t before = out.size();
                if (conn && conn->read_raw(out)) continue;
                out.resize(before);
                failed.insert(it.node);
                why[it.node] = "connection lost";
                r_.backend_errors_.fetch_add(1, std::memory_order_relaxed);
                out += node_error(it.node, "connection lost");
            }
            items.clear();
        };

        for (size_t i = from; i < to; ++i) {
            const Cmd& c = cmds[i];
            const std::string name = upper(c[0]);
            std::string local;
            const std::string* key = nullptr;
            switch (classify(name, c, &local, &key)) {
                case kLocal:
                    items.push_back({false, "", std::move(local)});
                    break;
                case kKey: {
                    const std::string* owner = route.ring->lookup(*key);
                    if (!owner) { items.push_back({false, "", no_nodes()}); break; }
                    if (route.any_replicated && !shard_available(route, *owner)) {
                        items.push_back({false, "", shard_unavailable(*owner)});
                        break;
                    }
                    sendbuf[*owner] += encode_command(c);
                    items.push_back({true, *owner, ""});
                    r_.forwarded_.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                case kBarrier:
                    flush();  // everything before it is answered first: order is preserved
                    out += barrier(route, name, c);
                    break;
            }
        }
        flush();
    }

    // -- single-key commands, with migration awareness ----------------------------------------

    std::mutex& stripe(const std::string& key) { return r_.stripes_[key_hash(key) % kStripes]; }

    std::string key_command(const Route& route, const std::string& name, const Cmd& c, const std::string& key) {
        const std::string* now = route.ring->lookup(key);
        if (!now) return no_nodes();
        if (route.any_replicated) {  // during a migration the old owner matters too
            if (!shard_available(route, *now)) return shard_unavailable(*now);
            const std::string* was = route.old_ring ? route.old_ring->lookup(key) : nullptr;
            if (was && !shard_available(route, *was)) return shard_unavailable(*was);
        }
        const std::string* before = route.old_ring ? route.old_ring->lookup(key) : nullptr;
        if (!before || *before == *now) return forward(route, *now, c);  // not affected by the move

        // The key's owner changed and its data may still be on the old owner. Everything below
        // runs under the key's stripe lock, so a migration step on this key cannot interleave.
        // Because the exchange is synchronous (reply read before the lock is released), "done"
        // really means done on the node.
        std::lock_guard<std::mutex> lock(stripe(key));
        if (name == "GET") {
            std::string r = forward(route, *now, c);
            if (r == "$-1\r\n") return forward(route, *before, c);  // not copied over yet
            return r;
        }
        if (name == "SET") {
            // Remove the old copy FIRST: if this fails we have written nothing, and if the
            // write below fails afterwards the key is simply missing (a cache miss), never
            // stale. The reverse order could leave an old value that migration then copies
            // over the new one.
            std::string r = forward(route, *before, Cmd{"DEL", key});
            if (!r.empty() && r[0] == '-') return r;
            return forward(route, *now, c);
        }
        if (name == "DEL") {
            std::string a = forward(route, *now, c);
            std::string b = forward(route, *before, c);
            long long x = 0, y = 0;
            if (!parse_integer_reply(a, &x)) return a;
            if (!parse_integer_reply(b, &y)) return b;
            return resp_integer((x + y) > 0 ? 1 : 0);
        }
        if (name == "RL.ACQUIRE" || name == "RL.PEEK") {
            // The bucket's state lives on exactly one node: the new owner once migrated, else the old
            // one. A bucket that exists nowhere is created on the NEW owner (so it is not orphaned
            // when the migration ends), which costs one extra PTTL while a migration is running.
            if (forward(route, *now, Cmd{"PTTL", key}) != ":-2\r\n") return forward(route, *now, c);
            if (forward(route, *before, Cmd{"PTTL", key}) != ":-2\r\n") return forward(route, *before, c);
            return forward(route, *now, c);
        }
        // EXPIRE / PEXPIRE / TTL / PTTL / PERSIST: act on whichever node currently holds the key.
        std::string where = forward(route, *now, Cmd{"PTTL", key});
        const std::string& target = (where == ":-2\r\n") ? *before : *now;
        return forward(route, target, c);
    }

    // -- commands that involve several nodes -------------------------------------------------

    std::string barrier(const Route& route, const std::string& name, const Cmd& c) {
        if (name == "DBSIZE") {
            if (route.nodes.empty()) return no_nodes();
            long long total = 0;
            for (const auto& kv : route.nodes) {
                if (!kv.second.available) return shard_unavailable(kv.first);
                std::string r = forward(route, kv.first, c);
                long long v = 0;
                if (!parse_integer_reply(r, &v)) return r;
                total += v;
            }
            return resp_integer(total);
        }
        if (name == "KEYS") {
            if (c.size() != 2) return wrong_args(name);
            std::set<std::string> keys;  // a set: during a migration a key can be visible twice
            for (const auto& kv : route.nodes) {
                std::string why;
                RespConn* conn = backend(route, kv.first, &why);
                Reply rep;
                if (!conn || !conn->call(c, rep) || rep.type != '*') {
                    r_.backend_errors_.fetch_add(1, std::memory_order_relaxed);
                    return node_error(kv.first, why.empty() ? "KEYS failed" : why);
                }
                for (const Reply& k : rep.array) keys.insert(k.str);
            }
            std::string out = "*" + std::to_string(keys.size()) + "\r\n";
            for (const auto& k : keys) out += resp_bulk(k);
            return out;
        }
        if (name == "INFO") return resp_bulk(r_.info());
        if (name == "SAVE") {
            if (c.size() != 1) return wrong_args(name);
            if (route.nodes.empty()) return no_nodes();
            for (const auto& kv : route.nodes) {
                std::string r = forward(route, kv.first, c);
                if (r != "+OK\r\n") return r;
            }
            return resp_simple("OK");
        }
        if (name == "DEL") {  // several keys: one DEL per key through the normal single-key path
            long long total = 0;
            for (size_t i = 1; i < c.size(); ++i) {
                std::string r = key_command(route, "DEL", Cmd{"DEL", c[i]}, c[i]);
                long long v = 0;
                if (!parse_integer_reply(r, &v)) return r;
                total += v;
            }
            return resp_integer(total);
        }
        if (name == "SHARD") {  // SHARD STATUS | SHARD EVENTS  (the failover coordinator's view)
            if (c.size() != 2) return wrong_args(name);
            Coordinator* co = r_.coordinator();
            if (!co) return resp_error("ERR no replicated shards are configured");
            const std::string sub = upper(c[1]);
            if (sub == "STATUS") return resp_bulk(co->status_text());
            if (sub == "EVENTS") {
                std::string out;
                for (const auto& e : co->events()) out += e + "\r\n";
                return resp_bulk(out);
            }
            return resp_error("ERR unknown SHARD subcommand");
        }
        if (name == "RING") {
            if (c.size() < 2) return wrong_args(name);
            const std::string sub = upper(c[1]);
            if (sub == "INFO" && c.size() == 2) return resp_bulk(r_.info());
            if (sub == "LOOKUP" && c.size() == 3) {
                const std::string* o = route.ring->lookup(c[2]);
                return o ? resp_bulk(*o) : resp_null();
            }
            return resp_error("ERR unknown RING subcommand or wrong number of arguments");
        }
        return resp_error("ERR unknown command '" + c[0] + "'");
    }

    // RING ADD name host port [weight] [MIGRATE]   /   RING REMOVE name [MIGRATE]
    std::string admin(const Cmd& c) {
        const std::string sub = upper(c[1]);
        bool migrate = false;
        MigrationReport rep;
        std::string err;
        bool ok;
        if (sub == "ADD") {
            if (c.size() < 5 || c.size() > 7) return wrong_args("ring add");
            RouterNode n;
            n.name = c[2];
            n.host = c[3];
            n.port = std::atoi(c[4].c_str());
            for (size_t i = 5; i < c.size(); ++i) {
                if (upper(c[i]) == "MIGRATE") migrate = true;
                else n.weight = static_cast<uint32_t>(std::max(1, std::atoi(c[i].c_str())));
            }
            ok = r_.add_node(n, migrate, &rep, &err);
        } else {
            if (c.size() < 3 || c.size() > 4) return wrong_args("ring remove");
            if (c.size() == 4) {
                if (upper(c[3]) != "MIGRATE") return resp_error("ERR syntax error");
                migrate = true;
            }
            ok = r_.remove_node(c[2], migrate, &rep, &err);
        }
        if (!ok) return resp_error("ERR " + err);
        if (!migrate) return resp_simple("OK");
        return resp_simple("OK scanned=" + std::to_string(rep.scanned) + " moved=" + std::to_string(rep.moved) +
                           " superseded=" + std::to_string(rep.superseded) + " vanished=" + std::to_string(rep.vanished) +
                           " errors=" + std::to_string(rep.errors));
    }

    Router& r_;
    int fd_;
    std::map<std::string, Backend> be_;
};

// ---- Router -------------------------------------------------------------------------------------

Router::Router(std::vector<RouterNode> nodes, RouterOptions opts) : opts_(opts) {
    auto r = std::make_shared<Route>();
    std::vector<HashRing::Node> ring_nodes;
    for (RouterNode& n : nodes) {
        if (n.weight == 0) n.weight = 1;
        if (r->nodes.count(n.name)) continue;
        ring_nodes.push_back({n.name, n.weight});
        r->nodes[n.name] = std::move(n);
    }
    r->ring = std::make_shared<HashRing>(std::move(ring_nodes), opts_.vnodes);
    r->version = 1;
    // Replicated shards: the coordinator owns their failure detection and failover.
    std::vector<ShardSpec> specs;
    for (const auto& kv : r->nodes) {
        const RouterNode& n = kv.second;
        if (n.replicas.empty()) continue;
        ShardSpec sp;
        sp.shard = n.name;
        sp.members.push_back({n.name + "#0", n.host, n.port});
        for (size_t i = 0; i < n.replicas.size(); ++i)
            sp.members.push_back({n.name + "#" + std::to_string(i + 1), n.replicas[i].host, n.replicas[i].port});
        specs.push_back(std::move(sp));
    }
    for (const auto& sp : specs) r->nodes[sp.shard].available = false;  // fail closed until the coordinator confirms the shard
    r->any_replicated = !specs.empty();
    route_ = std::move(r);
    if (!specs.empty())
        coordinator_ = std::make_unique<Coordinator>(
            std::move(specs), opts_.failover,
            [this](const std::string& s, const std::string& h, int p) { set_shard_primary(s, h, p); },
            [this](const std::string& s, bool a) { set_shard_available(s, a); });
}

Router::~Router() { stop(); }

bool Router::start(std::string* error) {
    auto fail = [&](const std::string& why) {
        if (error) *error = why;
        if (listen_fd_ >= 0) ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    };
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return fail("socket() failed");
    int one = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons(static_cast<uint16_t>(opts_.port));
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0)
        return fail(std::string("bind: ") + std::strerror(errno));
    if (::listen(listen_fd_, 256) != 0) return fail(std::string("listen: ") + std::strerror(errno));
    socklen_t len = sizeof a;
    getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&a), &len);
    port_ = ntohs(a.sin_port);
    running_ = true;
    acceptor_ = std::thread([this] { accept_loop(); });
    if (coordinator_) coordinator_->start();
    return true;
}

void Router::accept_loop() {
    while (running_) {
        pollfd p{listen_fd_, POLLIN, 0};
        const int pr = ::poll(&p, 1, 100);
        {   // reap finished sessions so a long-lived router does not accumulate dead threads
            std::lock_guard<std::mutex> lock(sessions_mu_);
            for (auto it = sessions_.begin(); it != sessions_.end();) {
                if ((*it)->done) {
                    if ((*it)->th.joinable()) (*it)->th.join();
                    it = sessions_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        if (pr <= 0) continue;
        const int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) continue;
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        auto h = std::make_unique<SessionHandle>();
        h->fd = fd;
        SessionHandle* hp = h.get();
        h->th = std::thread([this, hp] {
            {
                Session s(*this, hp->fd);
                s.run();
            }
            {
                std::lock_guard<std::mutex> lock(hp->mu);
                if (hp->fd >= 0) ::close(hp->fd);
                hp->fd = -1;
            }
            hp->done = true;
        });
        std::lock_guard<std::mutex> lock(sessions_mu_);
        sessions_.push_back(std::move(h));
    }
}

void Router::stop() {
    if (!running_.exchange(false)) {
        if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
        return;
    }
    if (coordinator_) coordinator_->stop();
    if (acceptor_.joinable()) acceptor_.join();
    if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
    std::vector<std::unique_ptr<SessionHandle>> sessions;
    {
        std::lock_guard<std::mutex> lock(sessions_mu_);
        sessions.swap(sessions_);
    }
    for (auto& h : sessions) {
        std::lock_guard<std::mutex> lock(h->mu);
        if (h->fd >= 0) ::shutdown(h->fd, SHUT_RDWR);  // wakes the thread's recv()
    }
    for (auto& h : sessions)
        if (h->th.joinable()) h->th.join();
}

std::string Router::owner_of(const std::string& key) const {
    auto r = route();
    const std::string* o = r->ring->lookup(key);
    return o ? *o : std::string();
}

std::string Router::info() const {
    auto r = route();
    std::string s = "# Router\r\nring_version:" + std::to_string(r->version) +
                    "\r\nmigrating:" + (r->old_ring ? "yes" : "no") +
                    "\r\nvnodes_per_weight:" + std::to_string(r->ring->vnodes_per_weight()) +
                    "\r\nring_points:" + std::to_string(r->ring->point_count()) +
                    "\r\nnodes:" + std::to_string(r->ring->node_count()) +
                    "\r\nforwarded_commands:" + std::to_string(forwarded_.load()) +
                    "\r\nbackend_errors:" + std::to_string(backend_errors_.load()) + "\r\n";
    const auto share = r->ring->ownership();
    const auto& nodes = r->ring->nodes();
    for (size_t i = 0; i < nodes.size(); ++i) {
        auto it = r->nodes.find(nodes[i].name);
        char buf[64];
        std::snprintf(buf, sizeof buf, "%.4f", share[i]);
        s += "node:" + nodes[i].name + " addr=" +
             (it != r->nodes.end() ? it->second.host + ":" + std::to_string(it->second.port) : std::string("?")) +
             " weight=" + std::to_string(nodes[i].weight) + " share=" + buf + "\r\n";
    }
    if (coordinator_) {
        s += "# Shards\r\n";
        for (const auto& sh : coordinator_->info())
            s += "shard:" + sh.shard + " state=" + sh.state + " primary=" + sh.primary + " epoch=" + std::to_string(sh.epoch) +
                 " failovers=" + std::to_string(sh.failovers) + "\r\n";
    }
    return s;
}

bool Router::add_node(const RouterNode& node_in, bool migrate, MigrationReport* report, std::string* error) {
    auto fail = [&](const std::string& why) {
        if (error) *error = why;
        return false;
    };
    RouterNode node = node_in;
    if (node.weight == 0) node.weight = 1;
    if (node.name.empty() || node.host.empty() || node.port <= 0 || node.port > 65535)
        return fail("invalid node (need name, host and port)");
    std::lock_guard<std::mutex> admin(admin_mu_);
    auto cur = route();
    if (cur->ring->contains(node.name)) return fail("node '" + node.name + "' is already in the ring");
    auto new_ring = std::make_shared<HashRing>(cur->ring->with_node({node.name, node.weight}));

    if (!migrate || cur->ring->empty()) {
        update_route([&](Route& r) { r.ring = new_ring; r.nodes[node.name] = node; });
        return true;
    }
    // 1. publish "migrating" and wait for every unlocked batch that started earlier to finish
    update_route([&](Route& r) { r.old_ring = r.ring; r.ring = new_ring; r.nodes[node.name] = node; });
    while (fast_inflight_.load() != 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
    // 2. copy keys. Every old node may lose keys to the new one.
    std::vector<std::string> sources;
    for (const auto& n : cur->ring->nodes()) sources.push_back(n.name);
    MigrationReport rep = migrate_keys(*route(), sources);
    if (report) *report = rep;
    // 3. migration over: only the new ring from now on
    update_route([&](Route& r) { r.old_ring.reset(); });
    return true;
}

bool Router::remove_node(const std::string& name, bool migrate, MigrationReport* report, std::string* error) {
    auto fail = [&](const std::string& why) {
        if (error) *error = why;
        return false;
    };
    std::lock_guard<std::mutex> admin(admin_mu_);
    auto cur = route();
    if (!cur->ring->contains(name)) return fail("no such node '" + name + "'");
    if (migrate && cur->ring->node_count() == 1) return fail("cannot migrate the last node's keys anywhere");
    auto new_ring = std::make_shared<HashRing>(cur->ring->without_node(name));

    if (!migrate) {
        update_route([&](Route& r) { r.ring = new_ring; r.nodes.erase(name); });
        return true;
    }
    // the leaving node stays reachable (in r.nodes) while its keys are copied out
    update_route([&](Route& r) { r.old_ring = r.ring; r.ring = new_ring; });
    while (fast_inflight_.load() != 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
    MigrationReport rep = migrate_keys(*route(), {name});
    if (report) *report = rep;
    update_route([&](Route& r) { r.old_ring.reset(); r.nodes.erase(name); });
    return true;
}

// Moves every key stored on a `sources` node to the node the NEW ring assigns it to.
//
// Runs while clients keep working; clients meanwhile use the migrating route (see
// Session::key_command): a moved key is read from its new owner first and from the old one as a
// fallback, writes go to the new owner after deleting the old copy, deletes go to both.
//
// Per batch of keys, under the stripe locks of ALL keys in the batch (taken in ascending order,
// so two lockers cannot deadlock; client threads only ever hold one):
//   1. GET + PTTL each key on the source
//   2. PTTL on the destination: if the destination already holds the key, a client wrote a
//      newer value there; the source copy is stale and is only deleted ("superseded")
//   3. SET key value [PX remaining-ttl] on the destination
//   4. DEL key on the source, only for keys that were copied or superseded
// Because a client operation on such a key needs the same stripe lock and completes its
// network exchange while holding it, steps 1-4 are atomic with respect to every client of this
// router. The remaining time to live travels with the key (PTTL -> PX).
//
// What it does not do: protect against clients that bypass this router (smart clients writing
// straight to nodes), or a second router migrating at the same time. A key left behind because
// a node was unreachable is counted in `errors`; after the ring switch it is an orphan that
// only wastes memory until it expires or is evicted.
MigrationReport Router::migrate_keys(const Route& route, const std::vector<std::string>& sources) {
    MigrationReport rep;
    std::map<std::string, std::unique_ptr<RespConn>> conns;
    auto conn = [&](const std::string& name) -> RespConn* {
        auto& p = conns[name];
        if (!p) p = std::make_unique<RespConn>();
        if (!p->is_open()) {
            auto latest = this->route();  // not the snapshot we started from: a failover may have moved the primary
            auto it = latest->nodes.find(name);
            if (it == latest->nodes.end()) return nullptr;
            std::string why;
            if (!p->connect(it->second.host, it->second.port, opts_.io_timeout_ms, &why)) return nullptr;
        }
        return p.get();
    };
    auto stripe_of = [&](const std::string& key) { return static_cast<size_t>(key_hash(key) % kStripes); };

    struct Item {
        std::string key, dest;
        Reply value, ttl;       // from the source
        bool have = false;      // the source still had the key
        bool dest_has = false;  // the destination already has it
        bool copied = false;
    };

    constexpr size_t kBatch = 64;
    for (const std::string& src : sources) {
        RespConn* s = conn(src);
        Reply keys;
        if (!s || !s->call({"KEYS", "*"}, keys) || keys.type != '*') {
            ++rep.errors;
            continue;
        }
        std::vector<std::pair<std::string, std::string>> todo;  // key -> destination node
        for (const Reply& k : keys.array) {
            ++rep.scanned;
            const std::string* d = route.ring->lookup(k.str);
            if (d && *d != src) todo.emplace_back(k.str, *d);
        }

        for (size_t base = 0; base < todo.size(); base += kBatch) {
            const size_t n = std::min(kBatch, todo.size() - base);
            std::vector<Item> items(n);
            std::vector<size_t> ids;
            for (size_t i = 0; i < n; ++i) {
                items[i].key = todo[base + i].first;
                items[i].dest = todo[base + i].second;
                ids.push_back(stripe_of(items[i].key));
            }
            std::sort(ids.begin(), ids.end());
            ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
            for (size_t id : ids) stripes_[id].lock();
            struct Unlock {
                Router& r;
                const std::vector<size_t>& ids;
                ~Unlock() { for (size_t id : ids) r.stripes_[id].unlock(); }
            } unlock{*this, ids};

            // 1. read from the source (pipelined)
            std::string req;
            for (const Item& it : items) req += encode_command({"GET", it.key}) + encode_command({"PTTL", it.key});
            bool ok = s->send_all(req);
            for (size_t i = 0; ok && i < n; ++i) ok = s->read_reply(items[i].value) && s->read_reply(items[i].ttl);
            if (!ok) { rep.errors += n; continue; }  // the source connection is closed; conn() reopens it
            for (Item& it : items) {
                it.have = !it.value.null && it.value.type == '$' && it.ttl.type == ':' && it.ttl.integer != -2 &&
                          it.ttl.integer != 0;
                if (!it.have) ++rep.vanished;
            }

            // 2. does the destination already hold a (newer) value?
            std::map<std::string, std::vector<size_t>> by_dest;
            for (size_t i = 0; i < n; ++i)
                if (items[i].have) by_dest[items[i].dest].push_back(i);
            for (auto& kv : by_dest) {
                RespConn* d = conn(kv.first);
                std::string q;
                for (size_t i : kv.second) q += encode_command({"PTTL", items[i].key});
                Reply r;
                bool dok = d && d->send_all(q);
                for (size_t i : kv.second) {
                    if (dok && d->read_reply(r) && r.type == ':') items[i].dest_has = r.integer != -2;
                    else { dok = false; items[i].have = false; ++rep.errors; }
                }
            }

            // 3. write what the destination lacks
            for (auto& kv : by_dest) {
                RespConn* d = conn(kv.first);
                std::string w;
                std::vector<size_t> sent;
                for (size_t i : kv.second) {
                    Item& it = items[i];
                    if (!it.have || it.dest_has) continue;
                    Cmd c{"SET", it.key, it.value.str};
                    if (it.ttl.integer > 0) { c.push_back("PX"); c.push_back(std::to_string(it.ttl.integer)); }
                    w += encode_command(c);
                    sent.push_back(i);
                }
                if (sent.empty()) continue;
                bool wok = d && d->send_all(w);
                Reply r;
                for (size_t i : sent) {
                    if (wok && d->read_reply(r) && r.ok()) items[i].copied = true;
                    else { wok = false; ++rep.errors; }
                }
            }

            // 4. remove the source copies that are now redundant
            std::string del;
            std::vector<size_t> deleted;
            for (size_t i = 0; i < n; ++i) {
                Item& it = items[i];
                if (!it.have) continue;
                if (it.copied || it.dest_has) { del += encode_command({"DEL", it.key}); deleted.push_back(i); }
            }
            if (!deleted.empty()) {
                bool dok = s->send_all(del);
                Reply r;
                for (size_t i : deleted) {
                    if (dok && s->read_reply(r) && r.type == ':') {
                        if (items[i].copied) ++rep.moved; else ++rep.superseded;
                    } else {
                        dok = false;
                        ++rep.errors;
                    }
                }
            }
        }
    }
    return rep;
}
