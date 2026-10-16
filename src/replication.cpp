#include "replication.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>

#include "record_apply.hpp"
#include "resp.hpp"
#include "resp_client.hpp"

namespace {

int64_t system_wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
int64_t steady_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

bool send_fd(int fd, const std::string& s) {
    size_t off = 0;
    while (off < s.size()) {
        const ssize_t n = ::send(fd, s.data() + off, s.size() - off, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;  // includes SO_SNDTIMEO expiry: the peer is not draining its socket
        off += static_cast<size_t>(n);
    }
    return true;
}

std::vector<std::string> strings_of(const Reply& r) {
    std::vector<std::string> v;
    v.reserve(r.array.size());
    for (const Reply& e : r.array) v.push_back(e.str);
    return v;
}

std::string frame_record(const ReplRecord& r) {
    return encode_command({"R", std::to_string(r.offset), r.payload});
}

}  // namespace

// =============================================================================================
// Primary side
// =============================================================================================

struct ReplicationPrimary::Link {
    std::mutex fd_mu;  // guards fd (stop() shuts it down from another thread)
    int fd = -1;
    std::string peer;
    std::atomic<int> state{0};  // 0 handshake, 1 full-sync, 2 streaming
    std::atomic<uint64_t> sent{0}, acked{0};
    std::atomic<int64_t> last_ack_ms{0};
    std::atomic<bool> done{false};
    uint64_t kill_gen = 0;
    std::thread th, ack_th;

    void shutdown_socket() {
        std::lock_guard<std::mutex> g(fd_mu);
        if (fd >= 0) ::shutdown(fd, SHUT_RDWR);
    }
};

ReplicationPrimary::ReplicationPrimary(Engine& engine, ReplicationLog& log) : ReplicationPrimary(engine, log, Options{}) {}

ReplicationPrimary::ReplicationPrimary(Engine& engine, ReplicationLog& log, Options opts)
    : engine_(engine), log_(log), opts_(std::move(opts)) {
    if (!opts_.wall_ms) opts_.wall_ms = system_wall_ms;
}

ReplicationPrimary::~ReplicationPrimary() { stop(); }

bool ReplicationPrimary::start(std::string* error) {
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
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) return fail(std::string("bind: ") + std::strerror(errno));
    if (::listen(listen_fd_, 64) != 0) return fail(std::string("listen: ") + std::strerror(errno));
    socklen_t len = sizeof a;
    getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&a), &len);
    port_ = ntohs(a.sin_port);
    running_ = true;
    acceptor_ = std::thread([this] { accept_loop(); });
    return true;
}

void ReplicationPrimary::accept_loop() {
    while (running_) {
        pollfd p{listen_fd_, POLLIN, 0};
        const int pr = ::poll(&p, 1, 100);
        {   // reap finished links
            std::lock_guard<std::mutex> g(mu_);
            for (auto it = links_.begin(); it != links_.end();) {
                if ((*it)->done) {
                    if ((*it)->th.joinable()) (*it)->th.join();
                    it = links_.erase(it);
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
        timeval tv{opts_.send_timeout_ms / 1000, (opts_.send_timeout_ms % 1000) * 1000};
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
        auto link = std::make_shared<Link>();
        link->fd = fd;
        link->kill_gen = kill_generation_.load();
        sockaddr_in pa{};
        socklen_t pl = sizeof pa;
        if (getpeername(fd, reinterpret_cast<sockaddr*>(&pa), &pl) == 0) {
            char ip[64];
            inet_ntop(AF_INET, &pa.sin_addr, ip, sizeof ip);
            link->peer = std::string(ip) + ":" + std::to_string(ntohs(pa.sin_port));
        }
        link->th = std::thread([this, link] { serve(link); });
        std::lock_guard<std::mutex> g(mu_);
        links_.push_back(std::move(link));
    }
}

void ReplicationPrimary::stop() {
    if (!running_.exchange(false)) {
        if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
        return;
    }
    if (acceptor_.joinable()) acceptor_.join();
    if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
    std::vector<std::shared_ptr<Link>> links;
    {
        std::lock_guard<std::mutex> g(mu_);
        links.swap(links_);
    }
    for (auto& l : links) l->shutdown_socket();
    for (auto& l : links)
        if (l->th.joinable()) l->th.join();
}

void ReplicationPrimary::disconnect_replicas() { kill_generation_.fetch_add(1); }

std::vector<ReplicationPrimary::ReplicaInfo> ReplicationPrimary::replicas() const {
    std::vector<ReplicaInfo> out;
    std::lock_guard<std::mutex> g(mu_);
    const int64_t now = steady_ms();
    for (const auto& l : links_) {
        if (l->done) continue;
        ReplicaInfo r;
        r.peer = l->peer;
        r.state = l->state == 2 ? "streaming" : (l->state == 1 ? "full-sync" : "handshake");
        r.sent = l->sent;
        r.acked = l->acked;
        r.last_ack_ms_ago = l->last_ack_ms ? now - l->last_ack_ms.load() : -1;
        out.push_back(std::move(r));
    }
    return out;
}

ReplicationPrimary::Stats ReplicationPrimary::stats() const {
    return Stats{n_links_, n_full_, n_partial_, n_too_old_, n_send_failed_};
}

// One replica connection: handshake, then (snapshot and) stream until anything goes wrong.
void ReplicationPrimary::serve(std::shared_ptr<Link> link) {
    ++n_links_;
    auto rc = std::make_unique<RespConn>();
    rc->adopt(::dup(link->fd), opts_.handshake_timeout_ms);  // reading half; the sender uses link->fd
    auto finish = [&] {
        link->shutdown_socket();
        if (link->ack_th.joinable()) link->ack_th.join();
        {
            std::lock_guard<std::mutex> g(link->fd_mu);
            if (link->fd >= 0) ::close(link->fd);
            link->fd = -1;
        }
        link->done = true;
    };

    Reply req;
    if (!rc->read_reply(req) || req.type != '*' || req.array.size() != 4 || req.array[0].str != "SYNC") {
        send_fd(link->fd, "-ERR expected SYNC <replid> <next_offset> <epoch>\r\n");
        return finish();
    }
    const std::string their_id = req.array[1].str;
    const uint64_t next = std::strtoull(req.array[2].str.c_str(), nullptr, 10);
    const std::string myid = log_.replid();
    const int64_t epoch = epoch_.load();

    // Partial resync only if the replica is on THIS lineage and its next record is still retained
    // (or it is exactly caught up). Anything else, including a replica claiming to be ahead, gets
    // a full resync.
    bool partial = false;
    if (their_id == myid && next >= 1) {
        std::vector<ReplRecord> probe;
        partial = log_.read_from(next, 1, &probe) == ReplicationLog::ReadStatus::Ok;
    }

    uint64_t cursor;
    if (partial) {
        ++n_partial_;
        if (!send_fd(link->fd, encode_command({"CONTINUE", myid, std::to_string(epoch), std::to_string(log_.last_offset())})))
            return finish();
        cursor = next;
    } else {
        ++n_full_;
        link->state = 1;
        uint64_t S = 0;
        if (!stream_snapshot(*link, &S)) { ++n_send_failed_; return finish(); }
        cursor = S + 1;
    }
    link->state = 2;
    link->sent = cursor - 1;
    link->last_ack_ms = steady_ms();

    // From here on the replica's ACKs are read on a second thread (rc is only read by it).
    rc->set_io_timeout(3000);  // streaming phase: the replica ACKs at least every ping_ms
    link->ack_th = std::thread([link, conn = std::move(rc)]() mutable {
        Reply r;
        while (conn->read_reply(r)) {
            if (r.type == '*' && r.array.size() == 2 && r.array[0].str == "ACK") {
                link->acked = std::strtoull(r.array[1].str.c_str(), nullptr, 10);
                link->last_ack_ms = steady_ms();
            }
        }
        link->shutdown_socket();  // replica gone or silent: stop the sender too
    });

    stream_records(*link, cursor);
    finish();
}

// Sends FULLSYNC, the (fuzzy) snapshot and SNAPEND. *S is the offset the replica must continue from.
bool ReplicationPrimary::stream_snapshot(Link& l, uint64_t* S) {
    // Every record <= S was appended after its mutation was applied to the engine (invariant I2),
    // so a snapshot taken from now on reflects at least everything up to S.
    *S = log_.last_offset();
    if (!send_fd(l.fd, encode_command({"FULLSYNC", log_.replid(), std::to_string(epoch_.load()), std::to_string(*S)}))) return false;
    std::string buf;
    uint64_t count = 0;
    bool ok = true;
    engine_.for_each([&](const std::string& k, const std::string& v, int64_t ttl) {
        if (!ok || !running_) { ok = false; return; }
        std::vector<std::string> a{"SET", k, v};
        if (ttl >= 0) { a.push_back("PXAT"); a.push_back(std::to_string(opts_.wall_ms() + ttl)); }
        buf += encode_command(a);
        ++count;
        if (buf.size() >= (64u << 10)) { ok = send_fd(l.fd, buf); buf.clear(); }
        if ((count & 63) == 0) {
            const int pause = faults_.snapshot_pause_us.load();
            if (pause > 0) std::this_thread::sleep_for(std::chrono::microseconds(pause));
        }
    });
    if (!ok) return false;
    buf += encode_command({"SNAPEND", std::to_string(count)});
    return send_fd(l.fd, buf);
}

bool ReplicationPrimary::stream_records(Link& l, uint64_t cursor) {
    std::vector<ReplRecord> batch;
    while (running_) {
        if (l.kill_gen != kill_generation_.load()) return false;  // disconnect_replicas()
        batch.clear();
        const auto st = log_.read_from(cursor, opts_.batch_bytes, &batch);
        if (st == ReplicationLog::ReadStatus::TooOld) { ++n_too_old_; return false; }  // fell behind the backlog
        if (st == ReplicationLog::ReadStatus::Future) return false;
        if (batch.empty()) {
            if (!log_.wait_beyond(cursor - 1, opts_.ping_ms)) {
                if (!send_fd(l.fd, encode_command({"PING", std::to_string(log_.last_offset())}))) { ++n_send_failed_; return false; }
            }
            continue;
        }
        const int delay = faults_.send_delay_ms.load();
        if (delay > 0) std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        std::string out;
        for (const ReplRecord& r : batch) {
            int d = faults_.drop_next.load();
            if (d > 0 && faults_.drop_next.compare_exchange_strong(d, d - 1)) continue;  // injected loss
            const std::string frame = frame_record(r);
            out += frame;
            int dup = faults_.duplicate_next.load();
            if (dup > 0 && faults_.duplicate_next.compare_exchange_strong(dup, dup - 1)) out += frame;  // injected duplicate
        }
        if (!send_fd(l.fd, out)) { ++n_send_failed_; return false; }
        cursor += batch.size();
        l.sent = cursor - 1;
    }
    return false;
}

// =============================================================================================
// Replica side
// =============================================================================================

ReplicationReplica::ReplicationReplica(Engine& engine, ReplicationLog& log, Options opts)
    : engine_(engine), log_(log), opts_(std::move(opts)) {
    if (!opts_.wall_ms) opts_.wall_ms = system_wall_ms;
    engine_.set_eviction_enabled(false);  // the primary's evictions arrive as DEL records
}

ReplicationReplica::~ReplicationReplica() { stop(); }

void ReplicationReplica::start() {
    if (thread_.joinable()) return;
    stop_ = false;
    thread_ = std::thread([this] { run(); });
}

void ReplicationReplica::stop() {
    stop_ = true;
    drop_link();
    if (thread_.joinable()) thread_.join();
}

void ReplicationReplica::drop_link() {
    std::lock_guard<std::mutex> g(mu_);
    const int fd = cur_fd_.load();
    if (fd >= 0) ::shutdown(fd, SHUT_RDWR);
}

ReplicationReplica::Status ReplicationReplica::status() const {
    std::lock_guard<std::mutex> g(mu_);
    Status s = status_;
    s.applied = log_.last_offset();
    s.replid = log_.replid();
    return s;
}

bool ReplicationReplica::wait_applied(uint64_t offset, int timeout_ms) const {
    const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < end) {
        if (log_.last_offset() >= offset) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return log_.last_offset() >= offset;
}

bool ReplicationReplica::wait_online(int timeout_ms) const {
    const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < end) {
        if (status().state == State::Online) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return status().state == State::Online;
}

void ReplicationReplica::run() {
    int backoff = opts_.reconnect_min_ms;
    bool first = true;
    while (!stop_) {
        while (hold_reconnect_ && !stop_) std::this_thread::sleep_for(std::chrono::milliseconds(2));
        if (stop_) break;
        if (!first) {
            std::lock_guard<std::mutex> g(mu_);
            ++status_.reconnects;
        }
        first = false;
        const bool reached = session();
        {
            std::lock_guard<std::mutex> g(mu_);
            status_.state = State::Disconnected;
        }
        if (stop_) break;
        backoff = reached ? opts_.reconnect_min_ms : std::min(opts_.reconnect_max_ms, backoff * 2);
        for (int waited = 0; waited < backoff && !stop_; waited += 2) std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}

// One connection's lifetime. Returns true if it got as far as streaming (resets the backoff).
bool ReplicationReplica::session() {
    RespConn rc;
    if (!rc.connect(opts_.host, opts_.port, opts_.link_timeout_ms)) return false;
    {
        std::lock_guard<std::mutex> g(mu_);
        cur_fd_ = rc.fd();
        status_.state = State::Syncing;
    }
    struct FdGuard {  // declared after rc: destroyed first, so drop_link() never sees a closed fd
        ReplicationReplica& r;
        ~FdGuard() { std::lock_guard<std::mutex> g(r.mu_); r.cur_fd_ = -1; }
    } guard{*this};

    if (!rc.send_all(encode_command({"SYNC", log_.replid(), std::to_string(log_.last_offset() + 1), std::to_string(epoch_)})))
        return false;
    Reply h;
    if (!rc.read_reply(h) || h.type != '*' || h.array.size() < 4) return false;  // includes an -ERR line
    const std::string kind = h.array[0].str;
    const std::string their_id = h.array[1].str;
    uint64_t primary_offset = 0;

    if (kind == "CONTINUE") {
        if (their_id != log_.replid()) return false;  // protocol violation
        primary_offset = std::strtoull(h.array[3].str.c_str(), nullptr, 10);
        std::lock_guard<std::mutex> g(mu_);
        ++status_.partial_syncs;
    } else if (kind == "FULLSYNC") {
        const uint64_t S = std::strtoull(h.array[3].str.c_str(), nullptr, 10);
        {
            std::lock_guard<std::mutex> g(mu_);
            ++status_.full_syncs;
        }
        engine_.clear();  // SYNCING: nothing below is visible as "a replica state" until SNAPEND
        for (;;) {
            Reply e;
            while (paused_ && !stop_) std::this_thread::sleep_for(std::chrono::milliseconds(2));
            if (stop_ || !rc.read_reply(e) || e.type != '*' || e.array.empty()) return false;
            if (e.array[0].str == "SNAPEND") break;
            if (apply_logical_record(engine_, strings_of(e), wall()) == ApplyResult::Invalid) return false;
        }
        log_.reset(their_id, S);  // from now on this node's offsets are the primary's
        primary_offset = S;
    } else {
        return false;
    }
    {
        std::lock_guard<std::mutex> g(mu_);
        status_.primary_offset = std::max(status_.primary_offset, primary_offset);
        status_.state = log_.last_offset() >= status_.primary_offset ? State::Online : State::Syncing;
    }

    bool dirty = true;
    int unacked = 0;
    auto last_ack = std::chrono::steady_clock::now();
    if (!rc.send_all(encode_command({"ACK", std::to_string(log_.last_offset())}))) return true;
    dirty = false;
    while (!stop_) {
        while (paused_ && !stop_) std::this_thread::sleep_for(std::chrono::milliseconds(2));
        Reply r;
        if (stop_ || !rc.read_reply(r) || r.type != '*' || r.array.empty()) return true;  // timeout, EOF, error
        const std::string& tag = r.array[0].str;
        if (tag == "R" && r.array.size() == 3) {
            const uint64_t offset = std::strtoull(r.array[1].str.c_str(), nullptr, 10);
            const uint64_t expected = log_.last_offset() + 1;
            if (offset < expected) {                      // already applied: duplicate delivery
                std::lock_guard<std::mutex> g(mu_);
                ++status_.duplicates_skipped;
                continue;
            }
            if (offset > expected) {                      // a record is missing: resynchronise
                std::lock_guard<std::mutex> g(mu_);
                ++status_.gaps;
                return true;
            }
            const std::string& payload = r.array[2].str;
            ParseResult pr = parse_command(payload);
            if (pr.status != ParseStatus::Ok || apply_logical_record(engine_, pr.args, wall()) == ApplyResult::Invalid) {
                std::lock_guard<std::mutex> g(mu_);
                ++status_.invalid;
                return true;
            }
            log_.append_replicated(offset, payload);      // mirror, same offset
            {
                std::lock_guard<std::mutex> g(mu_);
                ++status_.records_applied;
                status_.primary_offset = std::max(status_.primary_offset, offset);
            }
            dirty = true;
            ++unacked;
        } else if (tag == "PING" && r.array.size() == 2) {
            std::lock_guard<std::mutex> g(mu_);
            status_.primary_offset = std::max<uint64_t>(status_.primary_offset, std::strtoull(r.array[1].str.c_str(), nullptr, 10));
            dirty = true;
        }
        {
            std::lock_guard<std::mutex> g(mu_);
            status_.state = log_.last_offset() >= status_.primary_offset ? State::Online : State::Syncing;
        }
        // Acknowledge when everything that had already arrived is applied (not per record), but
        // also every 64 records / 20 ms under sustained load, so the primary always sees progress.
        const auto nowt = std::chrono::steady_clock::now();
        if (dirty && (!rc.has_pending_input() || unacked >= 64 || nowt - last_ack >= std::chrono::milliseconds(20))) {
            if (!rc.send_all(encode_command({"ACK", std::to_string(log_.last_offset())}))) return true;
            dirty = false;
            unacked = 0;
            last_ack = nowt;
        }
    }
    return true;
}
