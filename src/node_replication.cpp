#include <fcntl.h>
#include <unistd.h>

#include <fstream>
#include "node_replication.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <sstream>

#include "resp.hpp"

namespace {

int64_t steady_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

std::string upper(std::string s) {
    for (char& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return s;
}

bool parse_i64(const std::string& s, int64_t* out) {
    char* end = nullptr;
    const long long v = std::strtoll(s.c_str(), &end, 10);
    if (end == s.c_str() || *end != '\0') return false;
    *out = v;
    return true;
}

}  // namespace

ReplicationService::ReplicationService(Engine& engine, NodeReplicationOptions opts)
    : engine_(engine),
      opts_(std::move(opts)),
      log_(ReplicationLog::Options{opts_.backlog_bytes, {}}),
      gate_(&log_, /*enabled=*/opts_.replicaof_host.empty()) {
    role_ = opts_.replicaof_host.empty() ? 0 : 1;
    epoch_ = opts_.initial_epoch;
    if (!opts_.epoch_file.empty()) {
        std::ifstream f(opts_.epoch_file);
        long long stored = 0;
        if (f >> stored && stored > epoch_) epoch_ = stored;
    }
    rid_ = opts_.replica_id.empty() ? ReplicationLog::new_replid() : opts_.replica_id;
    last_host_ = opts_.replicaof_host;
    last_port_ = opts_.replicaof_port;
}

ReplicationService::~ReplicationService() { stop(); }

// write-temp, fsync, rename, fsync the directory: a crash leaves either the old or the new value
void ReplicationService::store_epoch(int64_t e) const {
    if (opts_.epoch_file.empty()) return;
    const std::string tmp = opts_.epoch_file + ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { std::fprintf(stderr, "warning: cannot store the epoch in %s\n", tmp.c_str()); return; }
    const std::string text = std::to_string(e) + "\n";
    bool ok = ::write(fd, text.data(), text.size()) == static_cast<ssize_t>(text.size()) && ::fsync(fd) == 0;
    ::close(fd);
    if (ok) ok = ::rename(tmp.c_str(), opts_.epoch_file.c_str()) == 0;
    const size_t slash = opts_.epoch_file.rfind('/');
    int dfd = ::open(slash == std::string::npos ? "." : opts_.epoch_file.substr(0, slash).c_str(), O_RDONLY);
    if (dfd >= 0) { ::fsync(dfd); ::close(dfd); }
    if (!ok) std::fprintf(stderr, "warning: cannot store the epoch in %s\n", opts_.epoch_file.c_str());
}

void ReplicationService::event(const std::string& msg) const {
    if (opts_.log_events)
        std::fprintf(stderr, "[repl role=%s epoch=%lld] %s\n", role() == Role::Primary ? "primary" : "replica",
                     static_cast<long long>(epoch()), msg.c_str());
}

bool ReplicationService::start_primary(std::string* err) {
    ReplicationPrimary::Options o;
    o.port = opts_.repl_port;
    o.bind = opts_.repl_bind;
    o.send_timeout_ms = opts_.send_timeout_ms;
    auto p = std::make_shared<ReplicationPrimary>(engine_, log_, o);
    p->set_epoch(epoch());
    p->set_ack_callback([this] { notify_ack_listeners(); });
    p->set_stale_callback([this](int64_t newer) {
        if (newer > epoch() && role() == Role::Primary) {  // (a report racing with a demotion is moot)
            fenced_ = true;  // a replica has seen a newer epoch: another node was promoted. Stop writing.
            event("FENCED: a replica reported epoch " + std::to_string(newer) + "; refusing writes");
        }
    });
    if (!p->start(err)) return false;
    std::atomic_store(&primary_, p);
    return true;
}

void ReplicationService::start_replica(const std::string& host, int port) {
    ReplicationReplica::Options o;
    o.host = host;
    o.port = port;
    o.link_timeout_ms = opts_.link_timeout_ms;
    o.replica_id = rid_;
    auto r = std::make_shared<ReplicationReplica>(engine_, log_, o);
    r->set_epoch(epoch());
    last_host_ = host;
    last_port_ = port;
    std::atomic_store(&replica_, r);
    r->start();
}

bool ReplicationService::start(std::string* error) {
    std::lock_guard<std::mutex> g(ctl_mu_);
    if (role() == Role::Replica) {
        start_replica(opts_.replicaof_host, opts_.replicaof_port);
        return true;
    }
    if (opts_.repl_port >= 0) return start_primary(error);
    return true;
}

void ReplicationService::stop() {
    std::lock_guard<std::mutex> g(ctl_mu_);
    if (auto r = std::atomic_load(&replica_)) r->stop();
    if (auto p = std::atomic_load(&primary_)) p->stop();
    log_.close();
}

int ReplicationService::repl_port() const {
    auto p = std::atomic_load(&primary_);
    return p ? p->port() : -1;
}

int64_t ReplicationService::lease_remaining_ms() const {
    const int64_t e = lease_expiry_ms_.load(std::memory_order_acquire);
    return e == 0 ? 0 : e - steady_ms();
}

// ---- command gating ---------------------------------------------------------------------------------------

bool ReplicationService::write_allowed(std::string* error) const {
    if (role() == Role::Replica) {
        *error = "READONLY You can't write against a read only replica.";
        return false;
    }
    if (fenced()) {
        *error = "READONLY This node was fenced by a newer epoch and waits to be demoted.";
        return false;
    }
    if (opts_.require_lease && steady_ms() >= lease_expiry_ms_.load(std::memory_order_acquire)) {
        *error = "READONLY No valid write lease (not granted or expired).";
        return false;
    }
    if (opts_.sync_replicas > 0) {  // sync N: refuse up front rather than accept a write nobody can confirm
        auto p = std::atomic_load(&primary_);
        const size_t have = p ? p->in_sync_replicas(opts_.max_ack_age_ms) : 0;
        if (have < static_cast<size_t>(opts_.sync_replicas)) {
            *error = "NOREPLICAS Not enough in-sync replicas to write (need " + std::to_string(opts_.sync_replicas) +
                     ", have " + std::to_string(have) + ")";
            return false;
        }
    }
    return true;
}

bool ReplicationService::read_allowed(std::string* error) const {
    if (role() == Role::Replica) {
        auto r = std::atomic_load(&replica_);
        if (r && !r->readable()) {
            *error = "LOADING the replica is synchronizing with its primary";
            return false;
        }
    }
    return true;
}

// Called when a held reply could be released. A write acknowledged after the lease lapsed would
// be a write the new primary may never see, so the lease is checked once more at release time.
bool ReplicationService::release_ok() const {
    if (role() != Role::Primary || fenced()) return false;
    if (opts_.require_lease && steady_ms() >= lease_expiry_ms_.load(std::memory_order_acquire)) return false;
    return true;
}

uint64_t ReplicationService::take_repl_pending() {
    const uint64_t o = ReplicationLog::take_thread_offset();  // always clear: it must not leak into the next batch
    return opts_.sync_replicas > 0 ? o : 0;
}

uint64_t ReplicationService::repl_acked() const {
    auto p = std::atomic_load(&primary_);
    if (opts_.sync_replicas <= 0 || !p) return UINT64_MAX;
    return p->acked_by(static_cast<size_t>(opts_.sync_replicas), opts_.max_ack_age_ms);
}

int ReplicationService::add_ack_listener(std::function<void()> fn) {
    std::lock_guard<std::mutex> g(lmu_);
    const int id = next_listener_++;
    ack_listeners_[id] = std::move(fn);
    return id;
}

void ReplicationService::remove_ack_listener(int id) {
    std::lock_guard<std::mutex> g(lmu_);
    ack_listeners_.erase(id);
}

void ReplicationService::notify_ack_listeners() {
    std::lock_guard<std::mutex> g(lmu_);
    for (auto& kv : ack_listeners_) kv.second();  // each just writes an eventfd
}

// ---- role changes -----------------------------------------------------------------------------------------------

bool ReplicationService::grant_lease(int64_t epoch_in, int64_t ms, std::string* err) {
    if (role() != Role::Primary) { *err = "not a primary"; return false; }
    if (fenced()) { *err = "FENCED this node saw a newer epoch"; return false; }
    if (epoch_in != epoch()) { *err = "EPOCH " + std::to_string(epoch()); return false; }  // the coordinator must demote us
    if (ms <= 0) { lease_expiry_ms_ = 0; return true; }  // revocation
    // Trust the lease for less than the coordinator assumes: L*(1-eps) measured from NOW (the
    // moment of receipt, which is not earlier than the moment it was sent).
    lease_expiry_ms_.store(steady_ms() + static_cast<int64_t>(static_cast<double>(ms) * (1.0 - opts_.lease_eps)), std::memory_order_release);
    return true;
}

// replica -> primary. See docs/design/replication.md section 5.
bool ReplicationService::promote(int64_t new_epoch, std::string* reply_info, std::string* err) {
    std::lock_guard<std::mutex> g(ctl_mu_);
    if (role() == Role::Primary) { *err = "already a primary"; return false; }
    if (new_epoch <= epoch()) { *err = "epoch " + std::to_string(new_epoch) + " is not higher than " + std::to_string(epoch()); return false; }
    if (opts_.repl_port < 0) { *err = "this node has no --repl-port, so it could not serve replicas after promotion"; return false; }
    auto rep = std::atomic_load(&replica_);
    if (!rep || !rep->readable()) {
        *err = "not promotable: this replica has not completed its synchronisation";  // would promote a half-loaded dataset
        return false;
    }
    rep->stop();  // joins the applier: from here no more records are applied, so applied offset == log offset
    std::atomic_store(&replica_, std::shared_ptr<ReplicationReplica>());
    engine_.set_eviction_enabled(true);
    const int64_t old_epoch = epoch();
    const std::string old_replid = log_.replid();
    log_.promote_lineage(ReplicationLog::new_replid());  // new lineage, offsets continue
    epoch_ = new_epoch;
    if (!start_primary(err)) {
        // Could not open the replication port: undo, and go back to being a replica of the old primary.
        epoch_ = old_epoch;
        log_.set_replid(old_replid);
        start_replica(last_host_, last_port_);
        return false;
    }
    fenced_ = false;
    store_epoch(new_epoch);
    lease_expiry_ms_ = 0;  // a new primary has NO lease until the coordinator grants one
    gate_.set_enabled(true);
    role_ = 0;
    ++transitions_;
    const uint64_t off = log_.last_offset();
    event("PROMOTED at offset " + std::to_string(off) + " replid " + log_.replid());
    if (reply_info) *reply_info = std::to_string(off) + " " + log_.replid() + " " + std::to_string(repl_port());
    return true;
}

// primary -> replica (demotion), or re-point an existing replica at another primary.
bool ReplicationService::replicate_from(const std::string& host, int port, int64_t epoch_in, std::string* err) {
    std::lock_guard<std::mutex> g(ctl_mu_);
    if (epoch_in < epoch()) { *err = "epoch " + std::to_string(epoch_in) + " is older than " + std::to_string(epoch()); return false; }
    if (role() == Role::Primary) {
        // Demotion. Stop accepting writes FIRST, then tear down the primary side. Whatever this node
        // wrote that no replica has is discarded by the forced full resynchronisation below: data
        // never flows from a demoted primary to the new one.
        role_ = 1;
        gate_.set_enabled(false);
        lease_expiry_ms_ = 0;
        if (auto p = std::atomic_load(&primary_)) p->stop();  // joins the link threads: no stale report can arrive after this
        std::atomic_store(&primary_, std::shared_ptr<ReplicationPrimary>());
        fenced_ = false;
        log_.reset(ReplicationLog::new_replid(), 0);  // unknown lineage: the new primary will send a snapshot
        ++transitions_;
        epoch_ = epoch_in;
        event("DEMOTED, replicating from " + host + ":" + std::to_string(port) + " (full resync)");
    } else {
        if (auto r = std::atomic_load(&replica_)) r->stop();
        epoch_ = epoch_in;
        event("re-pointed to " + host + ":" + std::to_string(port));
    }
    start_replica(host, port);
    return true;
}

// ---- control commands ----------------------------------------------------------------------------------------------

std::string ReplicationService::control(const std::vector<std::string>& args, bool* handled) {
    *handled = true;
    const std::string cmd = upper(args[0]);
    std::string err;
    if (cmd == "ROLE") {
        auto p = std::atomic_load(&primary_);
        auto r = std::atomic_load(&replica_);
        ReplicationReplica::Status s{};
        if (r) s = r->status();
        const bool primary_role = role() == Role::Primary;
        const std::vector<std::string> v = {
            primary_role ? "master" : "slave",
            std::to_string(epoch()),
            log_.replid(),
            std::to_string(log_.last_offset()),
            fenced() ? "1" : "0",
            std::to_string(opts_.require_lease ? lease_remaining_ms() : -1),
            primary_role ? "-" : (r && r->readable() ? "synced" : "syncing"),
            std::to_string(p ? p->port() : -1),
            std::to_string(primary_role ? 0 : s.lag()),
            std::to_string(primary_role ? (p ? p->in_sync_replicas(opts_.max_ack_age_ms) : 0) : 0),
        };
        std::string out = "*" + std::to_string(v.size()) + "\r\n";
        for (const auto& e : v) out += resp_bulk(e);
        return out;
    }
    if (cmd == "LEASE") {
        int64_t e, ms;
        if (args.size() != 3 || !parse_i64(args[1], &e) || !parse_i64(args[2], &ms)) return resp_error("ERR usage: LEASE <epoch> <ms>");
        return grant_lease(e, ms, &err) ? resp_simple("OK") : resp_error("ERR " + err);
    }
    if (cmd == "PROMOTE") {
        int64_t e;
        if (args.size() != 2 || !parse_i64(args[1], &e)) return resp_error("ERR usage: PROMOTE <new_epoch>");
        std::string info;
        return promote(e, &info, &err) ? resp_simple("OK " + info) : resp_error("ERR " + err);
    }
    if (cmd == "REPLICAOF") {
        int64_t port, e;
        if (args.size() != 4 || !parse_i64(args[2], &port) || !parse_i64(args[3], &e)) return resp_error("ERR usage: REPLICAOF <host> <port> <epoch>");
        return replicate_from(args[1], static_cast<int>(port), e, &err) ? resp_simple("OK") : resp_error("ERR " + err);
    }
    *handled = false;
    return "";
}

// ---- INFO --------------------------------------------------------------------------------------------------------------

std::string ReplicationService::info() const {
    std::ostringstream o;
    o << "# Replication\r\n";
    o << "epoch:" << epoch() << "\r\n"
      << "fenced:" << (fenced() ? "yes" : "no") << "\r\n"
      << "require_lease:" << (opts_.require_lease ? "yes" : "no") << "\r\n"
      << "lease_remaining_ms:" << (opts_.require_lease ? lease_remaining_ms() : -1) << "\r\n"
      << "role_transitions:" << transitions_.load() << "\r\n";
    if (role() == Role::Replica) {
        auto r = std::atomic_load(&replica_);
        ReplicationReplica::Status s{};
        if (r) s = r->status();
        const char* st = s.state == ReplicationReplica::State::Online ? "online"
                         : s.state == ReplicationReplica::State::Syncing ? "syncing" : "disconnected";
        o << "role:slave\r\n"
          << "master_host:" << last_host_ << "\r\n"
          << "master_port:" << last_port_ << "\r\n"
          << "master_link_status:" << (s.state == ReplicationReplica::State::Disconnected ? "down" : "up") << "\r\n"
          << "replica_state:" << st << "\r\n"
          << "replica_readable:" << (r && r->readable() ? "yes" : "no") << "\r\n"
          << "repl_id:" << s.replid << "\r\n"
          << "repl_offset:" << s.applied << "\r\n"
          << "master_offset_seen:" << s.primary_offset << "\r\n"
          << "lag_records:" << s.lag() << "\r\n"
          << "lag_ms:" << s.behind_ms << "\r\n"
          << "full_syncs:" << s.full_syncs << "\r\n"
          << "partial_syncs:" << s.partial_syncs << "\r\n"
          << "reconnects:" << s.reconnects << "\r\n"
          << "records_applied:" << s.records_applied << "\r\n"
          << "duplicates_skipped:" << s.duplicates_skipped << "\r\n"
          << "gaps:" << s.gaps << "\r\n"
          << "refused_stale_primary:" << (r ? r->refused_stale_primary() : 0) << "\r\n";
        return o.str();
    }
    auto prim = std::atomic_load(&primary_);
    const auto ls = log_.stats();
    o << "role:master\r\n"
      << "repl_id:" << log_.replid() << "\r\n"
      << "repl_offset:" << log_.last_offset() << "\r\n"
      << "repl_enabled:" << (prim ? "yes" : "no") << "\r\n"
      << "sync_replicas:" << opts_.sync_replicas << "\r\n";
    if (prim) {
        o << "in_sync_replicas:" << prim->in_sync_replicas(opts_.max_ack_age_ms) << "\r\n";
        if (opts_.sync_replicas > 0) o << "acked_by_quorum:" << repl_acked() << "\r\n";
        o << "repl_port:" << prim->port() << "\r\n"
          << "backlog_first_offset:" << log_.first_offset() << "\r\n"
          << "backlog_records:" << ls.buffered_records << "\r\n"
          << "backlog_bytes:" << ls.buffered_bytes << "\r\n"
          << "backlog_trimmed_records:" << ls.trimmed << "\r\n";
        const auto reps = prim->replicas();
        o << "connected_replicas:" << reps.size() << "\r\n";
        const uint64_t last = log_.last_offset();
        for (size_t i = 0; i < reps.size(); ++i) {
            o << "replica" << i << ":addr=" << reps[i].peer << ",state=" << reps[i].state << ",sent=" << reps[i].sent
              << ",acked=" << reps[i].acked << ",lag_records=" << (last > reps[i].acked ? last - reps[i].acked : 0)
              << ",ack_age_ms=" << reps[i].last_ack_ms_ago << "\r\n";
        }
        const auto st = prim->stats();
        o << "full_syncs:" << st.full_syncs << "\r\n"
          << "partial_syncs:" << st.partial_syncs << "\r\n"
          << "replicas_cut_too_old:" << st.cut_too_old << "\r\n"
          << "replicas_cut_send_failed:" << st.cut_send_failed << "\r\n";
    }
    return o.str();
}

// --- NodeHooks -----------------------------------------------------------------------------------

int NodeHooks::add_durable_listener(std::function<void()> fn) {
    Ids ids;
    if (p_) ids.p = p_->add_durable_listener(fn);
    if (r_) ids.r = r_->add_ack_listener(std::move(fn));
    std::lock_guard<std::mutex> g(mu_);
    const int id = next_id_++;
    ids_[id] = ids;
    return id;
}

void NodeHooks::remove_durable_listener(int id) {
    Ids ids;
    {
        std::lock_guard<std::mutex> g(mu_);
        auto it = ids_.find(id);
        if (it == ids_.end()) return;
        ids = it->second;
        ids_.erase(it);
    }
    if (p_) p_->remove_durable_listener(ids.p);
    if (r_) r_->remove_ack_listener(ids.r);
}

bool NodeHooks::save(std::string* error) {
    if (!p_) {
        if (error) *error = "persistence is not enabled (start with --dir)";
        return false;
    }
    return p_->save(error);
}

std::string NodeHooks::info() {
    std::string s = p_ ? p_->info() : std::string("# Persistence\r\nenabled:no\r\n");
    if (r_) s += r_->info();
    return s;
}

std::string NodeHooks::control(const std::vector<std::string>& args, bool* handled) {
    if (!r_) { *handled = false; return ""; }
    return r_->control(args, handled);
}
