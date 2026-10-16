#include "node_replication.hpp"

#include <algorithm>
#include <sstream>

ReplicationService::ReplicationService(Engine& engine, NodeReplicationOptions opts)
    : engine_(engine),
      opts_(std::move(opts)),
      log_(ReplicationLog::Options{opts_.backlog_bytes, {}}),
      gate_(&log_, /*enabled=*/opts_.replicaof_host.empty()) {}

ReplicationService::~ReplicationService() { stop(); }

bool ReplicationService::start(std::string* error) {
    if (role() == Role::Replica) {
        ReplicationReplica::Options o;
        o.host = opts_.replicaof_host;
        o.port = opts_.replicaof_port;
        o.link_timeout_ms = opts_.link_timeout_ms;
        replica_ = std::make_unique<ReplicationReplica>(engine_, log_, o);
        replica_->start();
        return true;
    }
    if (opts_.repl_port >= 0) {
        ReplicationPrimary::Options o;
        o.port = opts_.repl_port;
        o.send_timeout_ms = opts_.send_timeout_ms;
        primary_ = std::make_unique<ReplicationPrimary>(engine_, log_, o);
        if (!primary_->start(error)) { primary_.reset(); return false; }
    }
    return true;
}

void ReplicationService::stop() {
    if (replica_) replica_->stop();
    if (primary_) primary_->stop();
    log_.close();
}

bool ReplicationService::write_allowed(std::string* error) const {
    if (role() == Role::Replica) {
        *error = "READONLY You can't write against a read only replica.";
        return false;
    }
    return true;
}

bool ReplicationService::read_allowed(std::string* error) const {
    if (role() == Role::Replica && replica_ && !replica_->readable()) {
        *error = "LOADING the replica is synchronizing with its primary";
        return false;
    }
    return true;
}

std::string ReplicationService::info() const {
    std::ostringstream o;
    o << "# Replication\r\n";
    if (role() == Role::Replica) {
        ReplicationReplica::Status s{};
        if (replica_) s = replica_->status();
        const char* st = s.state == ReplicationReplica::State::Online ? "online"
                         : s.state == ReplicationReplica::State::Syncing ? "syncing" : "disconnected";
        o << "role:slave\r\n"
          << "master_host:" << opts_.replicaof_host << "\r\n"
          << "master_port:" << opts_.replicaof_port << "\r\n"
          << "master_link_status:" << (s.state == ReplicationReplica::State::Disconnected ? "down" : "up") << "\r\n"
          << "replica_state:" << st << "\r\n"
          << "replica_readable:" << (replica_ && replica_->readable() ? "yes" : "no") << "\r\n"
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
          << "gaps:" << s.gaps << "\r\n";
        return o.str();
    }
    const auto ls = log_.stats();
    o << "role:master\r\n"
      << "repl_id:" << log_.replid() << "\r\n"
      << "repl_offset:" << log_.last_offset() << "\r\n"
      << "repl_enabled:" << (primary_ ? "yes" : "no") << "\r\n";
    if (primary_) {
        o << "repl_port:" << primary_->port() << "\r\n"
          << "backlog_first_offset:" << log_.first_offset() << "\r\n"
          << "backlog_records:" << ls.buffered_records << "\r\n"
          << "backlog_bytes:" << ls.buffered_bytes << "\r\n"
          << "backlog_trimmed_records:" << ls.trimmed << "\r\n";
        const auto reps = primary_->replicas();
        o << "connected_replicas:" << reps.size() << "\r\n";
        const uint64_t last = log_.last_offset();
        for (size_t i = 0; i < reps.size(); ++i) {
            o << "replica" << i << ":addr=" << reps[i].peer << ",state=" << reps[i].state << ",sent=" << reps[i].sent
              << ",acked=" << reps[i].acked << ",lag_records=" << (last > reps[i].acked ? last - reps[i].acked : 0)
              << ",ack_age_ms=" << reps[i].last_ack_ms_ago << "\r\n";
        }
        const auto st = primary_->stats();
        o << "full_syncs:" << st.full_syncs << "\r\n"
          << "partial_syncs:" << st.partial_syncs << "\r\n"
          << "replicas_cut_too_old:" << st.cut_too_old << "\r\n"
          << "replicas_cut_send_failed:" << st.cut_send_failed << "\r\n";
    }
    return o.str();
}

// --- NodeHooks -----------------------------------------------------------------------------------

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
