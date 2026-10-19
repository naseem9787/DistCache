#include "coordinator.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <sstream>

#include <fstream>
#include <map>

#include <unistd.h>

#include "resp_client.hpp"

namespace {

int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

}  // namespace

struct Coordinator::Impl {
    struct Member {
        MemberSpec spec;
        RespConn conn;
        // result of the latest ROLE probe
        bool reachable = false;
        std::string role;  // "master" | "slave"
        int64_t epoch = 0;
        std::string replid;
        uint64_t offset = 0;
        bool fenced = false;
        bool synced = false;
        int repl_port = -1;
        bool ack_durable = false;   // ROLE field 11: its acknowledged writes are fsynced
    };
    enum class State { Starting, Healthy, FailingOver, Unavailable };
    struct Shard {
        std::string name;
        std::vector<std::unique_ptr<Member>> members;
        int primary = -1;
        bool epoch_known = false;
        int64_t epoch = 0;
        std::string replid;
        int primary_repl_port = -1;
        int failures = 0;
        int64_t safe_expiry = 0;  // steady ms: the old primary's lease cannot outlive this
        int64_t failing_since = 0;
        State state = State::Starting;
        std::string reason;
        uint64_t failovers = 0;
        std::string last_event_reason;
        int64_t persisted_epoch = -1;  // highest epoch ever recorded for this shard (state_file)
        int avail_reported = -1;       // -1 never, 0 false, 1 true
    };

    Impl(std::vector<ShardSpec> specs, CoordinatorOptions o, PrimaryChanged cb, AvailabilityChanged av)
        : opts(o), changed(std::move(cb)), availability(std::move(av)) {
        t0 = now_ms();
        for (auto& sp : specs) {
            Shard s;
            s.name = sp.shard;
            for (auto& m : sp.members) {
                auto mem = std::make_unique<Member>();
                mem->spec = m;
                s.members.push_back(std::move(mem));
            }
            // We remember nothing about leases granted before we started: assume one was just granted.
            s.safe_expiry = t0 + lease_bound();
            shards.push_back(std::move(s));
        }
        load_state();
    }

    // "shard epoch" per line
    void load_state() {
        if (opts.state_file.empty()) return;
        std::ifstream in(opts.state_file);
        std::string name;
        long long e;
        while (in >> name >> e)
            for (auto& s : shards)
                if (s.name == name) s.persisted_epoch = std::max<int64_t>(s.persisted_epoch, e);
    }
    void save_state() {
        if (opts.state_file.empty()) return;
        const std::string tmp = opts.state_file + ".tmp";
        {
            std::ofstream out(tmp, std::ios::trunc);
            for (auto& s : shards) out << s.name << " " << std::max<int64_t>(s.persisted_epoch, 0) << '\n';
            out.flush();
        }
        if (std::rename(tmp.c_str(), opts.state_file.c_str()) != 0) std::fprintf(stderr, "[coordinator] cannot write %s\n", opts.state_file.c_str());
    }
    void remember_epoch(Shard& s) {
        if (s.epoch > s.persisted_epoch) {
            s.persisted_epoch = s.epoch;
            save_state();
        }
    }

    // The longest a lease granted "now" can stay valid, as seen on the coordinator's clock.
    int64_t lease_bound() const { return static_cast<int64_t>(opts.lease_ms * (1.0 + opts.eps)) + opts.guard_ms; }

    void event(const Shard& s, const std::string& msg) {
        char head[64];
        std::snprintf(head, sizeof head, "+%lldms ", static_cast<long long>(now_ms() - t0));
        const std::string line = std::string(head) + "[" + s.name + "] " + msg;
        {
            std::lock_guard<std::mutex> g(mu);
            events.push_back(line);
            if (events.size() > 300) events.pop_front();
        }
        if (opts.log_events) std::fprintf(stderr, "[coordinator] %s\n", line.c_str());
    }

    // -- talking to a node ------------------------------------------------------------------------
    bool call(Member& m, const std::vector<std::string>& args, Reply& r) {
        if (!m.conn.is_open() && !m.conn.connect(m.spec.host, m.spec.port, opts.probe_timeout_ms)) return false;
        if (!m.conn.call(args, r)) {
            m.conn.close();  // a timed-out exchange leaves the stream in an unknown state
            return false;
        }
        return true;
    }

    void probe(Member& m) {
        Reply r;
        m.reachable = false;
        if (!call(m, {"ROLE"}, r) || r.type != '*' || r.array.size() < 10) return;
        m.reachable = true;
        m.role = r.array[0].str;
        m.epoch = std::strtoll(r.array[1].str.c_str(), nullptr, 10);
        m.replid = r.array[2].str;
        m.offset = std::strtoull(r.array[3].str.c_str(), nullptr, 10);
        m.fenced = r.array[4].str == "1";
        m.synced = m.role == "master" || r.array[6].str == "synced";
        m.repl_port = static_cast<int>(std::strtol(r.array[7].str.c_str(), nullptr, 10));
        m.ack_durable = r.array.size() > 10 && r.array[10].str == "1";
    }

    // -- one round for one shard ----------------------------------------------------------------------
    void step(Shard& s) {
        for (auto& m : s.members) probe(*m);
        if (!s.epoch_known || s.primary < 0) discover(s);
        if (!s.epoch_known) return;  // could not establish the shard's state: change nothing (fail closed)

        if (s.primary >= 0) {
            Member& p = *s.members[static_cast<size_t>(s.primary)];
            if (!p.reachable) {
                ++s.failures;
                s.reason = "primary " + p.spec.name + " unreachable (" + std::to_string(s.failures) + "/" + std::to_string(opts.fail_after) + ")";
            } else if (p.role != "master" || p.epoch != s.epoch || p.fenced) {
                // It answers, but it is no longer the primary of this epoch (restarted empty, demoted,
                // fenced). It cannot be holding a valid lease for this epoch, so no waiting is needed.
                s.failures = opts.fail_after;
                s.reason = "primary " + p.spec.name + " no longer holds the primary role of epoch " + std::to_string(s.epoch) +
                           " (reports " + p.role + " epoch " + std::to_string(p.epoch) + (p.fenced ? ", fenced" : "") + ")";
            } else if (!s.replid.empty() && p.replid != s.replid) {
                // Same epoch, but a different lineage: the primary PROCESS was restarted (a new process starts a new
                // replication log). Its in-memory WAL tail is gone, yet its replicas may hold those writes (sync N
                // acknowledged them). Re-adopting it would make the replicas resynchronise DOWN to it and lose
                // acknowledged writes. Only a node whose acknowledged writes were already fsynced may be re-adopted.
                if (p.ack_durable) {
                    event(s, "primary " + p.spec.name + " restarted; re-adopted because it acknowledges only fsynced writes (--fsync always)");
                    s.replid = p.replid;
                    s.primary_repl_port = p.repl_port;
                    grant_lease(s, p);
                } else {
                    s.failures = opts.fail_after;
                    s.reason = "primary " + p.spec.name + " restarted (new lineage) and does not fsync before acknowledging: "
                               "its replicas, not its WAL, hold the acknowledged writes";
                }
            } else {
                s.primary_repl_port = p.repl_port;
                grant_lease(s, p);
            }
        }

        if (s.primary < 0 || s.failures >= opts.fail_after) {
            failover(s);
        } else if (s.state == State::Healthy) {
            reconcile(s);
        }
    }

    // Find out who the primary is (first round after start, or after losing track of it).
    void discover(Shard& s) {
        int64_t max_epoch = -1;
        for (auto& m : s.members)
            if (m->reachable) max_epoch = std::max(max_epoch, m->epoch);
        if (max_epoch < 0) {
            s.state = State::Unavailable;
            s.reason = "no member of the shard is reachable";
            return;
        }
        if (max_epoch < s.persisted_epoch) {
            // Every reachable member is OLDER than an epoch this shard has already used: the members
            // of the newest epoch are unreachable. Adopting an older one would resurrect stale data.
            s.state = State::Unavailable;
            s.reason = "reachable members are at epoch " + std::to_string(max_epoch) + " but the shard already reached epoch " +
                       std::to_string(s.persisted_epoch) + "; refusing to adopt stale members";
            if (s.last_event_reason != s.reason) { event(s, s.reason); s.last_event_reason = s.reason; }
            return;
        }
        std::vector<int> masters;
        for (size_t i = 0; i < s.members.size(); ++i) {
            const Member& m = *s.members[i];
            if (m.reachable && m.role == "master" && m.epoch == max_epoch) masters.push_back(static_cast<int>(i));
        }
        int chosen = -1;
        if (masters.size() == 1) {
            chosen = masters[0];
        } else if (masters.size() > 1) {
            // Several nodes claim the primary role at the same epoch. Only the designated primary may
            // keep it; anything else is ambiguous and we fail closed.
            if (std::find(masters.begin(), masters.end(), 0) != masters.end()) {
                chosen = 0;
                event(s, "several masters at epoch " + std::to_string(max_epoch) + "; keeping the designated primary " + s.members[0]->spec.name);
            } else {
                s.state = State::Unavailable;
                s.reason = "several masters at epoch " + std::to_string(max_epoch) + " and none is the designated primary";
                return;
            }
        }
        const bool changed_primary = chosen != s.primary;
        s.epoch = max_epoch;
        s.epoch_known = true;
        remember_epoch(s);
        s.primary = chosen;
        if (chosen >= 0) {
            const Member& p = *s.members[static_cast<size_t>(chosen)];
            s.replid = p.replid;
            s.primary_repl_port = p.repl_port;
            if (changed_primary) {
                event(s, "primary is " + p.spec.name + " (epoch " + std::to_string(s.epoch) + ")");
                if (changed) changed(s.name, p.spec.host, p.spec.port);  // align the router's route with reality
            }
        } else {
            s.replid.clear();
            event(s, "no master at epoch " + std::to_string(max_epoch) + ": a failover is needed");
        }
    }

    void grant_lease(Shard& s, Member& p) {
        const int64_t t_send = now_ms();
        Reply r;
        const bool ok = call(p, {"LEASE", std::to_string(s.epoch), std::to_string(opts.lease_ms)}, r);
        const int64_t t_ack = now_ms();
        if (ok && r.type == '+') {
            // The node received the lease no later than t_ack, so (clock RATE error eps) it cannot
            // believe in it beyond t_ack + L*(1+eps); G covers its scheduling delay.
            s.safe_expiry = std::max(s.safe_expiry, t_ack + lease_bound());
            s.failures = 0;
            if (s.state != State::Healthy) {
                if (s.failing_since != 0) event(s, "recovered after " + std::to_string(t_ack - s.failing_since) + " ms");
                s.failing_since = 0;
            }
            s.state = State::Healthy;
            s.reason.clear();
        } else if (ok) {
            // It answered with an error (fenced, wrong epoch): alive but not a valid primary.
            s.failures = opts.fail_after;
            s.reason = "primary " + p.spec.name + " refused the lease: " + r.str;
        } else {
            // No answer. The message may still arrive within D, so the lease may have been granted.
            s.safe_expiry = std::max(s.safe_expiry, t_send + opts.probe_timeout_ms + lease_bound());
            ++s.failures;
            s.reason = "no answer to the lease request (" + std::to_string(s.failures) + "/" + std::to_string(opts.fail_after) + ")";
        }
    }

    void failover(Shard& s) {
        const int64_t now = now_ms();
        if (s.failing_since == 0) {
            s.failing_since = now;
            event(s, "PRIMARY FAILED: " + (s.reason.empty() ? std::string("no primary") : s.reason));
        }
        s.state = State::FailingOver;
        if (now < s.safe_expiry) {
            s.reason = "waiting " + std::to_string(s.safe_expiry - now) + " ms for the old primary's lease to expire before promoting";
            return;
        }
        // eligible replicas
        std::string lineage = s.replid;
        if (lineage.empty()) {  // no primary known: adopt the lineage of the most advanced reachable replica
            uint64_t best = 0;
            bool any = false;
            for (auto& m : s.members)
                if (m->reachable && m->role == "slave" && m->synced && (!any || m->offset > best)) { best = m->offset; lineage = m->replid; any = true; }
        }
        std::vector<int> cands;
        for (size_t i = 0; i < s.members.size(); ++i) {
            if (static_cast<int>(i) == s.primary) continue;
            const Member& m = *s.members[i];
            if (m.reachable && m.role == "slave" && m.synced && m.epoch == s.epoch && m.replid == lineage) cands.push_back(static_cast<int>(i));
        }
        const int total = static_cast<int>(s.members.size()) - 1;  // R: replicas configured (every member but the failed primary)
        const int n = opts.sync_replicas;
        const int need = n > 0 ? total - n + 1 : 1;
        if (static_cast<int>(cands.size()) < need) {
            s.state = State::Unavailable;
            s.reason = "NOT PROMOTING: " + std::to_string(cands.size()) + " eligible replica(s), " + std::to_string(need) +
                       " needed (R=" + std::to_string(total) + ", sync N=" + std::to_string(n) + ")";
            if (s.last_event_reason != s.reason) { event(s, s.reason); s.last_event_reason = s.reason; }
            return;
        }
        int best = cands[0];
        for (int c : cands) {
            const Member& a = *s.members[static_cast<size_t>(c)];
            const Member& b = *s.members[static_cast<size_t>(best)];
            if (a.offset > b.offset || (a.offset == b.offset && a.spec.name < b.spec.name)) best = c;
        }
        Member& m = *s.members[static_cast<size_t>(best)];
        Reply r;
        if (!call(m, {"PROMOTE", std::to_string(s.epoch + 1)}, r) || r.type != '+') {
            s.reason = "PROMOTE of " + m.spec.name + " failed: " + (r.type == '-' ? r.str : std::string("no answer"));
            if (s.last_event_reason != s.reason) { event(s, s.reason); s.last_event_reason = s.reason; }
            return;  // try again next round; nothing was changed
        }
        // "OK <offset> <replid> <repl_port>"
        std::istringstream in(r.str);
        std::string ok_word, off, newid;
        int port = -1;
        in >> ok_word >> off >> newid >> port;
        s.epoch += 1;
        remember_epoch(s);  // durable BEFORE anything else depends on the new epoch
        s.primary = best;
        s.replid = newid;
        s.primary_repl_port = port;
        s.failures = 0;
        s.safe_expiry = 0;
        ++s.failovers;
        s.last_event_reason.clear();
        event(s, "PROMOTED " + m.spec.name + " to epoch " + std::to_string(s.epoch) + " at offset " + off + " (" +
                     std::to_string(cands.size()) + " eligible)");
        if (changed) changed(s.name, m.spec.host, m.spec.port);
        grant_lease(s, m);  // writable again as soon as possible
        if (s.failing_since != 0 && s.state == State::Healthy)
            event(s, "writes possible again " + std::to_string(now_ms() - s.failing_since) + " ms after the failure was detected");
        reconcile(s);
    }

    // Everyone who is not the primary must replicate from it, at the current epoch.
    void reconcile(Shard& s) {
        if (s.primary < 0 || s.primary_repl_port <= 0) return;
        const Member& p = *s.members[static_cast<size_t>(s.primary)];
        for (size_t i = 0; i < s.members.size(); ++i) {
            if (static_cast<int>(i) == s.primary) continue;
            Member& m = *s.members[i];
            if (!m.reachable) continue;
            if (m.role == "master" || m.epoch < s.epoch) {
                Reply r;
                const bool ok = call(m, {"REPLICAOF", p.spec.host, std::to_string(s.primary_repl_port), std::to_string(s.epoch)}, r) && r.type == '+';
                event(s, std::string(m.role == "master" ? "demoting " : "re-pointing ") + m.spec.name + " to follow " + p.spec.name +
                             (ok ? "" : " (refused: " + r.str + ")"));
            }
        }
    }

    // -- thread and snapshots ---------------------------------------------------------------------------
    void run() {
        while (running) {
            const int64_t round_start = now_ms();
            for (auto& s : shards) {
                if (!running) break;
                step(s);
                report_availability(s);
                publish_snapshot(s);
            }
            while (running && now_ms() - round_start < opts.probe_ms) std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

    void report_availability(Shard& s) {
        const bool avail = s.epoch_known && s.primary >= 0 && s.failures < opts.fail_after && s.state == State::Healthy;
        if (s.avail_reported == (avail ? 1 : 0)) return;
        s.avail_reported = avail ? 1 : 0;
        if (availability) availability(s.name, avail);
    }

    void publish_snapshot(const Shard& s) {
        ShardInfo si;
        si.shard = s.name;
        switch (s.state) {
            case State::Starting: si.state = "starting"; break;
            case State::Healthy: si.state = "healthy"; break;
            case State::FailingOver: si.state = "failing-over"; break;
            case State::Unavailable: si.state = "unavailable"; break;
        }
        si.reason = s.reason;
        si.epoch = s.epoch;
        si.failovers = s.failovers;
        si.failures = s.failures;
        si.primary = s.primary >= 0 ? s.members[static_cast<size_t>(s.primary)]->spec.name : "-";
        si.safe_expiry_in_ms = std::max<int64_t>(0, s.safe_expiry - now_ms());
        for (const auto& m : s.members) {
            MemberInfo mi;
            mi.name = m->spec.name;
            mi.endpoint = m->spec.host + ":" + std::to_string(m->spec.port);
            mi.role = m->reachable ? m->role : "down";
            mi.reachable = m->reachable;
            mi.synced = m->synced;
            mi.epoch = m->epoch;
            mi.offset = m->offset;
            si.members.push_back(std::move(mi));
        }
        std::lock_guard<std::mutex> g(mu);
        for (auto& e : snapshot) {
            if (e.shard == si.shard) { e = std::move(si); return; }
        }
        snapshot.push_back(std::move(si));
    }

    CoordinatorOptions opts;
    PrimaryChanged changed;
    AvailabilityChanged availability;
    int64_t t0 = 0;
    std::vector<Shard> shards;
    std::thread th;
    std::atomic<bool> running{false};
    mutable std::mutex mu;
    std::vector<ShardInfo> snapshot;
    std::deque<std::string> events;
};

Coordinator::Coordinator(std::vector<ShardSpec> shards, CoordinatorOptions opts, PrimaryChanged cb, AvailabilityChanged av)
    : impl_(std::make_unique<Impl>(std::move(shards), opts, std::move(cb), std::move(av))) {}

Coordinator::~Coordinator() { stop(); }

void Coordinator::start() {
    if (impl_->running.exchange(true)) return;
    impl_->th = std::thread([this] { impl_->run(); });
}

void Coordinator::stop() {
    if (!impl_->running.exchange(false)) return;
    if (impl_->th.joinable()) impl_->th.join();
}

std::vector<Coordinator::ShardInfo> Coordinator::info() const {
    std::lock_guard<std::mutex> g(impl_->mu);
    return impl_->snapshot;
}

std::vector<std::string> Coordinator::events() const {
    std::lock_guard<std::mutex> g(impl_->mu);
    return std::vector<std::string>(impl_->events.begin(), impl_->events.end());
}

std::string Coordinator::status_text() const {
    std::ostringstream o;
    for (const auto& s : info()) {
        o << "shard:" << s.shard << " state=" << s.state << " primary=" << s.primary << " epoch=" << s.epoch
          << " failovers=" << s.failovers << " failures=" << s.failures << " lease_wait_ms=" << s.safe_expiry_in_ms;
        if (!s.reason.empty()) o << " reason=\"" << s.reason << "\"";
        o << "\r\n";
        for (const auto& m : s.members)
            o << "  member:" << m.name << " " << m.endpoint << " role=" << m.role << " epoch=" << m.epoch << " offset=" << m.offset
              << " synced=" << (m.synced ? "yes" : "no") << "\r\n";
    }
    return o.str();
}
