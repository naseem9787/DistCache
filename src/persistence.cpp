#include "persistence.hpp"

#include <fcntl.h>
#include <pthread.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>

#include "diag.hpp"
#include "record_apply.hpp"
#include "resp.hpp"

namespace {

int64_t system_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// Decodes one logged record (a RESP command) into its arguments.
bool decode(const std::string& payload, std::vector<std::string>* args) {
    ParseResult r = parse_command(payload);
    if (r.status != ParseStatus::Ok || r.consumed != payload.size() || r.args.empty()) return false;
    *args = std::move(r.args);
    return true;
}

bool parse_i64(const std::string& s, int64_t* out) {
    char* end = nullptr;
    errno = 0;
    long long v = std::strtoll(s.c_str(), &end, 10);
    if (errno != 0 || end == s.c_str() || *end != '\0') return false;
    *out = v;
    return true;
}

bool write_fully(int fd, const char* p, size_t n) {
    while (n > 0) {
        ssize_t w = ::write(fd, p, n);
        if (w < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        p += w;
        n -= static_cast<size_t>(w);
    }
    return true;
}

}  // namespace

Persistence::Persistence(Engine& engine, PersistenceOptions opts) : engine_(engine), opts_(std::move(opts)) {
    if (!opts_.wall_ms) opts_.wall_ms = system_ms;
}

Persistence::~Persistence() { close(); }

// --- recovery ----------------------------------------------------------------

// A snapshot is usable only if every record checks out AND it ends with the SNAPEND marker
// whose count matches. A snapshot cut short by a crash has no marker and is ignored.
bool Persistence::verify_snapshot(const std::string& path, uint64_t* keys) const {
    RecordReader r(path, kSnapshotMagic);
    if (!r.opened()) return false;
    std::string payload;
    uint64_t sets = 0;
    for (;;) {
        auto st = r.next(payload);
        if (st != RecordReader::Status::Record) return false;  // End without SNAPEND, or Corrupt
        std::vector<std::string> a;
        if (!decode(payload, &a)) return false;
        if (a[0] == "SNAPEND") {
            int64_t n;
            if (a.size() != 2 || !parse_i64(a[1], &n) || static_cast<uint64_t>(n) != sets) return false;
            *keys = sets;
            return r.next(payload) == RecordReader::Status::End;  // nothing after the marker
        }
        if (a[0] != "SET") return false;
        ++sets;
    }
}

// Applies one logged operation directly to the engine (observer is detached during recovery).
bool Persistence::apply_record(const std::string& payload, RecoveryStats* st) {
    std::vector<std::string> a;
    if (!decode(payload, &a)) return false;
    // The rules for applying a record live in apply_logical_record, shared with replication.
    switch (apply_logical_record(engine_, a, wall())) {
        case ApplyResult::Applied: return true;
        case ApplyResult::ExpiredSkipped: ++st->expired_skipped; return true;  // deadline passed while down
        case ApplyResult::Invalid: return false;
    }
    return false;
}

bool Persistence::recover(RecoveryStats* st, std::string* error) {
    ::unlink((opts_.dir + "/snapshot.tmp").c_str());  // leftover from a crash mid-snapshot

    // 1. newest *valid* snapshot
    uint64_t from_segment = 0;
    auto snaps = list_ids(opts_.dir, "snapshot-", ".dcs");
    for (auto it = snaps.rbegin(); it != snaps.rend(); ++it) {
        const std::string path = snapshot_path(opts_.dir, *it);
        uint64_t keys = 0;
        if (!verify_snapshot(path, &keys)) {
            st->warnings.push_back("ignoring incomplete or damaged snapshot " + path);
            ++st->damaged_files;
            continue;
        }
        RecordReader r(path, kSnapshotMagic);
        std::string payload;
        // verify_snapshot already proved every record is intact; SNAPEND (the last one) is
        // not an operation, apply_record just declines it.
        while (r.next(payload) == RecordReader::Status::Record) apply_record(payload, st);
        st->snapshot_loaded = true;
        st->snapshot_id = *it;
        st->snapshot_keys = keys;
        from_segment = *it;  // the snapshot contains everything before segment `it`
        break;
    }

    // 2. replay log segments >= from_segment, in order
    for (uint64_t id : list_ids(opts_.dir, "wal-", ".log")) {
        if (id < from_segment) continue;
        const std::string path = wal_path(opts_.dir, id);
        RecordReader r(path, kWalMagic);
        if (!r.opened()) { *error = "cannot open " + path; return false; }
        ++st->segments_replayed;
        std::string payload;
        for (;;) {
            auto s = r.next(payload);
            if (s == RecordReader::Status::Record) {
                if (!apply_record(payload, st)) {
                    st->warnings.push_back("unrecognised record in " + path + " (skipped)");
                    continue;
                }
                ++st->records_replayed;
            } else {
                if (s == RecordReader::Status::Corrupt) {
                    ++st->damaged_files;
                    st->warnings.push_back("stopped replaying " + path + " at byte " + std::to_string(r.good_offset()) +
                                           " (torn or damaged record; the rest of this file is ignored)");
                }
                break;
            }
        }
    }
    return true;
}

// --- open / close -------------------------------------------------------------

bool Persistence::open(RecoveryStats* recovery, std::string* error) {
    RecoveryStats local;
    RecoveryStats* st = recovery ? recovery : &local;
    std::string err;

    if (::mkdir(opts_.dir.c_str(), 0755) != 0 && errno != EEXIST) {
        *error = "cannot create " + opts_.dir + ": " + std::strerror(errno);
        return false;
    }
    lock_fd_ = ::open((opts_.dir + "/LOCK").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (lock_fd_ < 0 || ::flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
        *error = "data directory " + opts_.dir + " is in use by another process";
        if (lock_fd_ >= 0) { ::close(lock_fd_); lock_fd_ = -1; }
        return false;
    }

    engine_.set_observer(nullptr);  // replay must not be logged again
    const auto rec_t0 = std::chrono::steady_clock::now();
    if (!recover(st, &err)) { *error = err; close(); return false; }
    recovery_ms_ = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                             std::chrono::steady_clock::now() - rec_t0).count());

    uint64_t next = 1;
    for (uint64_t id : list_ids(opts_.dir, "wal-", ".log")) next = std::max(next, id + 1);
    for (uint64_t id : list_ids(opts_.dir, "snapshot-", ".dcs")) next = std::max(next, id + 1);

    wal_ = std::make_unique<Wal>(opts_.dir, opts_.fsync);
    if (!wal_->open(next, &err)) { *error = err; close(); return false; }
    wal_->set_durable_callback([this] { notify_listeners(); });  // before start(): the flusher reads it
    wal_->start();
    engine_.set_observer(this);
    open_ = true;
    snap_thread_ = std::thread([this] { snapshot_loop(); });
    return true;
}

void Persistence::close() {
    {
        std::lock_guard<std::mutex> g(stop_mu_);
        stopping_ = true;
    }
    stop_cv_.notify_all();
    if (snap_thread_.joinable()) snap_thread_.join();
    if (open_) {
        engine_.set_observer(nullptr);
        open_ = false;
    }
    if (wal_) wal_->stop();
    if (lock_fd_ >= 0) { ::close(lock_fd_); lock_fd_ = -1; }
}

// --- durability notifications -----------------------------------------------------

int Persistence::add_durable_listener(std::function<void()> fn) {
    std::lock_guard<std::mutex> g(listeners_mu_);
    const int id = next_listener_id_++;
    listeners_.emplace_back(id, std::move(fn));
    return id;
}

void Persistence::remove_durable_listener(int id) {
    std::lock_guard<std::mutex> g(listeners_mu_);  // also waits out a notify_listeners() in progress
    for (auto it = listeners_.begin(); it != listeners_.end(); ++it)
        if (it->first == id) { listeners_.erase(it); return; }
}

void Persistence::notify_listeners() {  // flusher thread
    std::lock_guard<std::mutex> g(listeners_mu_);
    for (auto& l : listeners_) l.second();  // each just writes an eventfd
}

// --- logging (observer callbacks) ----------------------------------------------

// These run INSIDE the engine's lock for every write, so each one encodes its record straight
// into a per-thread buffer that is reused (no vector, no temporary strings, no per-record
// allocation once warm) and hands the finished frame to the log with a single memcpy.
namespace {
thread_local RecordBuilder tl_rec;
}  // namespace

void Persistence::on_set(const std::string& key, const std::string& value, std::optional<int64_t> ttl_ms) {
    std::string_view framed;
    {
        DC_SCOPED(encode);
        tl_rec.begin(ttl_ms ? 5 : 3);
        tl_rec.arg("SET"); tl_rec.arg(key); tl_rec.arg(value);
        if (ttl_ms) { tl_rec.arg("PXAT"); tl_rec.arg(std::to_string(wall() + *ttl_ms)); }
        framed = tl_rec.finish();
    }
    wal_->append_framed(framed);
}
void Persistence::on_del(const std::string& key) {
    tl_rec.begin(2);
    tl_rec.arg("DEL"); tl_rec.arg(key);
    wal_->append_framed(tl_rec.finish());
}
void Persistence::on_expire(const std::string& key, int64_t ttl_ms) {
    tl_rec.begin(3);
    tl_rec.arg("PEXPIREAT"); tl_rec.arg(key); tl_rec.arg(std::to_string(wall() + ttl_ms));
    wal_->append_framed(tl_rec.finish());
}
void Persistence::on_persist(const std::string& key) {
    tl_rec.begin(2);
    tl_rec.arg("PERSIST"); tl_rec.arg(key);
    wal_->append_framed(tl_rec.finish());
}

void Persistence::on_flush() {
    tl_rec.begin(1);
    tl_rec.arg("FLUSHALL");
    wal_->append_framed(tl_rec.finish());
}

// --- snapshots ------------------------------------------------------------------

bool Persistence::snapshot(std::string* error) {
    std::lock_guard<std::mutex> one_at_a_time(snap_mu_);
    if (!open_) { if (error) *error = "persistence is not open"; return false; }
    const auto t0 = std::chrono::steady_clock::now();
    std::string err;

    uint64_t R;
    if (!wal_->rotate(&R, &err)) { if (error) *error = err; return false; }  // step 1: log boundary

    const std::string tmp = opts_.dir + "/snapshot.tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) { if (error) *error = "cannot create " + tmp + ": " + std::strerror(errno); return false; }

    bool ok = write_fully(fd, kSnapshotMagic, kMagicLen);
    std::string chunk;
    uint64_t count = 0;
    engine_.for_each([&](const std::string& k, const std::string& v, int64_t ttl) {  // step 2
        if (!ok) return;
        std::vector<std::string> a{"SET", k, v};
        if (ttl >= 0) { a.push_back("PXAT"); a.push_back(std::to_string(wall() + ttl)); }
        chunk += frame_record(resp_command(a));
        ++count;
        if (chunk.size() >= (1u << 20)) { ok = write_fully(fd, chunk.data(), chunk.size()); chunk.clear(); }
    });
    chunk += frame_record(resp_command({"SNAPEND", std::to_string(count)}));
    ok = ok && write_fully(fd, chunk.data(), chunk.size());
    ok = ok && fdatasync(fd) == 0;                                                            // step 3
    ::close(fd);
    const std::string final_path = snapshot_path(opts_.dir, R);
    ok = ok && ::rename(tmp.c_str(), final_path.c_str()) == 0 && fsync_dir(opts_.dir);
    if (!ok) {
        if (error) *error = std::string("snapshot failed: ") + std::strerror(errno);
        ::unlink(tmp.c_str());
        return false;
    }

    for (uint64_t id : list_ids(opts_.dir, "wal-", ".log")) if (id < R) ::unlink(wal_path(opts_.dir, id).c_str());  // step 4
    for (uint64_t id : list_ids(opts_.dir, "snapshot-", ".dcs")) if (id < R) ::unlink(snapshot_path(opts_.dir, id).c_str());
    fsync_dir(opts_.dir);

    std::lock_guard<std::mutex> g(stats_mu_);
    ++snapshots_taken_;
    last_snapshot_id_ = R;
    last_snapshot_keys_ = count;
    last_snapshot_ms_ = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count());
    return true;
}

void Persistence::snapshot_loop() {
    pthread_setname_np(pthread_self(), "dc-snapshot");
    DC_TAG_THREAD("snapshot");
    std::unique_lock<std::mutex> lk(stop_mu_);
    while (!stopping_) {
        stop_cv_.wait_for(lk, std::chrono::milliseconds(500), [&] { return stopping_; });
        if (stopping_) return;
        if (opts_.snapshot_wal_bytes == 0 || wal_->stats().segment_bytes < opts_.snapshot_wal_bytes) continue;
        lk.unlock();
        std::string err;
        if (!snapshot(&err)) std::fprintf(stderr, "automatic snapshot failed: %s\n", err.c_str());
        lk.lock();
    }
}

Persistence::Stats Persistence::stats() const {
    Stats s;
    if (wal_) s.wal = wal_->stats();
    std::lock_guard<std::mutex> g(stats_mu_);
    s.snapshots_taken = snapshots_taken_;
    s.last_snapshot_id = last_snapshot_id_;
    s.last_snapshot_keys = last_snapshot_keys_;
    s.last_snapshot_ms = last_snapshot_ms_;
    return s;
}

std::string Persistence::info() {
    Stats s = stats();
    return "# Persistence\r\nenabled:yes\r\nfsync:" + std::string(to_string(opts_.fsync)) +
           "\r\nrecovery_ms:" + std::to_string(recovery_ms_) +
           "\r\nwal_segment:" + std::to_string(s.wal.segment_id) +
           "\r\nwal_segment_bytes:" + std::to_string(s.wal.segment_bytes) +
           "\r\nwal_records:" + std::to_string(s.wal.records) +
           "\r\nwal_fsyncs:" + std::to_string(s.wal.fsyncs) +
           "\r\nsnapshots_taken:" + std::to_string(s.snapshots_taken) +
           "\r\nlast_snapshot_keys:" + std::to_string(s.last_snapshot_keys) +
           "\r\nlast_snapshot_ms:" + std::to_string(s.last_snapshot_ms) + "\r\n";
}
