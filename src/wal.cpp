#include "wal.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "diag.hpp"

namespace {

int64_t mono_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

void put_u32(std::string& s, uint32_t v) {
    for (int i = 0; i < 4; ++i) s.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
uint32_t get_u32(const unsigned char* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

constexpr int64_t kFlushIntervalMs = 10;   // how often non-"always" policies push data to the OS
constexpr int64_t kSyncIntervalMs = 1000;  // everysec

thread_local uint64_t tls_pending_seq = 0;

[[noreturn]] void fatal_io(const char* what) {
    std::fprintf(stderr, "FATAL: write-ahead log %s failed: %s. Aborting rather than "
                         "acknowledging writes that may not be durable.\n", what, std::strerror(errno));
    std::fflush(stderr);
    std::abort();
}

}  // namespace

bool parse_fsync_policy(const std::string& s, FsyncPolicy& out) {
    if (s == "always") out = FsyncPolicy::Always;
    else if (s == "everysec") out = FsyncPolicy::EverySec;
    else if (s == "no") out = FsyncPolicy::No;
    else return false;
    return true;
}

const char* to_string(FsyncPolicy p) {
    switch (p) {
        case FsyncPolicy::Always: return "always";
        case FsyncPolicy::EverySec: return "everysec";
        default: return "no";
    }
}

// Standard reflected CRC-32 (IEEE 802.3, polynomial 0xEDB88320), the same value the original
// byte-at-a-time version produced, so existing log files stay valid. "Slicing-by-8": eight
// derived tables let the loop consume 8 bytes per iteration instead of 1, which is the same
// arithmetic with far fewer dependent table lookups. Portable (no CPU-specific instructions);
// assumes a little-endian host, which the static_assert enforces.
static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "crc32 slicing-by-8 assumes little endian");

uint32_t crc32(const void* data, size_t n) {
    static const auto tables = [] {
        std::array<std::array<uint32_t, 256>, 8> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            t[0][i] = c;
        }
        for (uint32_t i = 0; i < 256; ++i)
            for (int k = 1; k < 8; ++k) t[static_cast<size_t>(k)][i] = (t[static_cast<size_t>(k) - 1][i] >> 8) ^ t[0][t[static_cast<size_t>(k) - 1][i] & 0xFF];
        return t;
    }();
    const auto& T = tables;
    uint32_t crc = 0xFFFFFFFFu;
    const unsigned char* p = static_cast<const unsigned char*>(data);
    while (n >= 8) {
        uint32_t lo, hi;
        std::memcpy(&lo, p, 4);      // memcpy: alignment-safe, compiles to a plain load
        std::memcpy(&hi, p + 4, 4);
        lo ^= crc;
        crc = T[7][lo & 0xFF] ^ T[6][(lo >> 8) & 0xFF] ^ T[5][(lo >> 16) & 0xFF] ^ T[4][lo >> 24] ^
              T[3][hi & 0xFF] ^ T[2][(hi >> 8) & 0xFF] ^ T[1][(hi >> 16) & 0xFF] ^ T[0][hi >> 24];
        p += 8;
        n -= 8;
    }
    while (n--) crc = T[0][(crc ^ *p++) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

std::string frame_record(std::string_view payload) {
    std::string out;
    out.reserve(8 + payload.size());
    put_u32(out, static_cast<uint32_t>(payload.size()));
    put_u32(out, crc32(payload.data(), payload.size()));
    out.append(payload);
    return out;
}

void RecordBuilder::begin(size_t argc) {
    buf_.assign(8, '\0');  // room for [len][crc], filled by finish()
    buf_ += '*';
    buf_ += std::to_string(argc);
    buf_ += "\r\n";
}

void RecordBuilder::arg(std::string_view a) {
    buf_ += '$';
    buf_ += std::to_string(a.size());
    buf_ += "\r\n";
    buf_.append(a);
    buf_ += "\r\n";
}

std::string_view RecordBuilder::finish() {
    const uint32_t len = static_cast<uint32_t>(buf_.size() - 8);
    const uint32_t crc = crc32(buf_.data() + 8, len);
    for (int i = 0; i < 4; ++i) {
        buf_[static_cast<size_t>(i)] = static_cast<char>((len >> (8 * i)) & 0xFF);
        buf_[static_cast<size_t>(4 + i)] = static_cast<char>((crc >> (8 * i)) & 0xFF);
    }
    return buf_;
}

// --- RecordReader ----------------------------------------------------------

RecordReader::RecordReader(const std::string& path, const char* magic) {
    f_ = std::fopen(path.c_str(), "rb");
    if (!f_) return;
    char m[kMagicLen];
    if (std::fread(m, 1, kMagicLen, f_) != kMagicLen || std::memcmp(m, magic, kMagicLen) != 0) bad_magic_ = true;
    good_offset_ = kMagicLen;
}

RecordReader::~RecordReader() { if (f_) std::fclose(f_); }

RecordReader::Status RecordReader::next(std::string& payload) {
    if (!f_ || bad_magic_) return Status::Corrupt;
    unsigned char hdr[8];
    size_t got = std::fread(hdr, 1, sizeof hdr, f_);
    if (got == 0 && std::feof(f_)) return Status::End;  // ended exactly on a record boundary
    if (got != sizeof hdr) return Status::Corrupt;      // torn header
    uint32_t len = get_u32(hdr), crc = get_u32(hdr + 4);
    if (len > kMaxRecordBytes) return Status::Corrupt;  // garbage length: don't allocate it
    payload.resize(len);
    if (len > 0 && std::fread(payload.data(), 1, len, f_) != len) return Status::Corrupt;  // torn payload
    if (crc32(payload.data(), len) != crc) return Status::Corrupt;                           // damaged
    good_offset_ += 8 + len;
    return Status::Record;
}

// --- files -------------------------------------------------------------------

std::string wal_path(const std::string& dir, uint64_t id) {
    char name[64];
    std::snprintf(name, sizeof name, "/wal-%010llu.log", static_cast<unsigned long long>(id));
    return dir + name;
}
std::string snapshot_path(const std::string& dir, uint64_t id) {
    char name[64];
    std::snprintf(name, sizeof name, "/snapshot-%010llu.dcs", static_cast<unsigned long long>(id));
    return dir + name;
}

std::vector<uint64_t> list_ids(const std::string& dir, const char* prefix, const char* suffix) {
    std::vector<uint64_t> ids;
    DIR* d = opendir(dir.c_str());
    if (!d) return ids;
    const size_t pl = std::strlen(prefix), sl = std::strlen(suffix);
    while (dirent* e = readdir(d)) {
        std::string n = e->d_name;
        if (n.size() <= pl + sl || n.compare(0, pl, prefix) != 0 || n.compare(n.size() - sl, sl, suffix) != 0) continue;
        std::string digits = n.substr(pl, n.size() - pl - sl);
        if (digits.find_first_not_of("0123456789") != std::string::npos) continue;
        ids.push_back(std::strtoull(digits.c_str(), nullptr, 10));
    }
    closedir(d);
    std::sort(ids.begin(), ids.end());
    return ids;
}

bool fsync_dir(const std::string& dir) {
    int fd = open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return false;
    bool ok = fsync(fd) == 0;  // makes a create/rename/unlink in the directory itself durable
    close(fd);
    return ok;
}

// --- Wal ---------------------------------------------------------------------

Wal::Wal(std::string dir, FsyncPolicy policy) : dir_(std::move(dir)), policy_(policy) {
    buf_.reserve(1u << 20);     // 1 MiB each: enough that steady-state appends never reallocate
    io_buf_.reserve(1u << 20);
}
Wal::~Wal() { stop(); }

bool Wal::open_segment(uint64_t id, std::string* err) {
    const std::string path = wal_path(dir_, id);
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) {
        if (err) *err = "cannot create " + path + ": " + std::strerror(errno);
        return false;
    }
    if (::write(fd, kWalMagic, kMagicLen) != static_cast<ssize_t>(kMagicLen) || fdatasync(fd) != 0 || !fsync_dir(dir_)) {
        if (err) *err = "cannot initialise " + path + ": " + std::strerror(errno);
        ::close(fd);
        return false;
    }
    fd_ = fd;
    segment_id_ = id;
    segment_bytes_ = kMagicLen;
    return true;
}

bool Wal::open(uint64_t segment_id, std::string* err) {
    std::lock_guard<std::mutex> io(io_mu_);
    last_sync_ms_ = mono_ms();
    return open_segment(segment_id, err);
}

void Wal::start() {
    if (started_) return;
    started_ = true;
    flusher_ = std::thread([this] { flusher_loop(); });
}

void Wal::stop() {
    if (started_) {
        {
            std::lock_guard<std::mutex> g(mu_);
            stop_ = true;
        }
        cv_work_.notify_all();
        flusher_.join();
        started_ = false;
    }
    std::lock_guard<std::mutex> io(io_mu_);
    if (fd_ >= 0) {
        flush_locked_io(/*force_sync=*/true);  // even policy "no" syncs on a clean shutdown
        ::close(fd_);
        fd_ = -1;
    }
}

uint64_t Wal::append(std::string_view payload) {
    return append_framed(frame_record(payload));  // CRC computed outside the lock
}

uint64_t Wal::append_framed(std::string_view framed) {
    uint64_t seq;
    {
        std::unique_lock<SpinMutex> g(buf_mu_, std::defer_lock);
#ifdef DISTCACHE_DIAGNOSTICS
        const uint64_t t0 = diag::now_ns();
        g.lock();
        const uint64_t t1 = diag::now_ns();
        diag::c().append_wait.add(t1 - t0);  // 0-ish when uncontended, the blocked time otherwise
#else
        g.lock();
#endif
#ifdef DISTCACHE_DIAGNOSTICS
        const size_t cap_before = buf_.capacity();
#endif
        buf_.append(framed);                 // the whole critical section: one memcpy + one increment
        seq = ++last_seq_;
#ifdef DISTCACHE_DIAGNOSTICS
        diag::c().append_locked.add(diag::now_ns() - t1);
        if (buf_.capacity() != cap_before) diag::c().buf_grow_events.fetch_add(1, std::memory_order_relaxed);
        {
            uint64_t peak = diag::c().buf_peak_bytes.load(std::memory_order_relaxed);
            const uint64_t now_size = buf_.size();
            while (now_size > peak && !diag::c().buf_peak_bytes.compare_exchange_weak(peak, now_size, std::memory_order_relaxed)) {}
        }
#endif
    }
    tls_pending_seq = seq;
    if (policy_ == FsyncPolicy::Always) {
        {
            std::lock_guard<std::mutex> g(mu_);  // pairs with the flusher's wait predicate: no lost wake-up
            work_pending_ = true;
        }
        cv_work_.notify_one();  // wake the flusher now
    }
    return seq;
}

void Wal::commit() {
    const uint64_t seq = tls_pending_seq;
    tls_pending_seq = 0;
    if (policy_ != FsyncPolicy::Always || seq == 0) return;
    std::unique_lock<std::mutex> lk(mu_);
    cv_durable_.wait(lk, [&] { return durable_seq_ >= seq; });
}

uint64_t Wal::take_pending_seq() {
    const uint64_t seq = tls_pending_seq;
    tls_pending_seq = 0;
    return policy_ == FsyncPolicy::Always ? seq : 0;  // other policies never make a client wait
}

void Wal::write_all(const char* data, size_t n) {
#ifdef DISTCACHE_DIAGNOSTICS
    if (diag::sink_is_null()) return;  // diagnostic: discard instead of writing, to isolate disk cost
#endif
    while (n > 0) {
        ssize_t w = ::write(fd_, data, n);
        if (w < 0) {
            if (errno == EINTR) continue;
            fatal_io("write");
        }
        data += w;
        n -= static_cast<size_t>(w);
    }
}

// Writes everything buffered so far, fsyncs if the policy (or force_sync) says so, then
// publishes the new durable sequence number. Caller holds io_mu_.
void Wal::flush_locked_io(bool force_sync) {
    std::string& out = io_buf_;  // empty, capacity retained from the previous flush
    {
        std::lock_guard<std::mutex> g(mu_);
        work_pending_ = false;   // before the swap: an append after this re-arms it
    }
    uint64_t upto;
    {
        DC_SCOPED(flush_swap);
        std::lock_guard<SpinMutex> g(buf_mu_);
        out.swap(buf_);          // appenders now fill the other (also pre-sized) buffer
        upto = last_seq_;
    }
    const bool wrote = !out.empty();
    if (wrote) {
        {
            DC_SCOPED(flush_write);
            write_all(out.data(), out.size());
        }
#ifdef DISTCACHE_DIAGNOSTICS
        diag::c().flush_cycles.fetch_add(1, std::memory_order_relaxed);
        diag::c().flush_bytes.fetch_add(out.size(), std::memory_order_relaxed);
#endif
        segment_bytes_ += out.size();
        bytes_.fetch_add(out.size(), std::memory_order_relaxed);
        unsynced_ = true;
        out.clear();             // keeps capacity
    }
    bool sync = false;
    if (unsynced_) {
        if (force_sync || policy_ == FsyncPolicy::Always) sync = true;
        else if (policy_ == FsyncPolicy::EverySec && mono_ms() - last_sync_ms_ >= kSyncIntervalMs) sync = true;
    }
    if (sync) {
        const int64_t delay = sync_delay_ms_.load(std::memory_order_relaxed);
        if (delay > 0) std::this_thread::sleep_for(std::chrono::milliseconds(delay));  // test hook only
        {
            DC_SCOPED(flush_sync);
            if (fdatasync(fd_) != 0) fatal_io("fdatasync");
        }
        fsyncs_.fetch_add(1, std::memory_order_relaxed);
        last_sync_ms_ = mono_ms();
        unsynced_ = false;
    }
    // Everything up to `upto` is on disk if we just synced, or if there was nothing to write and
    // nothing left unsynced from an earlier cycle.
    if (sync || (!unsynced_ && !wrote)) {
        bool advanced = false;
        {
            std::lock_guard<std::mutex> g(mu_);
            if (upto > durable_seq_) {
                durable_seq_ = upto;
                durable_atomic_.store(upto, std::memory_order_release);  // publish BEFORE anyone is told
                advanced = true;
            }
        }
        cv_durable_.notify_all();
        if (advanced && on_durable_) on_durable_();
    }
}

void Wal::flusher_loop() {
    DC_TAG_THREAD("flusher");
    pthread_setname_np(pthread_self(), "dc-flusher");  // visible in /proc/<pid>/task/*/comm for per-thread CPU
    for (;;) {
        {
            std::unique_lock<std::mutex> lk(mu_);
            cv_work_.wait_for(lk, std::chrono::milliseconds(kFlushIntervalMs),
                              [&] { return stop_ || work_pending_; });
            if (stop_) return;
        }
        std::lock_guard<std::mutex> io(io_mu_);
        flush_locked_io(false);
    }
}

bool Wal::rotate(uint64_t* new_id, std::string* err) {
    std::lock_guard<std::mutex> io(io_mu_);
    flush_locked_io(/*force_sync=*/true);  // the old segment must be complete and durable
    ::close(fd_);
    fd_ = -1;
    const uint64_t next = segment_id_ + 1;
    if (!open_segment(next, err)) return false;
    if (new_id) *new_id = next;
    return true;
}

WalStats Wal::stats() const {
    WalStats s;
    s.bytes = bytes_.load(std::memory_order_relaxed);
    s.fsyncs = fsyncs_.load(std::memory_order_relaxed);
    std::lock_guard<std::mutex> io(io_mu_);
    s.segment_id = segment_id_;
    s.segment_bytes = segment_bytes_;
    {
        std::lock_guard<SpinMutex> g(buf_mu_);
        s.records = last_seq_;               // every record gets the next number: the count IS last_seq
        s.last_seq = last_seq_;
    }
    {
        std::lock_guard<std::mutex> g(mu_);  // lock order io_mu_ -> mu_, as everywhere
        s.durable_seq = durable_seq_;
    }
    return s;
}
