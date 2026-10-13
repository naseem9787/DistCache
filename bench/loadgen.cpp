// distcache_loadgen: closed-loop, duration-based RESP load generator.
//
// WHY NOT redis-benchmark: it cannot mix GET and SET in one run, it is request-count based
// (short runs give quantized timings, which already misled an earlier measurement here), and
// its latency definition is not documented. This tool is small enough to read in one sitting.
//
// MODEL
//   * C connections spread over T threads. Each thread runs one epoll loop over its connections.
//   * Each connection is closed-loop: it sends a batch of P pipelined commands, waits for all
//     P replies, then immediately sends the next batch. Offered load is therefore limited by
//     the server; there is no fixed arrival rate.
//   * Latency of a request = (time its reply was read) - (time its batch started being written).
//     With P > 1 the requests in a batch share a start time, so later replies in the batch show
//     a larger latency: that is the queueing a pipelining client really experiences.
//   * Closed-loop caveat ("coordinated omission"): when the server stalls, a closed-loop client
//     stops sending, so the stall appears as ONE slow request per connection, not as the many
//     slow requests an open-loop arrival process would have produced. Percentiles from this
//     tool therefore understate tail latency during stalls. Read them as "latency seen by C
//     persistent clients", not as service-level latency under a fixed arrival rate.
//   * A warm-up period is run and discarded, then the measured period starts. Both are
//     wall-clock durations.
//
// OUTPUT: one JSON object on stdout. Latency histogram: log-linear, 32 sub-buckets per power of
// two (relative error < 1.6%).
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

uint64_t now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

struct Options {
    std::string host = "127.0.0.1";
    int port = 6380;
    int clients = 50;
    int threads = 4;
    int pipeline = 1;
    double duration = 10;
    double warmup = 2;
    uint64_t keyspace = 100000;
    int value_size = 64;
    double get_ratio = 0.8;       // fraction of requests that are GET
    int64_t ttl_ms = 0;           // >0: SET carries PX <ttl_ms>
    std::string key_dist = "uniform";
    double zipf_s = 0.99;
    bool preload = false;
    uint64_t seed = 1;
    std::string label;
};

// ---- latency histogram ---------------------------------------------------------

class Histogram {
public:
    Histogram() : b_(kBuckets, 0) {}
    void record(uint64_t ns) {
        b_[index(ns)]++;
        count_++;
        sum_ += static_cast<double>(ns);
        if (ns > max_) max_ = ns;
        if (ns < min_) min_ = ns;
    }
    void merge(const Histogram& o) {
        for (size_t i = 0; i < b_.size(); ++i) b_[i] += o.b_[i];
        count_ += o.count_;
        sum_ += o.sum_;
        max_ = std::max(max_, o.max_);
        min_ = std::min(min_, o.min_);
    }
    uint64_t count() const { return count_; }
    uint64_t min() const { return count_ ? min_ : 0; }
    uint64_t max() const { return max_; }
    double mean() const { return count_ ? sum_ / static_cast<double>(count_) : 0; }
    // value (ns) at quantile q in [0,1]
    uint64_t quantile(double q) const {
        if (!count_) return 0;
        uint64_t target = static_cast<uint64_t>(std::ceil(q * static_cast<double>(count_)));
        if (target < 1) target = 1;
        uint64_t seen = 0;
        for (size_t i = 0; i < b_.size(); ++i) {
            seen += b_[i];
            if (seen >= target) return std::min(value_of(i), max_);
        }
        return max_;
    }

private:
    static constexpr int kSub = 32;
    static constexpr int kSubBits = 5;
    static constexpr size_t kBuckets = (64 - kSubBits) * kSub;
    static size_t index(uint64_t v) {
        if (v < kSub) return static_cast<size_t>(v);
        int msb = 63 - __builtin_clzll(v);
        return static_cast<size_t>((msb - kSubBits + 1) * kSub + ((v >> (msb - kSubBits)) & (kSub - 1)));
    }
    static uint64_t value_of(size_t idx) {  // upper edge of the bucket
        if (idx < kSub) return idx;
        size_t group = idx / kSub, sub = idx % kSub;
        int msb = static_cast<int>(group) + kSubBits - 1;
        uint64_t lo = (uint64_t{1} << msb) | (static_cast<uint64_t>(sub) << (msb - kSubBits));
        return lo + (uint64_t{1} << (msb - kSubBits)) - 1;
    }
    std::vector<uint64_t> b_;
    uint64_t count_ = 0, max_ = 0, min_ = ~uint64_t{0};
    double sum_ = 0;
};

// ---- key distribution -----------------------------------------------------------

struct Rng {
    uint64_t s;
    explicit Rng(uint64_t seed) : s(seed * 0x9E3779B97F4A7C15ULL + 0x1234567ULL) { next(); next(); }
    uint64_t next() {
        s ^= s << 13; s ^= s >> 7; s ^= s << 17;
        return s;
    }
    double uniform01() { return static_cast<double>(next() >> 11) * (1.0 / 9007199254740992.0); }
};

struct KeyDist {
    uint64_t n = 0;
    bool zipf = false;
    std::vector<double> cdf;
    void init(const Options& o) {
        n = o.keyspace;
        zipf = o.key_dist == "zipf";
        if (zipf) {
            cdf.resize(n);
            double sum = 0;
            for (uint64_t i = 0; i < n; ++i) { sum += 1.0 / std::pow(static_cast<double>(i + 1), o.zipf_s); cdf[i] = sum; }
            for (auto& c : cdf) c /= sum;
        }
    }
    uint64_t pick(Rng& r) const {
        if (!zipf) return r.next() % n;
        double u = r.uniform01();
        return static_cast<uint64_t>(std::lower_bound(cdf.begin(), cdf.end(), u) - cdf.begin()) % n;
    }
};

std::string key_name(uint64_t i) {
    char buf[32];
    int n = std::snprintf(buf, sizeof buf, "key:%010llu", static_cast<unsigned long long>(i));
    return std::string(buf, static_cast<size_t>(n));
}

void append_bulk(std::string& out, const std::string& s) {
    out += '$';
    out += std::to_string(s.size());
    out += "\r\n";
    out += s;
    out += "\r\n";
}

// ---- one connection --------------------------------------------------------------

struct Conn {
    int fd = -1;
    std::string out;         // batch being sent
    size_t sent = 0;
    std::string in;          // unparsed reply bytes
    std::vector<uint8_t> kinds;  // 1 = GET, 0 = SET, per request in the batch
    size_t received = 0;     // replies parsed in the current batch
    uint64_t batch_start = 0;
};

struct ThreadResult {
    Histogram lat;
    uint64_t gets = 0, sets = 0, hits = 0, misses = 0, errors = 0, protocol_errors = 0, requests = 0;
    std::vector<uint64_t> per_second;
    uint64_t connect_failures = 0;
};

int connect_to(const Options& o) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(static_cast<uint16_t>(o.port));
    inet_pton(AF_INET, o.host.c_str(), &a.sin_addr);
    if (connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) { close(fd); return -1; }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return fd;
}

// Reads one complete RESP reply ("+..", "-..", ":..", "$..") from a blocking socket.
bool blocking_reply(int fd, std::string& buf, std::string* payload) {
    for (;;) {
        size_t eol = buf.find("\r\n");
        if (eol != std::string::npos) {
            if (buf[0] == '$') {
                long len = std::strtol(buf.c_str() + 1, nullptr, 10);
                if (len < 0) { if (payload) payload->clear(); buf.erase(0, eol + 2); return true; }
                size_t need = eol + 2 + static_cast<size_t>(len) + 2;
                if (buf.size() >= need) {
                    if (payload) payload->assign(buf, eol + 2, static_cast<size_t>(len));
                    buf.erase(0, need);
                    return true;
                }
            } else {
                if (payload) payload->assign(buf, 0, eol);
                buf.erase(0, eol + 2);
                return true;
            }
        }
        char tmp[8192];
        ssize_t n = read(fd, tmp, sizeof tmp);
        if (n <= 0) return false;
        buf.append(tmp, static_cast<size_t>(n));
    }
}

bool write_all(int fd, const std::string& s) {
    size_t off = 0;
    while (off < s.size()) {
        ssize_t n = write(fd, s.data() + off, s.size() - off);
        if (n <= 0) return false;
        off += static_cast<size_t>(n);
    }
    return true;
}

std::map<std::string, std::string> server_info(const Options& o) {
    std::map<std::string, std::string> m;
    int fd = connect_to(o);
    if (fd < 0) return m;
    std::string buf, payload;
    if (write_all(fd, "*1\r\n$4\r\nINFO\r\n") && blocking_reply(fd, buf, &payload)) {
        size_t pos = 0;
        while (pos < payload.size()) {
            size_t eol = payload.find("\r\n", pos);
            if (eol == std::string::npos) eol = payload.size();
            std::string line = payload.substr(pos, eol - pos);
            size_t c = line.find(':');
            if (c != std::string::npos) m[line.substr(0, c)] = line.substr(c + 1);
            pos = eol + 2;
        }
    }
    close(fd);
    return m;
}

bool preload_keys(const Options& o, const std::string& value) {
    int fd = connect_to(o);
    if (fd < 0) return false;
    const uint64_t kBatch = 256;
    std::string buf;
    for (uint64_t base = 0; base < o.keyspace; base += kBatch) {
        uint64_t cnt = std::min(kBatch, o.keyspace - base);
        std::string out;
        for (uint64_t i = 0; i < cnt; ++i) {
            out += "*3\r\n$3\r\nSET\r\n";
            append_bulk(out, key_name(base + i));
            append_bulk(out, value);
        }
        if (!write_all(fd, out)) { close(fd); return false; }
        for (uint64_t i = 0; i < cnt; ++i)
            if (!blocking_reply(fd, buf, nullptr)) { close(fd); return false; }
    }
    close(fd);
    return true;
}

void run_thread(const Options& o, const KeyDist& dist, const std::string& value, int nconns, int tid,
                uint64_t measure_start, uint64_t end, ThreadResult* res) {
    int ep = epoll_create1(0);
    std::vector<Conn> conns(static_cast<size_t>(nconns));
    Rng rng(o.seed * 1000003ULL + static_cast<uint64_t>(tid) + 1);
    const size_t seconds = static_cast<size_t>(std::ceil(o.duration)) + 2;
    res->per_second.assign(seconds, 0);

    auto build_batch = [&](Conn& c) {
        c.out.clear();
        c.sent = 0;
        c.kinds.clear();
        c.received = 0;
        for (int i = 0; i < o.pipeline; ++i) {
            bool is_get = rng.uniform01() < o.get_ratio;
            std::string key = key_name(dist.pick(rng));
            if (is_get) {
                c.out += "*2\r\n$3\r\nGET\r\n";
                append_bulk(c.out, key);
            } else if (o.ttl_ms > 0) {
                c.out += "*5\r\n$3\r\nSET\r\n";
                append_bulk(c.out, key);
                append_bulk(c.out, value);
                append_bulk(c.out, "PX");
                append_bulk(c.out, std::to_string(o.ttl_ms));
            } else {
                c.out += "*3\r\n$3\r\nSET\r\n";
                append_bulk(c.out, key);
                append_bulk(c.out, value);
            }
            c.kinds.push_back(is_get ? 1 : 0);
        }
        c.batch_start = now_ns();
    };
    auto try_send = [&](Conn& c, size_t idx) -> bool {
        while (c.sent < c.out.size()) {
            ssize_t n = write(c.fd, c.out.data() + c.sent, c.out.size() - c.sent);
            if (n > 0) { c.sent += static_cast<size_t>(n); continue; }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                epoll_event ev{};
                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.u64 = idx;
                epoll_ctl(ep, EPOLL_CTL_MOD, c.fd, &ev);
                return true;
            }
            return false;
        }
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.u64 = idx;
        epoll_ctl(ep, EPOLL_CTL_MOD, c.fd, &ev);
        return true;
    };

    for (size_t i = 0; i < conns.size(); ++i) {
        Conn& c = conns[i];
        c.fd = connect_to(o);
        if (c.fd < 0) { res->connect_failures++; continue; }
        fcntl(c.fd, F_SETFL, fcntl(c.fd, F_GETFL, 0) | O_NONBLOCK);
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.u64 = i;
        epoll_ctl(ep, EPOLL_CTL_ADD, c.fd, &ev);
    }
    for (size_t i = 0; i < conns.size(); ++i) {
        if (conns[i].fd < 0) continue;
        build_batch(conns[i]);
        try_send(conns[i], i);
    }

    epoll_event events[64];
    char buf[65536];
    while (true) {
        uint64_t t = now_ns();
        if (t >= end) break;
        int n = epoll_wait(ep, events, 64, 50);
        t = now_ns();
        for (int e = 0; e < n; ++e) {
            size_t idx = static_cast<size_t>(events[e].data.u64);
            Conn& c = conns[idx];
            if (c.fd < 0) continue;
            if (events[e].events & EPOLLOUT) try_send(c, idx);
            if (!(events[e].events & EPOLLIN)) continue;
            for (;;) {
                ssize_t r = read(c.fd, buf, sizeof buf);
                if (r > 0) { c.in.append(buf, static_cast<size_t>(r)); continue; }
                if (r == 0) { close(c.fd); c.fd = -1; break; }
                break;  // EAGAIN
            }
            if (c.fd < 0) continue;
            // parse as many complete replies as are buffered
            size_t pos = 0;
            while (pos < c.in.size() && c.received < c.kinds.size()) {
                char type = c.in[pos];
                size_t eol = c.in.find("\r\n", pos);
                if (eol == std::string::npos) break;
                size_t next;
                bool nil = false, error = false;
                if (type == '$') {
                    long len = std::strtol(c.in.c_str() + pos + 1, nullptr, 10);
                    if (len < 0) { nil = true; next = eol + 2; }
                    else {
                        next = eol + 2 + static_cast<size_t>(len) + 2;
                        if (next > c.in.size()) break;
                    }
                } else if (type == '+' || type == ':') {
                    next = eol + 2;
                } else if (type == '-') {
                    error = true;
                    next = eol + 2;
                } else {
                    res->protocol_errors++;
                    next = c.in.size();
                }
                pos = next;
                const bool is_get = c.kinds[c.received] != 0;
                c.received++;
                if (t >= measure_start) {
                    res->lat.record(t - c.batch_start);
                    res->requests++;
                    size_t sec = static_cast<size_t>((t - measure_start) / 1000000000ULL);
                    if (sec < res->per_second.size()) res->per_second[sec]++;
                    if (is_get) { res->gets++; if (nil) res->misses++; else res->hits++; }
                    else res->sets++;
                    if (error) res->errors++;
                }
            }
            if (pos > 0) c.in.erase(0, pos);
            if (c.received == c.kinds.size() && c.fd >= 0) {  // batch complete: closed loop
                build_batch(c);
                if (!try_send(c, idx)) { close(c.fd); c.fd = -1; }
            }
        }
    }
    for (auto& c : conns) if (c.fd >= 0) close(c.fd);
    close(ep);
}

std::string json_escape(const std::string& s) {
    std::string o;
    for (char ch : s) {
        if (ch == '"' || ch == '\\') { o += '\\'; o += ch; }
        else if (static_cast<unsigned char>(ch) < 0x20) o += ' ';
        else o += ch;
    }
    return o;
}

void usage() {
    std::puts(
        "distcache_loadgen [options]   closed-loop, duration-based load generator (prints one JSON line)\n"
        "  --host H --port P        target (default 127.0.0.1:6380)\n"
        "  --clients N              connections (default 50)\n"
        "  --threads N              client threads (default 4, capped at --clients)\n"
        "  --pipeline N             commands per batch per connection (default 1)\n"
        "  --duration S --warmup S  measured / discarded seconds (default 10 / 2)\n"
        "  --keyspace N             distinct keys (default 100000)\n"
        "  --value-size N           bytes per SET value (default 64)\n"
        "  --get-ratio R            fraction of GETs, 0..1 (default 0.8; 0 = SET only, 1 = GET only)\n"
        "  --ttl-ms N               SETs carry PX N (default 0 = no TTL)\n"
        "  --key-dist uniform|zipf  key popularity (default uniform); --zipf-s S (default 0.99)\n"
        "  --preload                SET every key once before the run (not measured)\n"
        "  --seed N --label TEXT");
}

}  // namespace

int main(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) { std::fprintf(stderr, "missing value for %s\n", a.c_str()); std::exit(2); }
            return argv[++i];
        };
        if (a == "--host") o.host = next();
        else if (a == "--port") o.port = std::atoi(next());
        else if (a == "--clients") o.clients = std::atoi(next());
        else if (a == "--threads") o.threads = std::atoi(next());
        else if (a == "--pipeline") o.pipeline = std::atoi(next());
        else if (a == "--duration") o.duration = std::atof(next());
        else if (a == "--warmup") o.warmup = std::atof(next());
        else if (a == "--keyspace") o.keyspace = static_cast<uint64_t>(std::atoll(next()));
        else if (a == "--value-size") o.value_size = std::atoi(next());
        else if (a == "--get-ratio") o.get_ratio = std::atof(next());
        else if (a == "--ttl-ms") o.ttl_ms = std::atoll(next());
        else if (a == "--key-dist") o.key_dist = next();
        else if (a == "--zipf-s") o.zipf_s = std::atof(next());
        else if (a == "--preload") o.preload = true;
        else if (a == "--seed") o.seed = static_cast<uint64_t>(std::atoll(next()));
        else if (a == "--label") o.label = next();
        else if (a == "--help" || a == "-h") { usage(); return 0; }
        else { std::fprintf(stderr, "unknown option %s\n", a.c_str()); usage(); return 2; }
    }
    if (o.clients < 1 || o.pipeline < 1 || o.keyspace < 1 || o.duration <= 0) { usage(); return 2; }
    o.threads = std::max(1, std::min(o.threads, o.clients));

    const std::string value(static_cast<size_t>(o.value_size), 'x');
    KeyDist dist;
    dist.init(o);

    if (o.preload && !preload_keys(o, value)) { std::fprintf(stderr, "preload failed (server down?)\n"); return 1; }
    auto info_before = server_info(o);
    if (info_before.empty()) { std::fprintf(stderr, "cannot reach server at %s:%d\n", o.host.c_str(), o.port); return 1; }

    const uint64_t t0 = now_ns();
    const uint64_t measure_start = t0 + static_cast<uint64_t>(o.warmup * 1e9);
    const uint64_t end = measure_start + static_cast<uint64_t>(o.duration * 1e9);

    std::vector<ThreadResult> results(static_cast<size_t>(o.threads));
    std::vector<std::thread> ts;
    for (int t = 0; t < o.threads; ++t) {
        int conns = o.clients / o.threads + (t < o.clients % o.threads ? 1 : 0);
        ts.emplace_back(run_thread, std::cref(o), std::cref(dist), std::cref(value), conns, t, measure_start, end,
                        &results[static_cast<size_t>(t)]);
    }
    for (auto& th : ts) th.join();

    ThreadResult total;
    total.per_second.assign(results[0].per_second.size(), 0);
    for (auto& r : results) {
        total.lat.merge(r.lat);
        total.gets += r.gets; total.sets += r.sets; total.hits += r.hits; total.misses += r.misses;
        total.errors += r.errors; total.protocol_errors += r.protocol_errors; total.requests += r.requests;
        total.connect_failures += r.connect_failures;
        for (size_t i = 0; i < r.per_second.size(); ++i) total.per_second[i] += r.per_second[i];
    }
    auto info_after = server_info(o);

    const double rps = static_cast<double>(total.requests) / o.duration;
    const auto us = [&](uint64_t ns) { return static_cast<double>(ns) / 1000.0; };
    std::printf("{\"label\":\"%s\",\"config\":{\"host\":\"%s\",\"port\":%d,\"clients\":%d,\"threads\":%d,"
                "\"pipeline\":%d,\"duration_s\":%.2f,\"warmup_s\":%.2f,\"keyspace\":%llu,\"value_size\":%d,"
                "\"get_ratio\":%.3f,\"ttl_ms\":%lld,\"key_dist\":\"%s\",\"preload\":%s,\"seed\":%llu},",
                json_escape(o.label).c_str(), o.host.c_str(), o.port, o.clients, o.threads, o.pipeline, o.duration,
                o.warmup, static_cast<unsigned long long>(o.keyspace), o.value_size, o.get_ratio,
                static_cast<long long>(o.ttl_ms), o.key_dist.c_str(), o.preload ? "true" : "false",
                static_cast<unsigned long long>(o.seed));
    std::printf("\"result\":{\"requests\":%llu,\"gets\":%llu,\"sets\":%llu,\"hits\":%llu,\"misses\":%llu,"
                "\"errors\":%llu,\"protocol_errors\":%llu,\"connect_failures\":%llu,\"rps\":%.1f,",
                static_cast<unsigned long long>(total.requests), static_cast<unsigned long long>(total.gets),
                static_cast<unsigned long long>(total.sets), static_cast<unsigned long long>(total.hits),
                static_cast<unsigned long long>(total.misses), static_cast<unsigned long long>(total.errors),
                static_cast<unsigned long long>(total.protocol_errors),
                static_cast<unsigned long long>(total.connect_failures), rps);
    std::printf("\"latency_us\":{\"min\":%.1f,\"mean\":%.1f,\"p50\":%.1f,\"p90\":%.1f,\"p95\":%.1f,\"p99\":%.1f,"
                "\"p999\":%.1f,\"max\":%.1f},",
                us(total.lat.min()), total.lat.mean() / 1000.0, us(total.lat.quantile(0.50)),
                us(total.lat.quantile(0.90)), us(total.lat.quantile(0.95)), us(total.lat.quantile(0.99)),
                us(total.lat.quantile(0.999)), us(total.lat.max()));
    std::printf("\"per_second\":[");
    const size_t secs = static_cast<size_t>(std::ceil(o.duration));
    for (size_t i = 0; i < secs && i < total.per_second.size(); ++i)
        std::printf("%s%llu", i ? "," : "", static_cast<unsigned long long>(total.per_second[i]));
    std::printf("]},\"server\":{");
    bool first = true;
    for (const char* k : {"mode", "shards", "keys", "max_keys", "evicted_keys", "expired_keys", "fsync", "wal_fsyncs",
                          "wal_records", "wal_segment_bytes", "snapshots_taken", "recovery_ms"}) {
        auto b = info_before.find(k), a = info_after.find(k);
        if (a == info_after.end()) continue;
        std::printf("%s\"%s\":\"%s\"", first ? "" : ",", k, json_escape(a->second).c_str());
        first = false;
        if (b != info_before.end() && b->second != a->second)
            std::printf(",\"%s_before\":\"%s\"", k, json_escape(b->second).c_str());
    }
    for (const auto& kv : info_after) {  // diagnostics build: every diag_* counter, verbatim
        if (kv.first.compare(0, 5, "diag_") != 0) continue;
        std::printf("%s\"%s\":\"%s\"", first ? "" : ",", kv.first.c_str(), json_escape(kv.second).c_str());
        first = false;
    }
    std::printf("}}\n");
    return total.errors || total.protocol_errors || total.connect_failures ? 3 : 0;
}
