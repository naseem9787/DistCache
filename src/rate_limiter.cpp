#include "rate_limiter.hpp"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>

#include "resp.hpp"

namespace ratelimit {

namespace {

std::atomic<uint64_t> g_allowed{0}, g_denied{0}, g_errors{0};
Clock g_clock;  // empty = system clock

int64_t ceil_div(int64_t a, int64_t b) { return (a + b - 1) / b; }  // a >= 0, b > 0

bool parse_int(const std::string& s, int64_t* out) {
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), *out);
    return ec == std::errc() && p == s.data() + s.size();
}

// "12", "0.5", "2.25", "0.001" -> milli units (12000, 500, 2250, 1). At most three decimals.
bool parse_milli(const std::string& s, int64_t* out) {
    if (s.empty()) return false;
    const size_t dot = s.find('.');
    int64_t whole = 0, frac = 0;
    const std::string w = s.substr(0, dot);
    if (w.empty() || !parse_int(w, &whole) || whole < 0) return false;
    if (dot != std::string::npos) {
        std::string f = s.substr(dot + 1);
        if (f.empty() || f.size() > 3) return false;
        for (char c : f) if (c < '0' || c > '9') return false;
        while (f.size() < 3) f += '0';
        frac = std::stoll(f);
    }
    if (whole > kMaxRateMilli / 1000) return false;
    *out = whole * 1000 + frac;
    return true;
}

}  // namespace

Decision decide(const std::optional<State>& current, const Params& p, int64_t now_ms, bool consume) {
    const int64_t cap = p.capacity * kUnit;
    const int64_t rate = p.rate_milli;               // micro-tokens per millisecond
    const int64_t full_ms = ceil_div(cap, rate);     // time to refill from empty

    State s;
    bool repaired = false;
    if (!current) {
        s = {cap, now_ms};                           // a missing bucket is a full bucket
    } else {
        s = *current;
        s.tokens_ut = std::clamp<int64_t>(s.tokens_ut, 0, cap);   // capacity may have been lowered
        if (s.last_ms - now_ms > full_ms) {                       // stored time absurdly far ahead: clock fault
            s.last_ms = now_ms;                                   // (must be STORED even if this call is denied,
            repaired = true;                                      //  or refill would restart from "now" every call)
        }
        const int64_t elapsed = now_ms - s.last_ms;
        if (elapsed > 0) {                                        // negative or zero: no refill, last_ms stays
            s.tokens_ut = elapsed >= full_ms ? cap : std::min(cap, s.tokens_ut + elapsed * rate);
            s.last_ms = now_ms;
        }
    }

    Decision d;
    const int64_t need = p.cost * kUnit;
    if (s.tokens_ut >= need) {
        d.allowed = true;
        if (consume) {
            s.tokens_ut -= need;
            d.write = true;
        }
    } else {
        d.retry_after_ms = ceil_div(need - s.tokens_ut, rate);
    }
    d.state = s;
    d.remaining = s.tokens_ut / kUnit;
    d.reset_after_ms = ceil_div(cap - s.tokens_ut, rate);
    if (repaired && consume) d.write = true;         // a read-only peek never mutates, it just reports the repaired view
    if (d.write) d.ttl_ms = std::max<int64_t>(1, d.reset_after_ms);   // key lives exactly until the bucket is full
    return d;
}

std::string encode(const State& s) { return "RL1 " + std::to_string(s.tokens_ut) + " " + std::to_string(s.last_ms); }

std::optional<State> decode(const std::string& v) {
    if (v.compare(0, 4, "RL1 ") != 0) return std::nullopt;
    const size_t sp = v.find(' ', 4);
    if (sp == std::string::npos) return std::nullopt;
    State s;
    if (!parse_int(v.substr(4, sp - 4), &s.tokens_ut) || !parse_int(v.substr(sp + 1), &s.last_ms)) return std::nullopt;
    if (s.tokens_ut < 0) return std::nullopt;
    return s;
}

std::string parse_params(const std::vector<std::string>& args, Params* p) {
    const std::string name = args[0];
    if (args.size() < 4 || args.size() > 5)
        return resp_error("ERR wrong number of arguments for '" + name + "' command");
    int64_t cap = 0, cost = 1, rate = 0;
    if (!parse_int(args[2], &cap) || cap < 1 || cap > kMaxCapacity)
        return resp_error("ERR capacity must be an integer between 1 and 1000000000");
    if (!parse_milli(args[3], &rate) || rate < 1 || rate > kMaxRateMilli)
        return resp_error("ERR rate must be tokens per second, 0.001 to 1000000, at most 3 decimals");
    if (args.size() == 5 && (!parse_int(args[4], &cost) || cost < 1))
        return resp_error("ERR cost must be a positive integer");
    if (cost > cap) return resp_error("ERR cost exceeds capacity");
    *p = {cap, rate, cost};
    return "";
}

int64_t system_wall_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}
void set_clock(Clock clock) { g_clock = std::move(clock); }
int64_t now_ms() { return g_clock ? g_clock() : system_wall_ms(); }

std::string run(Engine& engine, const std::vector<std::string>& args, bool consume) {
    Params p;
    std::string err = parse_params(args, &p);
    if (!err.empty()) { g_errors.fetch_add(1, std::memory_order_relaxed); return err; }

    Decision d;
    bool not_a_bucket = false;
    engine.update(args[1], [&](const std::optional<std::string>& cur) -> std::optional<EngineWrite> {
        std::optional<State> st;
        if (cur) {
            st = decode(*cur);
            if (!st) { not_a_bucket = true; return std::nullopt; }
        }
        d = decide(st, p, now_ms(), consume);
        if (!d.write) return std::nullopt;
        return EngineWrite{encode(d.state), d.ttl_ms};
    });
    if (not_a_bucket) {
        g_errors.fetch_add(1, std::memory_order_relaxed);
        return resp_error("WRONGTYPE key holds a value that is not a rate limiter bucket");
    }
    if (consume) (d.allowed ? g_allowed : g_denied).fetch_add(1, std::memory_order_relaxed);
    return "*4\r\n" + resp_integer(d.allowed ? 1 : 0) + resp_integer(d.remaining) + resp_integer(d.retry_after_ms) +
           resp_integer(d.reset_after_ms);
}

Counters counters() {
    return {g_allowed.load(std::memory_order_relaxed), g_denied.load(std::memory_order_relaxed),
            g_errors.load(std::memory_order_relaxed)};
}

}  // namespace ratelimit
