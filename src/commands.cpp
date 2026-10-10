#include "commands.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <optional>

#include "resp.hpp"

namespace {

// Largest TTL we accept (~31 years in ms). Keeps now()+ttl far from int64 overflow.
constexpr int64_t kMaxTtlMs = 1'000'000'000'000LL;

std::string upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); });
    return s;
}

std::string wrong_args(const std::string& cmd) {
    return resp_error("ERR wrong number of arguments for '" + cmd + "' command");
}

bool parse_i64(const std::string& s, int64_t& out) {
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && p == s.data() + s.size();
}

// Converts "<n>" in seconds or milliseconds to ms, rejecting junk and huge values.
// Returns an error reply on failure, empty string on success.
std::string parse_ttl(const std::string& text, bool seconds, int64_t& ms) {
    int64_t n;
    if (!parse_i64(text, n)) return resp_error("ERR value is not an integer or out of range");
    if (n > kMaxTtlMs / (seconds ? 1000 : 1) || n < -kMaxTtlMs) return resp_error("ERR invalid expire time");
    ms = seconds ? n * 1000 : n;
    return "";
}

std::string cmd_set(Engine& store, const std::vector<std::string>& args) {
    if (args.size() < 3) return wrong_args("set");
    std::optional<int64_t> ttl_ms;
    for (size_t i = 3; i < args.size(); ++i) {
        const std::string opt = upper(args[i]);
        if ((opt != "EX" && opt != "PX") || ttl_ms || i + 1 >= args.size()) return resp_error("ERR syntax error");
        int64_t ms;
        std::string err = parse_ttl(args[++i], opt == "EX", ms);
        if (!err.empty()) return err;
        if (ms <= 0) return resp_error("ERR invalid expire time in 'set' command");
        ttl_ms = ms;
    }
    store.set(args[1], args[2], ttl_ms);
    return resp_simple("OK");
}

// EXPIRE key seconds / PEXPIRE key milliseconds
std::string cmd_expire(Engine& store, const std::vector<std::string>& args, bool seconds) {
    if (args.size() != 3) return wrong_args(seconds ? "expire" : "pexpire");
    int64_t ms;
    std::string err = parse_ttl(args[2], seconds, ms);
    if (!err.empty()) return err;
    return resp_integer(store.expire(args[1], ms) ? 1 : 0);
}

// TTL key (seconds) / PTTL key (milliseconds): -2 no key, -1 no expiry.
std::string cmd_ttl(Engine& store, const std::vector<std::string>& args, bool seconds) {
    if (args.size() != 2) return wrong_args(seconds ? "ttl" : "pttl");
    int64_t ms = store.ttl_ms(args[1]);
    if (ms < 0) return resp_integer(ms);
    return resp_integer(seconds ? (ms + 500) / 1000 : ms);  // seconds are rounded, like Redis
}

}  // namespace

std::string execute(Engine& store, const std::vector<std::string>& args, CommandHooks* hooks) {
    if (args.empty()) return "";  // blank line: nothing to say
    const std::string cmd = upper(args[0]);

    if (cmd == "PING") {
        if (args.size() == 1) return resp_simple("PONG");
        if (args.size() == 2) return resp_bulk(args[1]);
        return wrong_args("ping");
    }
    if (cmd == "ECHO") {
        return args.size() == 2 ? resp_bulk(args[1]) : wrong_args("echo");
    }
    if (cmd == "SET") return cmd_set(store, args);
    if (cmd == "GET") {
        if (args.size() != 2) return wrong_args("get");
        auto v = store.get(args[1]);
        return v ? resp_bulk(*v) : resp_null();
    }
    if (cmd == "DEL") {
        if (args.size() < 2) return wrong_args("del");
        long long removed = 0;
        for (size_t i = 1; i < args.size(); ++i) removed += store.del(args[i]) ? 1 : 0;
        return resp_integer(removed);
    }
    if (cmd == "EXPIRE") return cmd_expire(store, args, true);
    if (cmd == "PEXPIRE") return cmd_expire(store, args, false);
    if (cmd == "TTL") return cmd_ttl(store, args, true);
    if (cmd == "PTTL") return cmd_ttl(store, args, false);
    if (cmd == "PERSIST") {
        if (args.size() != 2) return wrong_args("persist");
        return resp_integer(store.persist(args[1]) ? 1 : 0);
    }
    if (cmd == "DBSIZE") {
        return resp_integer(static_cast<long long>(store.size()));
    }
    if (cmd == "INFO") {
        return resp_bulk(std::string("# Concurrency\r\nmode:") + store.mode() +
                         "\r\nshards:" + std::to_string(store.shards()) +
                         "\r\n# Stats\r\nkeys:" + std::to_string(store.size()) +
                         "\r\nmax_keys:" + std::to_string(store.max_keys()) +
                         "\r\nevicted_keys:" + std::to_string(store.evicted_keys()) +
                         "\r\nexpired_keys:" + std::to_string(store.expired_keys()) + "\r\n" +
                         (hooks ? hooks->info() : std::string("# Persistence\r\nenabled:no\r\n")));
    }
    if (cmd == "SAVE") {
        if (args.size() != 1) return wrong_args("save");
        if (!hooks) return resp_error("ERR persistence is not enabled (start with --dir)");
        std::string err;
        return hooks->save(&err) ? resp_simple("OK") : resp_error("ERR " + err);
    }
    if (cmd == "COMMAND") {
        return "*0\r\n";  // redis-cli asks for this on startup; an empty list is fine
    }
    return resp_error("ERR unknown command '" + args[0] + "'");
}
