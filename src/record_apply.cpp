#include "record_apply.hpp"

#include <cerrno>
#include <cstdlib>

namespace {

bool parse_i64(const std::string& s, int64_t* out) {
    char* end = nullptr;
    errno = 0;
    const long long v = std::strtoll(s.c_str(), &end, 10);
    if (errno != 0 || end == s.c_str() || *end != '\0') return false;
    *out = v;
    return true;
}

}  // namespace

ApplyResult apply_logical_record(Engine& engine, const std::vector<std::string>& a, int64_t now_ms) {
    if (a.empty()) return ApplyResult::Invalid;
    const std::string& cmd = a[0];
    if (cmd == "SET" && (a.size() == 3 || (a.size() == 5 && a[3] == "PXAT"))) {
        if (a.size() == 3) { engine.set(a[1], a[2]); return ApplyResult::Applied; }
        int64_t at;
        if (!parse_i64(a[4], &at)) return ApplyResult::Invalid;
        if (at - now_ms <= 0) {   // deadline already passed
            engine.del(a[1]);     // and any older value of the key must not survive either
            return ApplyResult::ExpiredSkipped;
        }
        engine.set(a[1], a[2], at - now_ms);
        return ApplyResult::Applied;
    }
    if (cmd == "DEL" && a.size() == 2) { engine.del(a[1]); return ApplyResult::Applied; }
    if (cmd == "PEXPIREAT" && a.size() == 3) {
        int64_t at;
        if (!parse_i64(a[2], &at)) return ApplyResult::Invalid;
        engine.expire(a[1], at - now_ms);  // <= 0 deletes, same as at runtime
        return ApplyResult::Applied;
    }
    if (cmd == "PERSIST" && a.size() == 2) { engine.persist(a[1]); return ApplyResult::Applied; }
    return ApplyResult::Invalid;
}
