#include "resp.hpp"

#include <charconv>

namespace {

// Limits so a malicious or buggy client can't make us allocate unbounded memory.
constexpr long long kMaxArgs = 1024 * 1024;
constexpr long long kMaxBulkLen = 512LL * 1024 * 1024;

ParseResult make_error(std::string msg) {
    ParseResult r;
    r.status = ParseStatus::Error;
    r.error = std::move(msg);
    return r;
}

bool parse_int(std::string_view s, long long& out) {
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && p == s.data() + s.size();
}

// Reads one "\r\n"-terminated line starting at `pos`. On success sets `line`
// (without the CRLF) and advances `pos` past it.
bool read_line(std::string_view buf, size_t& pos, std::string_view& line) {
    size_t end = buf.find("\r\n", pos);
    if (end == std::string_view::npos) return false;
    line = buf.substr(pos, end - pos);
    pos = end + 2;
    return true;
}

ParseResult parse_inline(std::string_view buf) {
    size_t nl = buf.find('\n');
    if (nl == std::string_view::npos) return {};  // Incomplete
    std::string_view line = buf.substr(0, nl);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

    ParseResult r;
    r.status = ParseStatus::Ok;
    r.consumed = nl + 1;
    size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) ++i;
        size_t start = i;
        while (i < line.size() && line[i] != ' ' && line[i] != '\t') ++i;
        if (i > start) r.args.emplace_back(line.substr(start, i - start));
    }
    return r;
}

}  // namespace

ParseResult parse_command(std::string_view buf) {
    if (buf.empty()) return {};
    if (buf[0] != '*') return parse_inline(buf);

    size_t pos = 1;
    std::string_view line;
    if (!read_line(buf, pos, line)) return {};

    long long count;
    if (!parse_int(line, count) || count > kMaxArgs) return make_error("invalid multibulk length");

    ParseResult r;
    r.status = ParseStatus::Ok;
    if (count <= 0) {  // "*0\r\n" or "*-1\r\n": empty command, just consume it
        r.consumed = pos;
        return r;
    }

    r.args.reserve(static_cast<size_t>(count));
    for (long long i = 0; i < count; ++i) {
        if (pos >= buf.size()) return {};
        if (buf[pos] != '$') return make_error("expected '$', got other byte");
        ++pos;
        if (!read_line(buf, pos, line)) return {};

        long long len;
        if (!parse_int(line, len) || len < 0 || len > kMaxBulkLen) return make_error("invalid bulk length");

        // Need the payload plus its trailing CRLF.
        if (buf.size() - pos < static_cast<size_t>(len) + 2) return {};
        if (buf[pos + len] != '\r' || buf[pos + len + 1] != '\n') return make_error("bad bulk terminator");
        r.args.emplace_back(buf.substr(pos, static_cast<size_t>(len)));
        pos += static_cast<size_t>(len) + 2;
    }
    r.consumed = pos;
    return r;
}

std::string resp_simple(std::string_view s) { return "+" + std::string(s) + "\r\n"; }
std::string resp_error(std::string_view s) { return "-" + std::string(s) + "\r\n"; }
std::string resp_integer(long long n) { return ":" + std::to_string(n) + "\r\n"; }
std::string resp_bulk(std::string_view s) {
    return "$" + std::to_string(s.size()) + "\r\n" + std::string(s) + "\r\n";
}
std::string resp_null() { return "$-1\r\n"; }
