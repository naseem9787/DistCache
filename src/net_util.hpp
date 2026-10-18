#pragma once
// Small helpers shared by the three binaries: strict command-line numbers and IPv4 bind addresses.
#include <arpa/inet.h>
#include <netinet/in.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <string>

// Parses a decimal integer in [lo, hi]. Unlike atoi, junk ("abc", "12x", "") is an error instead of
// a silent 0 (which for --port would mean "pick a random port").
inline bool parse_int_arg(const char* flag, const char* text, long long lo, long long hi, long long* out) {
    char* end = nullptr;
    errno = 0;
    const long long v = std::strtoll(text, &end, 10);
    if (end == text || *end != '\0' || errno != 0 || v < lo || v > hi) {
        std::fprintf(stderr, "%s wants an integer between %lld and %lld, got '%s'\n", flag, lo, hi, text);
        return false;
    }
    *out = v;
    return true;
}

// "127.0.0.1" / "0.0.0.0" -> network-order address for bind(). IPv4 only.
inline bool parse_bind_address(const std::string& host, in_addr_t* out) {
    in_addr a{};
    if (inet_pton(AF_INET, host.c_str(), &a) != 1) return false;
    *out = a.s_addr;
    return true;
}
