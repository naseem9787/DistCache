#pragma once
// Glob matching for KEYS. A deliberate subset of Redis's patterns:
//   *   any run of characters (including none)
//   ?   exactly one character
//   \x  the character x, literally (so "\*" matches a star)
// Character classes ([abc]) are NOT supported; '[' matches itself.
// Iterative with one backtrack point, so it is O(len(pattern) * len(text)) worst case and
// cannot blow the stack on a hostile pattern.
#include <string_view>

inline bool glob_match(std::string_view pattern, std::string_view text) {
    size_t p = 0, t = 0;
    size_t star_p = std::string_view::npos, star_t = 0;
    while (t < text.size()) {
        if (p < pattern.size() && pattern[p] == '*') {
            star_p = p++;
            star_t = t;
            continue;
        }
        bool match = false;
        if (p < pattern.size()) {
            if (pattern[p] == '?') {
                match = true;
                ++p;
            } else if (pattern[p] == '\\' && p + 1 < pattern.size()) {
                match = pattern[p + 1] == text[t];
                if (match) p += 2;
            } else {
                match = pattern[p] == text[t];
                if (match) ++p;
            }
        }
        if (match) {
            ++t;
        } else if (star_p != std::string_view::npos) {
            p = star_p + 1;
            t = ++star_t;
        } else {
            return false;
        }
    }
    while (p < pattern.size() && pattern[p] == '*') ++p;
    return p == pattern.size();
}
