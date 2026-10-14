#pragma once
// Hashing for key placement.
//
// MurmurHash64A (Austin Appleby, public domain), seed 0. The hash is part of the *placement
// contract*: the router, the smart client in the load generator and every future node must map
// a key to the same ring position, so the function and its output are pinned by tests
// (tests/test_ring.cpp checks values produced by an independent implementation). Changing it
// would silently re-map every key in every deployed cluster.
//
// The implementation reads 8 bytes at a time through memcpy, i.e. little-endian byte order
// (x86-64 and AArch64 Linux). It would produce different values on a big-endian machine.
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

inline uint64_t murmur64a(const void* data, size_t len, uint64_t seed = 0) {
    constexpr uint64_t m = 0xc6a4a7935bd1e995ULL;
    constexpr int r = 47;
    uint64_t h = seed ^ (len * m);
    const unsigned char* p = static_cast<const unsigned char*>(data);
    const unsigned char* const end = p + (len / 8) * 8;
    while (p != end) {
        uint64_t k;
        std::memcpy(&k, p, 8);
        p += 8;
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
    }
    switch (len & 7) {
        case 7: h ^= static_cast<uint64_t>(p[6]) << 48; [[fallthrough]];
        case 6: h ^= static_cast<uint64_t>(p[5]) << 40; [[fallthrough]];
        case 5: h ^= static_cast<uint64_t>(p[4]) << 32; [[fallthrough]];
        case 4: h ^= static_cast<uint64_t>(p[3]) << 24; [[fallthrough]];
        case 3: h ^= static_cast<uint64_t>(p[2]) << 16; [[fallthrough]];
        case 2: h ^= static_cast<uint64_t>(p[1]) << 8; [[fallthrough]];
        case 1: h ^= static_cast<uint64_t>(p[0]); h *= m;
    }
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

// Hash tag: if the key contains "{...}" with at least one character between the FIRST '{'
// and the first '}' after it, only that part is hashed ("user:{42}:profile" and
// "user:{42}:cart" land on the same node). Same rule as Redis Cluster, so multi-key
// operations can be kept on one node by the application.
inline std::string_view hash_tag(std::string_view key) {
    const size_t open = key.find('{');
    if (open == std::string_view::npos) return key;
    const size_t close = key.find('}', open + 1);
    if (close == std::string_view::npos || close == open + 1) return key;
    return key.substr(open + 1, close - open - 1);
}

// The position of a key on the ring.
inline uint64_t key_hash(std::string_view key) {
    const std::string_view t = hash_tag(key);
    return murmur64a(t.data(), t.size());
}
