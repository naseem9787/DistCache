#!/usr/bin/env python3
"""Independent MurmurHash64A (seed 0) used to pin the C++ implementation in tests/test_ring.cpp."""
M = 0xC6A4A7935BD1E995
MASK = (1 << 64) - 1
R = 47


def murmur64a(data: bytes, seed: int = 0) -> int:
    n = len(data)
    h = (seed ^ (n * M)) & MASK
    nblocks = n // 8
    for i in range(nblocks):
        k = int.from_bytes(data[8 * i:8 * i + 8], "little")
        k = (k * M) & MASK
        k ^= k >> R
        k = (k * M) & MASK
        h ^= k
        h = (h * M) & MASK
    tail = data[8 * nblocks:]
    if tail:
        h ^= int.from_bytes(tail, "little")
        h = (h * M) & MASK
    h ^= h >> R
    h = (h * M) & MASK
    h ^= h >> R
    return h


if __name__ == "__main__":
    for s in ["", "a", "abc", "hello world", "user:123", "key:0000000001", "node-a#0", "node-a#159",
              "0123456789abcdef", "0123456789abcdefg", "{tag}", "tag"]:
        print(f'{{"{s}", 0x{murmur64a(s.encode()):016x}ULL}},')
