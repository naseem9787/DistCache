// Consistent hash ring: placement contract, minimal disruption, balance, weights.
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "glob.hpp"
#include "hash.hpp"
#include "hash_ring.hpp"

namespace {

std::vector<HashRing::Node> make_nodes(int n, uint32_t weight = 1) {
    std::vector<HashRing::Node> v;
    for (int i = 0; i < n; ++i) v.push_back({"node-" + std::to_string(i), weight});
    return v;
}

std::string key_of(int i) { return "key:" + std::to_string(i); }

double stddev_over_mean(const std::vector<double>& v) {
    double mean = 0;
    for (double x : v) mean += x;
    mean /= static_cast<double>(v.size());
    double var = 0;
    for (double x : v) var += (x - mean) * (x - mean);
    return std::sqrt(var / static_cast<double>(v.size())) / mean;
}

}  // namespace

// ---- hash function: pinned, because it IS the placement contract -------------------------------

TEST(Hash, MatchesIndependentReferenceImplementation) {
    // Values computed by tests' sibling script (murmur64a in Python, written separately from the
    // C++ code). If this fails, every key in every cluster would be re-mapped.
    struct V { const char* s; uint64_t h; };
    const V vectors[] = {
        {"", 0x0000000000000000ULL},
        {"a", 0x071717d2d36b6b11ULL},
        {"abc", 0x9cc9c33498a95efbULL},
        {"hello world", 0xd3ba2368a832afceULL},
        {"user:123", 0xca8d71afe8beaf15ULL},
        {"key:0000000001", 0x06e859b9dc2dc2b4ULL},
        {"node-a#0", 0x99f4a2eeaf276e35ULL},
        {"node-a#159", 0xc24af03848c0fda3ULL},
        {"0123456789abcdef", 0x93a92d1a91a24bc7ULL},   // exactly two 8-byte blocks, no tail
        {"0123456789abcdefg", 0x0b3d8f28dd9e798fULL},  // two blocks and a 1-byte tail
    };
    for (const V& v : vectors) EXPECT_EQ(murmur64a(v.s, std::strlen(v.s)), v.h) << v.s;
}

TEST(Hash, HashTagRules) {
    EXPECT_EQ(hash_tag("plain"), "plain");
    EXPECT_EQ(hash_tag("user:{42}:cart"), "42");
    EXPECT_EQ(hash_tag("{tag}"), "tag");
    EXPECT_EQ(hash_tag("a{}b"), "a{}b");           // empty tag: whole key
    EXPECT_EQ(hash_tag("a{b"), "a{b");              // unterminated
    EXPECT_EQ(hash_tag("a}b{c}"), "c");             // the first '{' counts, not a stray '}'
    EXPECT_EQ(hash_tag("{a}{b}"), "a");             // only the first tag
    EXPECT_EQ(key_hash("x{tag}y"), key_hash("{tag}"));
    EXPECT_EQ(key_hash("{tag}"), 0xaafa10a38d65c4cbULL);  // = murmur64a("tag"): the tag, not the braces
}

// ---- ring behaviour --------------------------------------------------------------------------

TEST(HashRing, EmptyRingHasNoOwner) {
    HashRing r;
    EXPECT_TRUE(r.empty());
    EXPECT_EQ(r.lookup("k"), nullptr);
    EXPECT_EQ(r.owner_index(123), -1);
    EXPECT_TRUE(r.owner_indices(1, 3).empty());
}

TEST(HashRing, SingleNodeOwnsEverything) {
    HashRing r(make_nodes(1));
    for (int i = 0; i < 1000; ++i) ASSERT_EQ(*r.lookup(key_of(i)), "node-0");
    EXPECT_NEAR(r.ownership()[0], 1.0, 1e-9);
}

TEST(HashRing, PlacementDoesNotDependOnInsertionOrder) {
    std::vector<HashRing::Node> a = {{"a", 1}, {"b", 1}, {"c", 1}};
    std::vector<HashRing::Node> b = {{"c", 1}, {"a", 1}, {"b", 1}};
    HashRing ra(a), rb(b);
    for (int i = 0; i < 5000; ++i) ASSERT_EQ(*ra.lookup(key_of(i)), *rb.lookup(key_of(i)));
    // ... and the incremental route to the same membership gives the same ring
    HashRing rc = HashRing({{"a", 1}}).with_node({"b", 1}).with_node({"c", 1});
    for (int i = 0; i < 5000; ++i) ASSERT_EQ(*ra.lookup(key_of(i)), *rc.lookup(key_of(i)));
}

TEST(HashRing, DuplicateNamesAreIgnoredAndZeroWeightIsOne) {
    HashRing r({{"a", 1}, {"a", 5}, {"b", 0}});
    ASSERT_EQ(r.node_count(), 2u);
    EXPECT_EQ(r.nodes()[0].weight, 1u);
    EXPECT_EQ(r.nodes()[1].weight, 1u);
}

TEST(HashRing, WrapsAroundAtTheTopOfTheCircle) {
    HashRing r(make_nodes(3));
    EXPECT_EQ(r.owner_index(~uint64_t{0}), r.owner_index(0));
}

TEST(HashRing, OwnershipSumsToOneAndMatchesSampledKeys) {
    HashRing r(make_nodes(5));
    const auto share = r.ownership();
    double total = 0;
    for (double s : share) total += s;
    EXPECT_NEAR(total, 1.0, 1e-9);
    std::vector<int> count(5, 0);
    constexpr int kKeys = 100000;
    for (int i = 0; i < kKeys; ++i) ++count[static_cast<size_t>(r.owner_index(key_hash(key_of(i))))];
    for (size_t n = 0; n < 5; ++n)
        EXPECT_NEAR(static_cast<double>(count[n]) / kKeys, share[n], 0.01) << "node " << n;
}

// The defining property of consistent hashing, checked for several cluster sizes.
TEST(HashRing, AddingANodeMovesOnlyKeysToTheNewNodeAndAboutOneNth) {
    constexpr int kKeys = 100000;
    for (int n = 1; n <= 9; ++n) {
        HashRing before(make_nodes(n));
        HashRing after = before.with_node({"node-new", 1});
        int moved = 0;
        for (int i = 0; i < kKeys; ++i) {
            const std::string k = key_of(i);
            const std::string& o1 = *before.lookup(k);
            const std::string& o2 = *after.lookup(k);
            if (o1 != o2) {
                ++moved;
                ASSERT_EQ(o2, "node-new") << "a key moved between two OLD nodes (n=" << n << ")";
            }
        }
        const double fraction = static_cast<double>(moved) / kKeys;
        const double ideal = 1.0 / (n + 1);
        EXPECT_GT(fraction, ideal * 0.7) << "n=" << n;
        EXPECT_LT(fraction, ideal * 1.3) << "n=" << n;
    }
}

TEST(HashRing, RemovingANodeMovesOnlyItsOwnKeys) {
    constexpr int kKeys = 100000;
    HashRing before(make_nodes(6));
    HashRing after = before.without_node("node-3");
    int moved = 0;
    for (int i = 0; i < kKeys; ++i) {
        const std::string k = key_of(i);
        const std::string& o1 = *before.lookup(k);
        const std::string& o2 = *after.lookup(k);
        if (o1 == "node-3") {
            ++moved;
            ASSERT_NE(o2, "node-3");
        } else {
            ASSERT_EQ(o1, o2) << "a key that was not on the removed node moved";
        }
    }
    const double fraction = static_cast<double>(moved) / kKeys;
    EXPECT_GT(fraction, 1.0 / 6 * 0.7);
    EXPECT_LT(fraction, 1.0 / 6 * 1.3);
}

// The contrast that motivates the ring: hash(key) % N reshuffles nearly everything.
TEST(HashRing, ModuloPlacementMovesAlmostEveryKeyWhenNChanges) {
    constexpr int kKeys = 100000;
    int moved = 0;
    for (int i = 0; i < kKeys; ++i) {
        const uint64_t h = key_hash(key_of(i));
        if (h % 4 != h % 5) ++moved;
    }
    EXPECT_GT(static_cast<double>(moved) / kKeys, 0.75);  // expected 4/5
}

TEST(HashRing, VirtualNodesImproveBalance) {
    double prev = 1e9;
    for (uint32_t vn : {1u, 8u, 64u, 512u}) {
        HashRing r(make_nodes(10), vn);
        const double cv = stddev_over_mean(r.ownership());
        EXPECT_LT(cv, prev) << "vnodes=" << vn;
        prev = cv;
    }
    // with the default 160 points per node the shares are within ~25 % of fair
    HashRing r(make_nodes(10));
    for (double s : r.ownership()) {
        EXPECT_GT(s, 0.1 * 0.75);
        EXPECT_LT(s, 0.1 * 1.25);
    }
}

TEST(HashRing, WeightScalesOwnership) {
    HashRing r({{"small", 1}, {"big", 3}});
    const auto share = r.ownership();  // sorted by name: big, small
    EXPECT_NEAR(share[0], 0.75, 0.05);
    EXPECT_NEAR(share[1], 0.25, 0.05);
}

TEST(HashRing, OwnerIndicesAreDistinctClockwiseAndStartWithTheOwner) {
    HashRing r(make_nodes(5));
    for (int i = 0; i < 2000; ++i) {
        const uint64_t h = key_hash(key_of(i));
        auto o = r.owner_indices(h, 3);
        ASSERT_EQ(o.size(), 3u);
        EXPECT_EQ(o[0], r.owner_index(h));
        EXPECT_EQ(std::set<int>(o.begin(), o.end()).size(), 3u);
    }
    EXPECT_EQ(r.owner_indices(7, 99).size(), 5u);  // capped at the number of nodes
}

TEST(HashRing, HashTagsKeepRelatedKeysOnOneNode) {
    HashRing r(make_nodes(8));
    for (int u = 0; u < 200; ++u) {
        const std::string tag = "{user" + std::to_string(u) + "}";
        const std::string& a = *r.lookup(tag + ":profile");
        EXPECT_EQ(*r.lookup(tag + ":cart"), a);
        EXPECT_EQ(*r.lookup("session" + tag), a);
    }
}

// ---- KEYS glob ----------------------------------------------------------------------------------

TEST(Glob, Basics) {
    EXPECT_TRUE(glob_match("*", ""));
    EXPECT_TRUE(glob_match("*", "anything"));
    EXPECT_TRUE(glob_match("user:*", "user:42"));
    EXPECT_FALSE(glob_match("user:*", "usr:42"));
    EXPECT_TRUE(glob_match("a?c", "abc"));
    EXPECT_FALSE(glob_match("a?c", "ac"));
    EXPECT_TRUE(glob_match("*:42:*", "user:42:cart"));
    EXPECT_TRUE(glob_match("a*b*c", "aXXbYYc"));
    EXPECT_FALSE(glob_match("a*b*c", "aXXbYY"));
    EXPECT_TRUE(glob_match("\\*", "*"));
    EXPECT_FALSE(glob_match("\\*", "x"));
    EXPECT_TRUE(glob_match("", ""));
    EXPECT_FALSE(glob_match("", "x"));
    EXPECT_TRUE(glob_match("a**b", "ab"));
}

TEST(Glob, HostilePatternDoesNotBlowUp) {
    const std::string text(2000, 'a');
    std::string pat;
    for (int i = 0; i < 200; ++i) pat += "a*";
    pat += "b";
    EXPECT_FALSE(glob_match(pat, text));  // must finish quickly (no exponential backtracking)
}
