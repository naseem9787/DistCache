#include <gtest/gtest.h>

#include "commands.hpp"
#include "store.hpp"

namespace {
struct FakeClock {
    int64_t t = 1000;
    Store::Clock fn() { return [this] { return t; }; }
};
}  // namespace

TEST(StoreLru, EvictsLeastRecentlyUsed) {
    Store s(Store::real_now_ms, 3);
    s.set("A", "1"); s.set("B", "2"); s.set("C", "3");
    s.set("D", "4");                       // full: A is the oldest
    EXPECT_FALSE(s.get("A"));
    EXPECT_TRUE(s.get("B") && s.get("C") && s.get("D"));
    EXPECT_EQ(s.size(), 3u);
    EXPECT_EQ(s.evicted_keys(), 1u);
}

TEST(StoreLru, GetRefreshesRecency) {
    Store s(Store::real_now_ms, 3);
    s.set("A", "1"); s.set("B", "2"); s.set("C", "3");
    s.get("A");                            // order now: A C B (B is oldest)
    s.set("D", "4");
    EXPECT_FALSE(s.get("B"));
    EXPECT_TRUE(s.get("A") && s.get("C") && s.get("D"));
}

TEST(StoreLru, OverwriteRefreshesRecencyAndDoesNotEvict) {
    Store s(Store::real_now_ms, 2);
    s.set("A", "1"); s.set("B", "2");
    s.set("A", "new");                     // existing key: no eviction, A becomes newest
    EXPECT_EQ(s.evicted_keys(), 0u);
    s.set("C", "3");                       // evicts B
    EXPECT_FALSE(s.get("B"));
    EXPECT_EQ(s.get("A"), "new");
}

TEST(StoreLru, DelFreesASlot) {
    Store s(Store::real_now_ms, 2);
    s.set("A", "1"); s.set("B", "2");
    s.del("A");
    s.set("C", "3");
    EXPECT_EQ(s.evicted_keys(), 0u);
    EXPECT_TRUE(s.get("B") && s.get("C"));
}

TEST(StoreLru, CapacityOne) {
    Store s(Store::real_now_ms, 1);
    s.set("A", "1"); s.set("B", "2");
    EXPECT_FALSE(s.get("A"));
    EXPECT_EQ(s.get("B"), "2");
}

TEST(StoreLru, ZeroMeansUnlimited) {
    Store s;
    for (int i = 0; i < 5000; ++i) s.set("k" + std::to_string(i), "v");
    EXPECT_EQ(s.size(), 5000u);
    EXPECT_EQ(s.evicted_keys(), 0u);
}

TEST(StoreLru, ExpiredKeysAreReclaimedBeforeEvictingLiveOnes) {
    FakeClock c; Store s(c.fn(), 3);
    s.set("live1", "v"); s.set("short", "v", 100); s.set("live2", "v");
    c.t += 500;                            // "short" has expired but is still stored
    s.set("new", "v");                     // should drop "short", not the LRU live key
    EXPECT_EQ(s.evicted_keys(), 0u);
    EXPECT_TRUE(s.get("live1") && s.get("live2") && s.get("new"));
    EXPECT_EQ(s.expired_keys(), 1u);
}

TEST(StoreLru, EvictedKeyLeavesNoStaleTtlEntry) {
    FakeClock c; Store s(c.fn(), 1);
    s.set("A", "1", 100);
    s.set("B", "2");                       // evicts A (and must drop its deadline entry)
    c.t += 1000;
    EXPECT_EQ(s.sweep(), 0u);
    EXPECT_EQ(s.get("B"), "2");
}

TEST(StoreLru, ManyInsertsNeverExceedCapacity) {
    Store s(Store::real_now_ms, 100);
    for (int i = 0; i < 10000; ++i) {
        s.set("k" + std::to_string(i), "v");
        if (i % 3 == 0) s.get("k" + std::to_string(i / 2));
        ASSERT_LE(s.size(), 100u);
    }
    EXPECT_EQ(s.evicted_keys(), 10000u - 100u);
    // The 100 most recent inserts must all survive (nothing else was touched after them).
    EXPECT_TRUE(s.get("k9999"));
}

TEST(CommandsInfo, ReportsKeysAndEvictions) {
    Store s(Store::real_now_ms, 1);
    execute(s, {"SET", "a", "1"});
    execute(s, {"SET", "b", "2"});
    std::string info = execute(s, {"INFO"});
    EXPECT_NE(info.find("keys:1"), std::string::npos);
    EXPECT_NE(info.find("evicted_keys:1"), std::string::npos);
    EXPECT_NE(info.find("max_keys:1"), std::string::npos);
}
