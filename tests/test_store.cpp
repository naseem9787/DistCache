#include <gtest/gtest.h>

#include "store.hpp"

// A fake clock: tests move time forward by hand instead of sleeping.
struct FakeClock {
    int64_t t = 1000;
    Store::Clock fn() { return [this] { return t; }; }
};

TEST(StoreTtl, KeyWithoutTtlNeverExpires) {
    FakeClock c; Store s(c.fn());
    s.set("k", "v");
    c.t += 1'000'000;
    EXPECT_EQ(s.get("k"), "v");
    EXPECT_EQ(s.ttl_ms("k"), -1);
}

TEST(StoreTtl, LazyExpirationOnGet) {
    FakeClock c; Store s(c.fn());
    s.set("k", "v", 5000);
    c.t += 4999;
    EXPECT_EQ(s.get("k"), "v");
    EXPECT_EQ(s.ttl_ms("k"), 1);
    c.t += 1;                       // exactly at the deadline: expired
    EXPECT_FALSE(s.get("k"));
    EXPECT_EQ(s.size(), 0u);        // the GET physically removed it
    EXPECT_EQ(s.ttl_ms("k"), -2);
}

TEST(StoreTtl, ExpiredKeyStaysInMemoryUntilTouchedOrSwept) {
    FakeClock c; Store s(c.fn());
    s.set("k", "v", 100);
    c.t += 200;
    EXPECT_EQ(s.size(), 1u);        // nobody looked at it yet
    EXPECT_EQ(s.sweep(), 1u);       // active expiration cleans it
    EXPECT_EQ(s.size(), 0u);
}

TEST(StoreTtl, SweepOnlyRemovesExpiredAndRespectsLimit) {
    FakeClock c; Store s(c.fn());
    for (int i = 0; i < 10; ++i) s.set("short" + std::to_string(i), "v", 100);
    s.set("long", "v", 10'000);
    s.set("forever", "v");
    c.t += 500;
    EXPECT_EQ(s.sweep(4), 4u);      // bounded work per call
    EXPECT_EQ(s.sweep(), 6u);
    EXPECT_EQ(s.sweep(), 0u);
    EXPECT_EQ(s.size(), 2u);
    EXPECT_TRUE(s.get("long"));
    EXPECT_TRUE(s.get("forever"));
}

TEST(StoreTtl, PlainSetClearsOldTtl) {
    FakeClock c; Store s(c.fn());
    s.set("k", "v1", 100);
    s.set("k", "v2");
    c.t += 1000;
    EXPECT_EQ(s.get("k"), "v2");
    EXPECT_EQ(s.sweep(), 0u);       // the stale index entry must be gone
}

TEST(StoreTtl, OverwriteWithNewTtlReplacesIndexEntry) {
    FakeClock c; Store s(c.fn());
    s.set("k", "v1", 100);
    s.set("k", "v2", 10'000);
    c.t += 500;
    EXPECT_EQ(s.sweep(), 0u);
    EXPECT_EQ(s.get("k"), "v2");
}

TEST(StoreTtl, ExpireAndPersist) {
    FakeClock c; Store s(c.fn());
    EXPECT_FALSE(s.expire("missing", 100));
    s.set("k", "v");
    EXPECT_TRUE(s.expire("k", 100));
    EXPECT_EQ(s.ttl_ms("k"), 100);
    EXPECT_TRUE(s.persist("k"));
    EXPECT_FALSE(s.persist("k"));   // already persistent
    c.t += 1000;
    EXPECT_EQ(s.get("k"), "v");
}

TEST(StoreTtl, NonPositiveExpireDeletesImmediately) {
    FakeClock c; Store s(c.fn());
    s.set("k", "v");
    EXPECT_TRUE(s.expire("k", 0));
    EXPECT_FALSE(s.get("k"));
}

TEST(StoreTtl, DelOnExpiredKeyReportsNotFound) {
    FakeClock c; Store s(c.fn());
    s.set("k", "v", 100);
    c.t += 100;
    EXPECT_FALSE(s.del("k"));
}

TEST(StoreTtl, DelRemovesIndexEntry) {
    FakeClock c; Store s(c.fn());
    s.set("k", "v", 100);
    EXPECT_TRUE(s.del("k"));
    s.set("k", "new");              // same key, no TTL
    c.t += 1000;
    EXPECT_EQ(s.sweep(), 0u);
    EXPECT_EQ(s.get("k"), "new");
}
