// Rate limiter (Phase 9): the token-bucket algorithm as a pure function, then the RL.ACQUIRE /
// RL.PEEK commands against real engines with an injected clock. Time is always fake here, so
// every refill, boundary and clock fault is exact and nothing sleeps.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "commands.hpp"
#include "rate_limiter.hpp"
#include "synced_store.hpp"

namespace {

using namespace ratelimit;

// 5 tokens, 2 tokens/s  ==  2000 micro-tokens per ms
Params P(int64_t cap, const char* rate_text, int64_t cost = 1) {
    int64_t milli = 0;
    std::vector<std::string> a = {"RL.ACQUIRE", "k", std::to_string(cap), rate_text, std::to_string(cost)};
    Params p;
    EXPECT_EQ(parse_params(a, &p), "") << rate_text;
    milli = p.rate_milli;
    (void)milli;
    return p;
}

// Drives decide() the way the command does: keeps the stored state between calls.
struct Bucket {
    Params p;
    std::optional<State> st;
    Decision call(int64_t now, bool consume = true) {
        Decision d = decide(st, p, now, consume);
        if (d.write) st = d.state;
        return d;
    }
};

// ---------------------------------------------------------------------------------------------
// Capacity and refill
// ---------------------------------------------------------------------------------------------

TEST(TokenBucket, AMissingBucketIsFullAndTheCapacityIsAHardLimit) {
    Bucket b{P(5, "1")};
    for (int i = 0; i < 5; ++i) {
        Decision d = b.call(1000);
        EXPECT_TRUE(d.allowed) << i;
        EXPECT_EQ(d.remaining, 4 - i);
    }
    Decision d = b.call(1000);
    EXPECT_FALSE(d.allowed);
    EXPECT_EQ(d.remaining, 0);
    EXPECT_EQ(d.retry_after_ms, 1000);   // 1 token/s
    EXPECT_FALSE(d.write);               // a denial stores nothing
}

TEST(TokenBucket, RefillIsExactAtTheMillisecondBoundary) {
    Bucket b{P(5, "2")};                  // 2 tokens/s: one token per 500 ms
    for (int i = 0; i < 5; ++i) ASSERT_TRUE(b.call(0).allowed);
    Decision d = b.call(499);
    EXPECT_FALSE(d.allowed);
    EXPECT_EQ(d.retry_after_ms, 1);       // one more millisecond
    d = b.call(500);
    EXPECT_TRUE(d.allowed);               // exactly one token has accrued
    EXPECT_EQ(d.remaining, 0);
    EXPECT_FALSE(b.call(500).allowed);
    EXPECT_FALSE(b.call(999).allowed);
    EXPECT_TRUE(b.call(1000).allowed);
}

TEST(TokenBucket, FractionalRatesAreExact) {
    Bucket b{P(1, "0.5")};                // one token per 2000 ms
    ASSERT_TRUE(b.call(0).allowed);
    EXPECT_EQ(b.call(1999).retry_after_ms, 1);
    EXPECT_TRUE(b.call(2000).allowed);
    Bucket slow{P(1, "0.001")};           // the smallest rate: one token per 1000 s
    ASSERT_TRUE(slow.call(0).allowed);
    EXPECT_EQ(slow.call(0).retry_after_ms, 1'000'000);
    EXPECT_TRUE(slow.call(1'000'000).allowed);
}

TEST(TokenBucket, RefillNeverExceedsCapacityHoweverLongTheBucketIdles) {
    Bucket b{P(3, "10")};
    for (int i = 0; i < 3; ++i) ASSERT_TRUE(b.call(0).allowed);
    for (int64_t idle : {1000LL, 3600LL * 1000, 365LL * 24 * 3600 * 1000}) {
        Bucket c = b;
        int allowed = 0;
        while (c.call(idle).allowed) ++allowed;
        EXPECT_EQ(allowed, 3) << "idle " << idle;
    }
}

TEST(TokenBucket, CostConsumesSeveralTokensAndCanEqualCapacity) {
    Bucket b{P(10, "1", 4)};
    EXPECT_TRUE(b.call(0).allowed);
    EXPECT_TRUE(b.call(0).allowed);
    Decision d = b.call(0);               // 2 left, 4 needed
    EXPECT_FALSE(d.allowed);
    EXPECT_EQ(d.remaining, 2);
    EXPECT_EQ(d.retry_after_ms, 2000);
    Bucket whole{P(10, "1", 10)};
    EXPECT_TRUE(whole.call(0).allowed);
    EXPECT_EQ(whole.call(0).retry_after_ms, 10'000);
    EXPECT_TRUE(whole.call(10'000).allowed);
}

TEST(TokenBucket, PeekAnswersLikeAcquireButConsumesNothing) {
    Bucket b{P(2, "1")};
    for (int i = 0; i < 10; ++i) EXPECT_TRUE(b.call(0, false).allowed);
    EXPECT_TRUE(b.call(0).allowed);
    EXPECT_TRUE(b.call(0).allowed);
    Decision d = b.call(0, false);
    EXPECT_FALSE(d.allowed);
    EXPECT_FALSE(d.write);
    EXPECT_EQ(d.retry_after_ms, 1000);
}

TEST(TokenBucket, TheStoredKeyLivesExactlyUntilTheBucketIsFullAgain) {
    Bucket b{P(10, "2")};
    Decision d = b.call(100);             // 9 tokens left; 1 token to go at 2/s = 500 ms
    ASSERT_TRUE(d.write);
    EXPECT_EQ(d.ttl_ms, 500);
    EXPECT_EQ(d.reset_after_ms, 500);
    d = b.call(100);
    EXPECT_EQ(d.ttl_ms, 1000);
}

TEST(TokenBucket, NoDriftOverManySmallSteps) {
    // 1 token per second, polled every millisecond for an hour of fake time: exactly one allowed
    // per 1000 ms after the initial burst, never one more, never one less.
    Bucket b{P(1, "1")};
    int allowed = 0;
    const int64_t hour = 3600 * 1000;
    for (int64_t t = 0; t <= hour; ++t)
        if (b.call(t).allowed) ++allowed;
    EXPECT_EQ(allowed, 3601);             // t = 0, 1000, ..., 3'600'000
}

TEST(TokenBucket, ChangingTheParametersTakesEffectImmediately) {
    Bucket b{P(10, "1")};
    for (int i = 0; i < 10; ++i) ASSERT_TRUE(b.call(0).allowed);
    b.p = P(3, "1");                      // capacity lowered: stored tokens are clamped, not kept
    b.st->tokens_ut = 8 * kUnit;
    int allowed = 0;
    while (b.call(0).allowed) ++allowed;
    EXPECT_EQ(allowed, 3);
}

// ---------------------------------------------------------------------------------------------
// Clock behaviour
// ---------------------------------------------------------------------------------------------

TEST(TokenBucketClock, ABackwardsClockRefillsNothingAndNeverMovesLastBackwards) {
    Bucket b{P(5, "1")};
    for (int i = 0; i < 5; ++i) ASSERT_TRUE(b.call(10'000).allowed);
    const int64_t last = b.st->last_ms;
    Decision d = b.call(9'000);           // the clock jumped back one second
    EXPECT_FALSE(d.allowed);
    EXPECT_EQ(b.st->last_ms, last);
    // time then runs forward again from the ORIGINAL last: the second that was "replayed" is not
    // credited twice
    EXPECT_FALSE(b.call(10'500).allowed);
    EXPECT_TRUE(b.call(11'000).allowed);
}

TEST(TokenBucketClock, AJitteringClockCannotGrantMoreThanTheLatestTimeItEverShowedAllows) {
    // The clock readings wobble by up to +-150 ms around real time. However they wobble, the tokens
    // granted by time T are at most capacity + rate * (latest reading seen - first reading).
    for (uint64_t seed = 1; seed <= 20; ++seed) {
        std::mt19937_64 rng(seed);
        const int64_t t0 = 1'000'000;
        Bucket b{P(20, "5")};
        int64_t t = t0, first = 0, latest = 0, granted = 0;
        for (int i = 0; i < 20000; ++i) {
            t += static_cast<int64_t>(rng() % 40);
            const int64_t reading = t + static_cast<int64_t>(rng() % 301) - 150;
            if (i == 0) first = latest = reading;
            latest = std::max(latest, reading);
            granted += b.call(reading).allowed;
            ASSERT_LE(granted * 1000, 20 * 1000 + 5 * (latest - first)) << "seed " << seed << " step " << i;
        }
    }
}

TEST(TokenBucketClock, AStoredTimeFarInTheFutureIsTreatedAsAClockFaultNotAFreeze) {
    // The state was written by a machine whose clock is hours ahead of ours (e.g. after a failover).
    Bucket b{P(5, "1")};
    for (int i = 0; i < 5; ++i) ASSERT_TRUE(b.call(0).allowed);
    b.st->last_ms = 3600LL * 1000;        // one hour ahead
    // refill restarts from our clock: nothing at t = 1 ms, and one token one second later
    EXPECT_FALSE(b.call(1).allowed);
    EXPECT_FALSE(b.call(1000).allowed);
    EXPECT_TRUE(b.call(1001).allowed);
}

TEST(TokenBucketClock, AStoredTimeSlightlyAheadJustPausesRefillUntilWeCatchUp) {
    Bucket b{P(5, "1")};
    for (int i = 0; i < 5; ++i) ASSERT_TRUE(b.call(1000).allowed);
    b.st->last_ms = 3000;                 // 2 s ahead, less than a full refill period (5 s)
    EXPECT_FALSE(b.call(2000).allowed);   // stricter: no refill while we are behind
    EXPECT_FALSE(b.call(3000).allowed);   // caught up, but nothing has accrued yet
    EXPECT_TRUE(b.call(4000).allowed);    // one second after the stored time
}

TEST(TokenBucketClock, AForwardJumpCanFillTheBucketButNotMore) {
    Bucket b{P(4, "1")};
    for (int i = 0; i < 4; ++i) ASSERT_TRUE(b.call(0).allowed);
    int allowed = 0;
    while (b.call(10LL * 24 * 3600 * 1000).allowed) ++allowed;   // clock leaps ten days
    EXPECT_EQ(allowed, 4);
}

// ---------------------------------------------------------------------------------------------
// The invariant, checked on random schedules
// ---------------------------------------------------------------------------------------------

TEST(TokenBucketProperty, TokensGrantedNeverExceedCapacityPlusWhatTimeAllows) {
    // For ANY schedule of calls (random costs, random gaps, random peeks): the tokens granted in
    // [t0, t] are at most capacity + rate * (t - t0). Integer arithmetic, so the check is exact.
    for (uint64_t seed = 1; seed <= 40; ++seed) {
        std::mt19937_64 rng(seed);
        const int64_t cap = 1 + static_cast<int64_t>(rng() % 50);
        const int64_t rate_milli = 1 + static_cast<int64_t>(rng() % 20000);
        Bucket b{Params{cap, rate_milli, 1}};
        const int64_t t0 = 1'700'000'000'000LL;
        int64_t t = t0, granted_ut = 0;
        for (int i = 0; i < 4000; ++i) {
            t += static_cast<int64_t>(rng() % 30);
            b.p.cost = 1 + static_cast<int64_t>(rng() % cap);
            const bool consume = (rng() % 4) != 0;
            Decision d = b.call(t, consume);
            if (d.allowed && consume) granted_ut += b.p.cost * kUnit;
            ASSERT_LE(granted_ut, cap * kUnit + (t - t0) * rate_milli) << "seed " << seed << " step " << i;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Encoding and argument parsing
// ---------------------------------------------------------------------------------------------

TEST(RateLimiterFormat, StateRoundTripsAndGarbageIsRejected) {
    State s{123456789, 1700000000123LL};
    auto back = decode(encode(s));
    ASSERT_TRUE(back);
    EXPECT_EQ(back->tokens_ut, s.tokens_ut);
    EXPECT_EQ(back->last_ms, s.last_ms);
    for (const char* bad : {"", "hello", "RL1", "RL1 5", "RL1 x y", "RL1 -5 10", "RL2 1 2", "RL1 1 2 3"})
        EXPECT_FALSE(decode(bad)) << bad;
}

TEST(RateLimiterFormat, ArgumentsAreValidatedWithClearErrors) {
    Params p;
    auto err = [&](std::vector<std::string> a) { return parse_params(a, &p); };
    EXPECT_EQ(err({"RL.ACQUIRE", "k", "10", "5"}), "");
    EXPECT_EQ(p.rate_milli, 5000);
    EXPECT_EQ(err({"RL.ACQUIRE", "k", "10", "0.25", "3"}), "");
    EXPECT_EQ(p.rate_milli, 250);
    EXPECT_EQ(p.cost, 3);
    for (auto bad : std::vector<std::vector<std::string>>{
             {"RL.ACQUIRE", "k"}, {"RL.ACQUIRE", "k", "10"}, {"RL.ACQUIRE", "k", "10", "1", "1", "1"},
             {"RL.ACQUIRE", "k", "0", "1"}, {"RL.ACQUIRE", "k", "-1", "1"}, {"RL.ACQUIRE", "k", "abc", "1"},
             {"RL.ACQUIRE", "k", "10", "0"}, {"RL.ACQUIRE", "k", "10", "-1"}, {"RL.ACQUIRE", "k", "10", "1.2345"},
             {"RL.ACQUIRE", "k", "10", "x"}, {"RL.ACQUIRE", "k", "10", "1", "0"}, {"RL.ACQUIRE", "k", "10", "1", "11"},
             {"RL.ACQUIRE", "k", "10000000000", "1"}, {"RL.ACQUIRE", "k", "10", "2000000"}})
        EXPECT_EQ(err(bad).substr(0, 4), "-ERR") << bad.size();
}

// ---------------------------------------------------------------------------------------------
// The commands against real engines, with a fake wall clock
// ---------------------------------------------------------------------------------------------

struct FakeWall {
    std::atomic<int64_t> t{1'000'000};
    FakeWall() { set_clock([this] { return t.load(); }); }
    ~FakeWall() { set_clock(nullptr); }
};

struct Reply {
    int64_t allowed = -9, remaining = -9, retry = -9, reset = -9;
    bool ok = false;
    std::string raw;
};

Reply rl(Engine& e, const std::vector<std::string>& args) {
    Reply r;
    r.raw = execute(e, args, nullptr);
    long long a, b, c, d;
    r.ok = std::sscanf(r.raw.c_str(), "*4\r\n:%lld\r\n:%lld\r\n:%lld\r\n:%lld\r\n", &a, &b, &c, &d) == 4;
    if (r.ok) { r.allowed = a; r.remaining = b; r.retry = c; r.reset = d; }
    return r;
}
Reply acquire(Engine& e, const std::string& key, const char* cap, const char* rate, const char* cost = nullptr) {
    std::vector<std::string> a = {"RL.ACQUIRE", key, cap, rate};
    if (cost) a.push_back(cost);
    return rl(e, a);
}

TEST(RateLimiterCommand, ReplyFormatAndBasicFlow) {
    FakeWall wall;
    ShardedStore store(4, [&] { return wall.t.load(); });
    Reply r = acquire(store, "api:user:1", "3", "1");
    ASSERT_TRUE(r.ok) << r.raw;
    EXPECT_EQ(r.raw, "*4\r\n:1\r\n:2\r\n:-1\r\n:1000\r\n");   // allowed, 2 left, no retry, full in 1 s
    EXPECT_TRUE(acquire(store, "api:user:1", "3", "1").allowed);
    EXPECT_TRUE(acquire(store, "api:user:1", "3", "1").allowed);
    r = acquire(store, "api:user:1", "3", "1");
    EXPECT_EQ(r.raw, "*4\r\n:0\r\n:0\r\n:1000\r\n:3000\r\n");
    wall.t += 1000;
    EXPECT_TRUE(acquire(store, "api:user:1", "3", "1").allowed);
    // keys are independent
    EXPECT_TRUE(acquire(store, "api:user:2", "3", "1").allowed);
    EXPECT_EQ(rl(store, {"RL.PEEK", "api:user:2", "3", "1"}).remaining, 2);
}

TEST(RateLimiterCommand, ErrorsAndWrongTypeLeaveTheStoreUntouched) {
    FakeWall wall;
    ShardedStore store(4, [&] { return wall.t.load(); });
    EXPECT_EQ(execute(store, {"RL.ACQUIRE", "k", "10"}, nullptr).substr(0, 4), "-ERR");
    EXPECT_EQ(execute(store, {"RL.ACQUIRE", "k", "10", "1", "11"}, nullptr), "-ERR cost exceeds capacity\r\n");
    EXPECT_EQ(store.size(), 0u);
    store.set("plain", "hello");
    EXPECT_EQ(execute(store, {"RL.ACQUIRE", "plain", "10", "1"}, nullptr).substr(0, 10), "-WRONGTYPE");
    EXPECT_EQ(*store.get("plain"), "hello");
}

TEST(RateLimiterCommand, DeniedCallsDoNotWriteAndAllowedOnesExpireWhenTheBucketIsFull) {
    FakeWall wall;
    ShardedStore store(4, [&] { return wall.t.load(); });
    ASSERT_TRUE(acquire(store, "k", "2", "1").allowed);                 // 1 left, full again in 1000 ms
    EXPECT_EQ(store.ttl_ms("k"), 1000);
    const std::string v = *store.get("k");
    ASSERT_TRUE(acquire(store, "k", "2", "1").allowed);
    EXPECT_EQ(store.ttl_ms("k"), 2000);
    ASSERT_FALSE(acquire(store, "k", "2", "1").allowed);
    EXPECT_EQ(store.ttl_ms("k"), 2000);                                  // a denial leaves state and TTL alone
    wall.t += 2001;                                                      // bucket full: the key is gone...
    EXPECT_EQ(store.ttl_ms("k"), -2);
    EXPECT_EQ(rl(store, {"RL.PEEK", "k", "2", "1"}).remaining, 2);       // ...and a missing bucket is a full one
    (void)v;
}

TEST(RateLimiterCommand, PeekDoesNotConsumeAndDoesNotCreateKeys) {
    FakeWall wall;
    ShardedStore store(4, [&] { return wall.t.load(); });
    for (int i = 0; i < 20; ++i) EXPECT_TRUE(rl(store, {"RL.PEEK", "k", "1", "1"}).allowed);
    EXPECT_EQ(store.size(), 0u);
    EXPECT_TRUE(acquire(store, "k", "1", "1").allowed);
    EXPECT_FALSE(rl(store, {"RL.PEEK", "k", "1", "1"}).allowed);
}

TEST(RateLimiterCommand, TheClockIsReadInsideTheCriticalSection) {
    // Whatever the clock does between requests, last_ms in the store never decreases.
    FakeWall wall;
    ShardedStore store(1, [&] { return wall.t.load(); });
    int64_t prev = 0;
    std::mt19937_64 rng(3);
    for (int i = 0; i < 500; ++i) {
        wall.t = 1'000'000 + static_cast<int64_t>(rng() % 5000);
        acquire(store, "k", "100", "1");
        auto st = decode(*store.get("k"));
        ASSERT_TRUE(st);
        ASSERT_GE(st->last_ms, prev);
        prev = st->last_ms;
    }
}

// ---------------------------------------------------------------------------------------------
// Concurrency: no race may let the callers collectively exceed the limit
// ---------------------------------------------------------------------------------------------

template <typename StoreT>
void hammer_one_key(StoreT& store, FakeWall& wall, int threads, int calls_per_thread, bool advance_time) {
    const int64_t start = wall.t.load();
    std::atomic<int64_t> allowed{0};
    std::atomic<bool> stop{false};
    std::thread ticker;
    if (advance_time)  // one fake millisecond per 20 us of real time, so the refill bound is small enough to bite
        ticker = std::thread([&] { while (!stop) { wall.t += 1; std::this_thread::sleep_for(std::chrono::microseconds(20)); } });
    std::vector<std::thread> ts;
    for (int i = 0; i < threads; ++i)
        ts.emplace_back([&] {
            for (int j = 0; j < calls_per_thread; ++j) allowed += acquire(store, "hot", "100", "50").allowed ? 1 : 0;
        });
    for (auto& t : ts) t.join();
    stop = true;
    if (ticker.joinable()) ticker.join();
    const int64_t elapsed = wall.t.load() - start;
    // capacity 100 + 50 tokens/s * elapsed seconds, computed in integers (milli-tokens)
    EXPECT_LE(allowed.load() * 1000, 100 * 1000 + 50 * elapsed) << "elapsed " << elapsed << " ms";
    if (!advance_time) EXPECT_EQ(allowed.load(), 100);   // frozen clock: EXACTLY the capacity, no more and no less
}

TEST(RateLimiterConcurrency, FrozenClockGrantsExactlyTheCapacityToSixteenThreads) {
    FakeWall wall;
    ShardedStore sharded(16, [&] { return wall.t.load(); });
    hammer_one_key(sharded, wall, 16, 2000, false);
    GlobalLockStore global([&] { return wall.t.load(); });
    hammer_one_key(global, wall, 16, 2000, false);
}

TEST(RateLimiterConcurrency, RunningClockNeverGrantsMoreThanCapacityPlusRefill) {
    for (int round = 0; round < 5; ++round) {
        FakeWall wall;
        ShardedStore sharded(16, [&] { return wall.t.load(); });
        hammer_one_key(sharded, wall, 8, 20000, true);
    }
}

TEST(RateLimiterConcurrency, ManyKeysAreIndependentAndEachExact) {
    FakeWall wall;
    ShardedStore store(16, [&] { return wall.t.load(); });
    constexpr int kKeys = 64, kThreads = 8, kCap = 10;
    std::vector<std::atomic<int>> allowed(kKeys);
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t)
        ts.emplace_back([&, t] {
            for (int i = 0; i < 400; ++i) {
                const int k = (i * 7 + t) % kKeys;
                if (acquire(store, "k" + std::to_string(k), "10", "1").allowed) allowed[k]++;
            }
        });
    for (auto& t : ts) t.join();
    for (int k = 0; k < kKeys; ++k) EXPECT_EQ(allowed[k].load(), kCap) << k;
}

}  // namespace
