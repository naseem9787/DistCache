#pragma once
// Distributed rate limiter (Phase 9): a token bucket whose state is an ordinary DistCache key.
//
//   RL.ACQUIRE key capacity rate [cost]   take `cost` tokens if available   (a write)
//   RL.PEEK    key capacity rate [cost]   same answer, nothing consumed     (a read)
//
// Reply (RESP array of four integers):
//   [ allowed (1|0), remaining whole tokens, retry_after_ms (-1 if allowed or never), reset_after_ms ]
//
// THE ALGORITHM (all integer arithmetic, no floating point):
//   * Tokens are counted in micro-tokens (1 token = 1,000,000). The refill rate is given in tokens
//     per second with up to three decimals, which is *exactly* that many micro-tokens per
//     millisecond (rate_milli = tokens/s * 1000), so refill is exact: no rounding drift.
//   * state = (tokens, last_ms). On every call: tokens = min(capacity, tokens + elapsed_ms * rate),
//     where elapsed = now - last_ms, and last_ms moves to now only when time moved forward.
//   * allowed iff tokens >= cost. Allowed: the cost is subtracted and the state stored. Denied:
//     NOTHING is stored (the next call recomputes the same refill from the same stored state).
//   * A missing key is a full bucket. The stored key carries a TTL equal to the time the bucket
//     needs to become full again, after which "missing" and "stored" are the same thing: idle
//     buckets clean themselves up and cost no memory.
//
// WHY THE STATE IS A NORMAL KEY: the bucket then rides on everything the cache already has. The
// read-modify-write runs under the key's shard lock (Engine::update), so concurrent callers are
// serialised per key; the result is logged through the same MutationObserver as SET, so the WAL,
// replication (async or sync N), full resync, failover and Phase 7 migration all carry it with no
// new protocol. The record the replicas receive is the resulting state ("SET key RL1... PXAT t"),
// not the command, so applying it twice, or out of a fuzzy snapshot, is harmless.
//
// CLOCK: the state holds wall-clock milliseconds, because after a failover another machine's
// clock reads it. Behaviour (tested): a clock that goes BACKWARDS refills nothing and never moves
// last_ms back (stricter, never more permissive); if the stored time is more than one full refill
// period in the future it is treated as a clock fault and refill resumes from now; a clock that
// jumps FORWARD can at most fill the bucket (over-admission bounded by one capacity).
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "engine.hpp"

namespace ratelimit {

constexpr int64_t kUnit = 1'000'000;                 // micro-tokens per token
constexpr int64_t kMaxCapacity = 1'000'000'000;      // tokens
constexpr int64_t kMaxRateMilli = 1'000'000'000;     // 1,000,000 tokens per second

struct Params {
    int64_t capacity = 0;    // tokens, >= 1
    int64_t rate_milli = 0;  // milli-tokens per second == micro-tokens per millisecond, >= 1
    int64_t cost = 1;        // tokens, 1..capacity
};

struct State {
    int64_t tokens_ut = 0;   // micro-tokens
    int64_t last_ms = 0;     // wall clock, ms
};

struct Decision {
    bool allowed = false;
    int64_t remaining = 0;       // whole tokens left
    int64_t retry_after_ms = -1; // ms until `cost` tokens are available; -1 when allowed
    int64_t reset_after_ms = 0;  // ms until the bucket is full
    bool write = false;          // true: `state` must be stored with `ttl_ms`
    State state;
    int64_t ttl_ms = 0;
};

// The whole algorithm, as a pure function: current stored state (nullopt = none), parameters, the
// clock reading and whether to consume. Deterministic, so every behaviour is unit-testable.
Decision decide(const std::optional<State>& current, const Params& p, int64_t now_ms, bool consume);

std::string encode(const State& s);                  // "RL1 <tokens_ut> <last_ms>"
std::optional<State> decode(const std::string& v);   // nullopt if the value is not a bucket

// Argument parsing shared by both commands: args = {CMD, key, capacity, rate, [cost]}.
// Returns "" and fills `p`, or a RESP error reply.
std::string parse_params(const std::vector<std::string>& args, Params* p);

// The wall clock the limiter reads (ms since the epoch). Tests replace it.
using Clock = std::function<int64_t()>;
int64_t system_wall_ms();
void set_clock(Clock clock);   // nullptr restores the system clock. Not thread-safe: call before serving.
int64_t now_ms();

// Executes RL.ACQUIRE (consume = true) or RL.PEEK against an engine; returns the RESP reply.
// The clock is read INSIDE the key's critical section, so clock readings are ordered like the
// updates they belong to.
std::string run(Engine& engine, const std::vector<std::string>& args, bool consume);

struct Counters { uint64_t allowed; uint64_t denied; uint64_t errors; };
Counters counters();

}  // namespace ratelimit
