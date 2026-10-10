#pragma once
// Two thread-safe wrappers around Store. See README "Phase 4" for the full discussion.
//
// WHY A PLAIN MUTEX AND NOT shared_mutex (reader/writer lock)?
//   get() is not a read: it moves the key to the front of the LRU list and may delete an
//   expired key. Two concurrent get()s would both modify the list, so every operation
//   must take the lock exclusively. A shared_mutex would only add overhead here.
//
// LOCKING RULES (these are what rule out deadlock):
//   * Every method takes exactly ONE lock, does all its work, and releases it.
//   * No method ever holds one lock while acquiring another, and no lock is held while
//     calling out to code that could take a lock. With no nesting there is no cycle.
//   * Return values are copied out before the lock is released.
#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>

#include "store.hpp"

// ---------------------------------------------------------------------------
// One mutex for the whole keyspace. Simple and strictly correct, including a
// true global LRU order and atomic stats. Throughput stops scaling with threads
// because every operation, on any key, waits for the same lock.
// ---------------------------------------------------------------------------
class GlobalLockStore final : public Engine {
public:
    explicit GlobalLockStore(Store::Clock clock = Store::real_now_ms, size_t max_keys = 0)
        : store_(std::move(clock), max_keys) {}

    void set(const std::string& key, std::string value, std::optional<int64_t> ttl_ms = std::nullopt) override {
        std::lock_guard<std::mutex> g(mu_);
        store_.set(key, std::move(value), ttl_ms);
    }
    std::optional<std::string> get(const std::string& key) override {
        std::lock_guard<std::mutex> g(mu_);
        return store_.get(key);
    }
    bool del(const std::string& key) override {
        std::lock_guard<std::mutex> g(mu_);
        return store_.del(key);
    }
    int64_t ttl_ms(const std::string& key) override {
        std::lock_guard<std::mutex> g(mu_);
        return store_.ttl_ms(key);
    }
    bool expire(const std::string& key, int64_t ttl_ms) override {
        std::lock_guard<std::mutex> g(mu_);
        return store_.expire(key, ttl_ms);
    }
    bool persist(const std::string& key) override {
        std::lock_guard<std::mutex> g(mu_);
        return store_.persist(key);
    }
    size_t sweep(size_t limit = 1000) override {
        std::lock_guard<std::mutex> g(mu_);
        return store_.sweep(limit);
    }

    void set_observer(MutationObserver* o) override { store_.set_observer(o); }
    void for_each(const EntryVisitor& visit) const override {
        std::vector<std::tuple<std::string, std::string, int64_t>> copy;  // copy under the lock...
        {
            std::lock_guard<std::mutex> g(mu_);
            copy.reserve(store_.size());
            store_.for_each([&](const std::string& k, const std::string& v, int64_t t) { copy.emplace_back(k, v, t); });
        }
        for (const auto& [k, v, t] : copy) visit(k, v, t);  // ...visit after releasing it
    }

    size_t size() const override { std::lock_guard<std::mutex> g(mu_); return store_.size(); }
    size_t max_keys() const override { std::lock_guard<std::mutex> g(mu_); return store_.max_keys(); }
    uint64_t evicted_keys() const override { std::lock_guard<std::mutex> g(mu_); return store_.evicted_keys(); }
    uint64_t expired_keys() const override { std::lock_guard<std::mutex> g(mu_); return store_.expired_keys(); }
    const char* mode() const override { return "global"; }
    size_t shards() const override { return 1; }
    bool consistent() const override { std::lock_guard<std::mutex> g(mu_); return store_.consistent(); }

private:
    mutable std::mutex mu_;
    Store store_;
};

// ---------------------------------------------------------------------------
// The keyspace is split into N shards by hash(key) % N. Each shard is a complete
// Store (own map, LRU list, deadline index, counters) behind its own mutex, so
// operations on keys in different shards run in parallel.
//
// What this changes, honestly:
//   * Single-key operations (SET/GET/DEL/TTL/EXPIRE/PERSIST) touch exactly one shard,
//     so one shard lock is enough. The three internal structures of a key all live in
//     that shard, so they can never be seen half-updated.
//   * LRU is PER SHARD, not global. Capacity is divided evenly (ceil(max_keys/N) per
//     shard) and each shard evicts its own least recently used key. A hot shard can
//     evict a key that is more recent than a cold shard's oldest. This is "approximate
//     LRU": a deliberate trade for not having a global list that everything contends on.
//     Strict global LRU would need a lock on one shared list for every GET, which is
//     exactly the bottleneck sharding removes.
//   * sweep() visits shards one at a time, locking each only while it works on it.
//     It starts at a rotating shard so a busy first shard can't eat the whole budget.
//   * size()/INFO counters are sums over shards, locking one shard at a time. Each
//     term is exact, but the total is not an atomic snapshot under concurrent writes.
//   * Multi-key commands (DEL a b c) are executed key by key. They are NOT atomic as a
//     group, matching Redis Cluster semantics.
// ---------------------------------------------------------------------------
class ShardedStore final : public Engine {
public:
    explicit ShardedStore(size_t num_shards = 16, Store::Clock clock = Store::real_now_ms,
                          size_t max_keys = 0) {
        size_t n = std::max<size_t>(1, num_shards);
        if (max_keys > 0) n = std::min(n, max_keys);  // never a shard with capacity 0
        const size_t per_shard = max_keys == 0 ? 0 : (max_keys + n - 1) / n;
        effective_max_ = per_shard * n;
        shards_.reserve(n);
        for (size_t i = 0; i < n; ++i) shards_.push_back(std::make_unique<Shard>(clock, per_shard));
    }

    void set(const std::string& key, std::string value, std::optional<int64_t> ttl_ms = std::nullopt) override {
        Shard& s = shard_for(key);
        std::lock_guard<std::mutex> g(s.mu);
        s.store.set(key, std::move(value), ttl_ms);
    }
    std::optional<std::string> get(const std::string& key) override {
        Shard& s = shard_for(key);
        std::lock_guard<std::mutex> g(s.mu);
        return s.store.get(key);
    }
    bool del(const std::string& key) override {
        Shard& s = shard_for(key);
        std::lock_guard<std::mutex> g(s.mu);
        return s.store.del(key);
    }
    int64_t ttl_ms(const std::string& key) override {
        Shard& s = shard_for(key);
        std::lock_guard<std::mutex> g(s.mu);
        return s.store.ttl_ms(key);
    }
    bool expire(const std::string& key, int64_t ttl_ms) override {
        Shard& s = shard_for(key);
        std::lock_guard<std::mutex> g(s.mu);
        return s.store.expire(key, ttl_ms);
    }
    bool persist(const std::string& key) override {
        Shard& s = shard_for(key);
        std::lock_guard<std::mutex> g(s.mu);
        return s.store.persist(key);
    }

    size_t sweep(size_t limit = 1000) override {
        const size_t n = shards_.size();
        const size_t start = sweep_cursor_.fetch_add(1, std::memory_order_relaxed);
        size_t removed = 0;
        for (size_t i = 0; i < n && removed < limit; ++i) {
            Shard& s = *shards_[(start + i) % n];
            std::lock_guard<std::mutex> g(s.mu);  // one shard lock at a time, never nested
            removed += s.store.sweep(limit - removed);
        }
        return removed;
    }

    void set_observer(MutationObserver* o) override {
        for (auto& sh : shards_) sh->store.set_observer(o);  // every shard reports to the same observer
    }
    void for_each(const EntryVisitor& visit) const override {
        for (const auto& sh : shards_) {  // one shard at a time: bounded copy, bounded lock hold
            std::vector<std::tuple<std::string, std::string, int64_t>> copy;
            {
                std::lock_guard<std::mutex> g(sh->mu);
                copy.reserve(sh->store.size());
                sh->store.for_each([&](const std::string& k, const std::string& v, int64_t t) { copy.emplace_back(k, v, t); });
            }
            for (const auto& [k, v, t] : copy) visit(k, v, t);
        }
    }

    size_t size() const override { return sum([](const Store& s) { return s.size(); }); }
    size_t max_keys() const override { return effective_max_; }
    uint64_t evicted_keys() const override { return sum([](const Store& s) { return s.evicted_keys(); }); }
    uint64_t expired_keys() const override { return sum([](const Store& s) { return s.expired_keys(); }); }
    const char* mode() const override { return "sharded"; }
    size_t shards() const override { return shards_.size(); }
    bool consistent() const override {
        for (const auto& sh : shards_) {
            std::lock_guard<std::mutex> g(sh->mu);
            if (!sh->store.consistent()) return false;
        }
        return true;
    }

private:
    // alignas(64): each shard starts on its own cache line, so two threads locking
    // neighbouring shards don't fight over the same line (false sharing).
    struct alignas(64) Shard {
        Shard(Store::Clock clock, size_t cap) : store(std::move(clock), cap) {}
        mutable std::mutex mu;
        Store store;
    };

    Shard& shard_for(const std::string& key) { return *shards_[std::hash<std::string>{}(key) % shards_.size()]; }

    template <typename F>
    uint64_t sum(F f) const {
        uint64_t total = 0;
        for (const auto& sh : shards_) {
            std::lock_guard<std::mutex> g(sh->mu);
            total += f(sh->store);
        }
        return total;
    }

    std::vector<std::unique_ptr<Shard>> shards_;
    size_t effective_max_ = 0;
    std::atomic<size_t> sweep_cursor_{0};
};
