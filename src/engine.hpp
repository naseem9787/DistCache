#pragma once
// The interface every storage backend implements. Command handling and the server
// only know about Engine, so the concurrency strategy is chosen at startup:
//
//   Store            - no locking at all (Phase 1-3 engine). Only safe from ONE thread.
//   GlobalLockStore  - one mutex around a Store. Safe from many threads, but serial.
//   ShardedStore     - N independent Stores, each with its own mutex.
//
// Thread-safety contract for every implementation except Store: any method may be
// called from any thread at any time. Methods return values BY VALUE (never
// references into the engine) so nothing dangles after the lock is released.
#include <cstdint>
#include <optional>
#include <string>

class Engine {
public:
    virtual ~Engine() = default;

    virtual void set(const std::string& key, std::string value,
                     std::optional<int64_t> ttl_ms = std::nullopt) = 0;
    // NOT a pure read: it refreshes LRU recency and may delete an expired key.
    virtual std::optional<std::string> get(const std::string& key) = 0;
    virtual bool del(const std::string& key) = 0;
    virtual int64_t ttl_ms(const std::string& key) = 0;   // -2 missing, -1 no expiry
    virtual bool expire(const std::string& key, int64_t ttl_ms) = 0;
    virtual bool persist(const std::string& key) = 0;
    virtual size_t sweep(size_t limit = 1000) = 0;        // active expiration

    // Diagnostics. On sharded engines these are sums taken shard by shard, so under
    // concurrent writes they are NOT an atomic snapshot (each is exact per shard).
    virtual size_t size() const = 0;
    virtual size_t max_keys() const = 0;                  // effective capacity, 0 = unlimited
    virtual uint64_t evicted_keys() const = 0;
    virtual uint64_t expired_keys() const = 0;
    virtual const char* mode() const = 0;                 // "single" | "global" | "sharded"
    virtual size_t shards() const = 0;

    // Verifies that the hash map, LRU list and deadline index agree with each other.
    // O(n); meant for tests and debugging, not the request path.
    virtual bool consistent() const = 0;
};
