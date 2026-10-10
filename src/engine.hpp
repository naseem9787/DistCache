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
#include <functional>
#include <optional>
#include <string>

// Receives every *logical* mutation as it is applied, for the write-ahead log.
//
// CONTRACT: callbacks run INSIDE the engine's critical section (shard lock held), right
// after the change is applied. That is what makes the log order for any one key equal to
// the order in which the engine applied it. Consequences for implementors:
//   * never call back into the engine (it would self-deadlock),
//   * keep it short (it delays every other operation on that shard),
//   * lock order is always engine lock -> observer lock, never the reverse.
// Expirations are NOT reported (the logged deadline is absolute, so replay reproduces them).
// Evictions ARE reported, as on_del, so a recovered store matches what the live one held.
class MutationObserver {
public:
    virtual ~MutationObserver() = default;
    virtual void on_set(const std::string& key, const std::string& value, std::optional<int64_t> ttl_ms) = 0;
    virtual void on_del(const std::string& key) = 0;
    virtual void on_expire(const std::string& key, int64_t ttl_ms) = 0;  // ttl_ms > 0
    virtual void on_persist(const std::string& key) = 0;
};

// (key, value, remaining ttl in ms or -1 for none)
using EntryVisitor = std::function<void(const std::string&, const std::string&, int64_t)>;

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

    // Persistence hooks. set_observer must be called before the engine is shared between
    // threads. for_each visits every live (unexpired) entry; on locked engines it copies a
    // shard's entries under the lock and calls the visitor AFTER releasing it, so the visitor
    // may do slow work (file I/O). It is not a point-in-time snapshot across shards.
    virtual void set_observer(MutationObserver* observer) = 0;
    virtual void for_each(const EntryVisitor& visit) const = 0;

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
