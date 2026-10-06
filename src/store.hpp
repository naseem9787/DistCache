#pragma once
// The storage engine: an in-memory hash map with TTL expiry and LRU eviction.
//
// Expiration (Phase 2) uses BOTH strategies:
//   * Lazy:   every access first checks the key's deadline and deletes it if passed.
//   * Active: sweep() is called regularly by the server loop. It pops keys whose
//             deadline has passed from an index sorted by deadline, so it only ever
//             touches keys that are actually expired (O(k log n) for k expired).
//
// Eviction (Phase 3): with a capacity limit, inserting a NEW key into a full store
// first evicts the Least Recently Used key. Recency is a doubly linked list
// (front = most recent, back = least recent) and every map entry remembers its list
// position, so "move to front" and "drop the back" are both O(1):
//
//      unordered_map<key, {value, expire_at, ---+}>        list (most recent first)
//                                                |         [ k3 ] <-> [ k1 ] <-> [ k2 ]
//                                                +------->     ^ iterator points here
//
// The list stores POINTERS to the map's own key strings (unordered_map nodes never
// move, even on rehash), so keys aren't duplicated in memory.
//
// Time is injected (Clock) so tests can move it forward without sleeping.
// Single-threaded for now (one epoll loop); locking arrives in Phase 4.
#include <chrono>
#include <cstdint>
#include <functional>
#include <list>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

class Store {
public:
    using Clock = std::function<int64_t()>;  // milliseconds, monotonic

    // max_keys == 0 means unlimited (no eviction).
    explicit Store(Clock clock = real_now_ms, size_t max_keys = 0)
        : clock_(std::move(clock)), max_keys_(max_keys) {}

    static int64_t real_now_ms() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    }

    // Stores key=value and marks it most recently used. No ttl_ms means "never expires"
    // and clears any old TTL (like Redis). A new key in a full store evicts the LRU key.
    void set(const std::string& key, std::string value, std::optional<int64_t> ttl_ms = std::nullopt) {
        auto it = map_.find(key);
        if (it != map_.end()) {
            drop_expiry(key, it->second);
            touch(it);
        } else {
            make_room();
            it = map_.emplace(key, Entry{}).first;
            lru_.push_front(&it->first);
            it->second.lru_pos = lru_.begin();
        }
        Entry& e = it->second;
        e.value = std::move(value);
        e.expire_at = -1;
        if (ttl_ms) set_deadline(key, e, now() + *ttl_ms);
    }

    std::optional<std::string> get(const std::string& key) {
        auto it = live(key);
        if (it == map_.end()) return std::nullopt;
        touch(it);  // a read counts as a "use"
        return it->second.value;
    }

    // Returns true if the key existed (and wasn't already expired).
    bool del(const std::string& key) {
        auto it = live(key);
        if (it == map_.end()) return false;
        erase(it);
        return true;
    }

    // Remaining life in ms. -2 = no such key, -1 = key has no expiry.
    int64_t ttl_ms(const std::string& key) {
        auto it = live(key);
        if (it == map_.end()) return -2;
        if (it->second.expire_at < 0) return -1;
        return it->second.expire_at - now();
    }

    // Sets/replaces a TTL on an existing key. A TTL <= 0 deletes the key at once.
    // Returns false if the key doesn't exist.
    bool expire(const std::string& key, int64_t ttl_ms) {
        auto it = live(key);
        if (it == map_.end()) return false;
        if (ttl_ms <= 0) { erase(it); return true; }
        drop_expiry(key, it->second);
        set_deadline(key, it->second, now() + ttl_ms);
        return true;
    }

    // Removes a TTL. Returns true if the key existed and had one.
    bool persist(const std::string& key) {
        auto it = live(key);
        if (it == map_.end() || it->second.expire_at < 0) return false;
        drop_expiry(key, it->second);
        it->second.expire_at = -1;
        return true;
    }

    // Active expiration: delete up to `limit` expired keys. Returns how many.
    // `limit` bounds the work per call so a mass expiry can't stall the event loop.
    size_t sweep(size_t limit = 1000) {
        size_t removed = 0;
        const int64_t t = now();
        while (removed < limit && !expiries_.empty() && expiries_.begin()->first <= t) {
            erase(map_.find(expiries_.begin()->second));
            ++expired_;
            ++removed;
        }
        return removed;
    }

    // Counts keys physically stored, including expired ones not yet swept (same as Redis DBSIZE).
    size_t size() const { return map_.size(); }
    size_t max_keys() const { return max_keys_; }
    uint64_t evicted_keys() const { return evicted_; }
    uint64_t expired_keys() const { return expired_; }

private:
    using LruList = std::list<const std::string*>;
    struct Entry {
        std::string value;
        int64_t expire_at = -1;  // absolute ms on the clock; -1 = never
        LruList::iterator lru_pos;
    };
    using Map = std::unordered_map<std::string, Entry>;

    int64_t now() const { return clock_(); }

    // Mark as most recently used: relink the node to the front. O(1), iterators stay valid.
    void touch(Map::iterator it) { lru_.splice(lru_.begin(), lru_, it->second.lru_pos); }

    // Called before inserting a new key. Expired keys are free to remove, so reclaim
    // those first; only evict live keys if the store is still full.
    void make_room() {
        if (max_keys_ == 0 || map_.size() < max_keys_) return;
        sweep(16);
        while (map_.size() >= max_keys_ && !lru_.empty()) {
            erase(map_.find(*lru_.back()));  // least recently used
            ++evicted_;
        }
    }

    // Lazy expiration lives here: find the key, delete it if its deadline passed.
    Map::iterator live(const std::string& key) {
        auto it = map_.find(key);
        if (it != map_.end() && it->second.expire_at >= 0 && it->second.expire_at <= now()) {
            erase(it);
            ++expired_;
            return map_.end();
        }
        return it;
    }

    // The one place a key leaves the store: keeps map, LRU list and deadline index in sync.
    void erase(Map::iterator it) {
        drop_expiry(it->first, it->second);
        lru_.erase(it->second.lru_pos);
        map_.erase(it);
    }

    void set_deadline(const std::string& key, Entry& e, int64_t at) {
        e.expire_at = at;
        expiries_.emplace(at, key);
    }

    void drop_expiry(const std::string& key, const Entry& e) {
        if (e.expire_at >= 0) expiries_.erase({e.expire_at, key});
    }

    Clock clock_;
    size_t max_keys_;
    Map map_;
    LruList lru_;                                          // front = most recently used
    std::set<std::pair<int64_t, std::string>> expiries_;  // (deadline, key), soonest first
    uint64_t evicted_ = 0;
    uint64_t expired_ = 0;
};
