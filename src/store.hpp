#pragma once
// The storage engine: an in-memory hash map with TTL expiry and LRU eviction.
// NOT thread-safe by itself: wrap it in GlobalLockStore / ShardedStore (synced_store.hpp)
// to share it between threads.
//
// Expiration (Phase 2) uses BOTH strategies:
//   * Lazy:   every access first checks the key's deadline and deletes it if passed.
//   * Active: sweep() is called regularly by the server. It pops keys whose deadline
//             has passed from an index sorted by deadline, so it only ever touches
//             keys that are actually expired (O(k log n) for k expired).
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
// THREE structures describe one key (map, LRU list, deadline index). They must change
// together, which is why every removal goes through erase(). That also defines the
// critical section for concurrency: one Store operation must run under ONE lock, start
// to finish, or another thread could observe the structures half-updated.
//
// Time is injected (Clock) so tests can move it forward without sleeping.
#include <chrono>
#include <cstdint>
#include <functional>
#include <list>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include "engine.hpp"

class Store final : public Engine {
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
    void set(const std::string& key, std::string value,
             std::optional<int64_t> ttl_ms = std::nullopt) override {
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
        if (observer_) observer_->on_set(key, e.value, ttl_ms);
    }

    std::optional<std::string> get(const std::string& key) override {
        auto it = live(key);
        if (it == map_.end()) return std::nullopt;
        touch(it);  // a read counts as a "use"
        return it->second.value;
    }

    // Returns true if the key existed (and wasn't already expired).
    bool del(const std::string& key) override {
        auto it = live(key);
        if (it == map_.end()) return false;
        erase(it);
        if (observer_) observer_->on_del(key);
        return true;
    }

    // Remaining life in ms. -2 = no such key, -1 = key has no expiry.
    int64_t ttl_ms(const std::string& key) override {
        auto it = live(key);
        if (it == map_.end()) return -2;
        if (it->second.expire_at < 0) return -1;
        return it->second.expire_at - now();
    }

    // Sets/replaces a TTL on an existing key. A TTL <= 0 deletes the key at once.
    // Returns false if the key doesn't exist.
    bool expire(const std::string& key, int64_t ttl_ms) override {
        auto it = live(key);
        if (it == map_.end()) return false;
        if (ttl_ms <= 0) {
            erase(it);
            if (observer_) observer_->on_del(key);
            return true;
        }
        drop_expiry(key, it->second);
        set_deadline(key, it->second, now() + ttl_ms);
        if (observer_) observer_->on_expire(key, ttl_ms);
        return true;
    }

    // Removes a TTL. Returns true if the key existed and had one.
    bool persist(const std::string& key) override {
        auto it = live(key);
        if (it == map_.end() || it->second.expire_at < 0) return false;
        drop_expiry(key, it->second);
        it->second.expire_at = -1;
        if (observer_) observer_->on_persist(key);
        return true;
    }

    // Active expiration: delete up to `limit` expired keys. Returns how many.
    // `limit` bounds the work per call so a mass expiry can't stall the event loop.
    size_t sweep(size_t limit = 1000) override {
        size_t removed = 0;
        const int64_t t = now();
        while (removed < limit && !expiries_.empty() && expiries_.begin()->first <= t) {
            erase(map_.find(expiries_.begin()->second));
            ++expired_;
            ++removed;
        }
        return removed;
    }

    void set_observer(MutationObserver* observer) override { observer_ = observer; }

    void for_each(const EntryVisitor& visit) const override {
        const int64_t t = now();
        for (const auto& [key, e] : map_) {
            if (e.expire_at >= 0 && e.expire_at <= t) continue;  // expired, just not swept yet
            visit(key, e.value, e.expire_at < 0 ? -1 : e.expire_at - t);
        }
    }

    // Counts keys physically stored, including expired ones not yet swept (same as Redis DBSIZE).
    size_t size() const override { return map_.size(); }
    size_t max_keys() const override { return max_keys_; }
    uint64_t evicted_keys() const override { return evicted_; }
    uint64_t expired_keys() const override { return expired_; }
    const char* mode() const override { return "single"; }
    size_t shards() const override { return 1; }

    bool consistent() const override {
        if (lru_.size() != map_.size()) return false;
        size_t with_ttl = 0;
        for (const auto& [key, e] : map_) {
            if (*e.lru_pos != &key) return false;  // list node must point back at this entry
            if (e.expire_at >= 0) {
                ++with_ttl;
                if (!expiries_.count({e.expire_at, key})) return false;
            }
        }
        if (expiries_.size() != with_ttl) return false;
        return max_keys_ == 0 || map_.size() <= max_keys_;
    }

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
            const std::string victim = *lru_.back();  // least recently used (copy: erase frees it)
            erase(map_.find(victim));
            ++evicted_;
            if (observer_) observer_->on_del(victim);  // logged, so replay reproduces the eviction
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
    MutationObserver* observer_ = nullptr;
};
