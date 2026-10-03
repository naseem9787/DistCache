#pragma once
// The storage engine: an in-memory hash map from key to value, with optional TTL.
//
// Expiration uses BOTH strategies:
//   * Lazy:   every access (get/del/ttl/expire) first checks the key's deadline
//             and deletes it if it has passed. Costs nothing for idle keys, but a
//             key nobody touches again would sit in memory forever...
//   * Active: ...so sweep() is called regularly by the server loop. It pops keys
//             whose deadline has passed from an index sorted by deadline, so it only
//             ever touches keys that are actually expired (O(k log n) for k expired).
//
// Time is injected (Clock) so tests can move it forward without sleeping.
// Phase 1-2 are single-threaded (one epoll loop), so no locking yet (Phase 4).
#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

class Store {
public:
    using Clock = std::function<int64_t()>;  // milliseconds, monotonic

    explicit Store(Clock clock = real_now_ms) : clock_(std::move(clock)) {}

    static int64_t real_now_ms() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    }

    // Stores key=value. No ttl_ms means "never expires" and clears any old TTL,
    // like Redis: overwriting a key with a plain SET removes its expiry.
    void set(const std::string& key, std::string value, std::optional<int64_t> ttl_ms = std::nullopt) {
        auto it = map_.find(key);
        if (it != map_.end()) drop_expiry(key, it->second);
        Entry& e = map_[key];
        e.value = std::move(value);
        e.expire_at = -1;
        if (ttl_ms) set_deadline(key, e, now() + *ttl_ms);
    }

    std::optional<std::string> get(const std::string& key) {
        auto it = live(key);
        if (it == map_.end()) return std::nullopt;
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
            map_.erase(expiries_.begin()->second);
            expiries_.erase(expiries_.begin());
            ++removed;
        }
        return removed;
    }

    // Counts keys physically stored, including expired ones not yet swept (same as Redis DBSIZE).
    size_t size() const { return map_.size(); }

private:
    struct Entry {
        std::string value;
        int64_t expire_at = -1;  // absolute ms on the clock; -1 = never
    };
    using Map = std::unordered_map<std::string, Entry>;

    int64_t now() const { return clock_(); }

    // Lazy expiration lives here: find the key, delete it if its deadline passed.
    Map::iterator live(const std::string& key) {
        auto it = map_.find(key);
        if (it != map_.end() && it->second.expire_at >= 0 && it->second.expire_at <= now()) {
            erase(it);
            return map_.end();
        }
        return it;
    }

    void erase(Map::iterator it) {
        drop_expiry(it->first, it->second);
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
    Map map_;
    std::set<std::pair<int64_t, std::string>> expiries_;  // (deadline, key), soonest first
};
