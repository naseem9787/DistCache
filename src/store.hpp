#pragma once
// The storage engine: a plain in-memory hash map from key to value.
// Phase 1 is single-threaded (one epoll loop), so no locking yet.
// Later phases add TTL (Phase 2), LRU eviction (Phase 3) and locks (Phase 4).
#include <optional>
#include <string>
#include <unordered_map>

class Store {
public:
    void set(const std::string& key, std::string value) { map_[key] = std::move(value); }

    std::optional<std::string> get(const std::string& key) const {
        auto it = map_.find(key);
        if (it == map_.end()) return std::nullopt;
        return it->second;
    }

    // Returns true if the key existed.
    bool del(const std::string& key) { return map_.erase(key) > 0; }

    size_t size() const { return map_.size(); }

private:
    std::unordered_map<std::string, std::string> map_;
};
