#pragma once
// Fan-out for MutationObserver. An engine has ONE observer slot; with persistence AND
// replication enabled both need every mutation, so the slot holds a chain.
//
// The chain calls its members in the order they were added, on the calling thread, inside the
// engine's critical section (that is the MutationObserver contract, see engine.hpp). Members
// must therefore be short, must not call back into the engine, and must follow the global lock
// order  engine lock -> WAL buffer lock -> replication-log lock.
//
// Ordering guarantee this gives (and replication relies on): by the time any thread can SEE a
// mutation (it needs the engine lock, which is only released after the whole chain ran), the
// mutation has a position in every member's log. Members cannot fail the mutation (the
// callbacks return void and are infallible by construction: the WAL aborts the process on an
// I/O error, the replication log trims instead of failing).
#include <vector>

#include "engine.hpp"

class ObserverChain : public MutationObserver {
public:
    // Not thread-safe: build the chain before handing the engine to other threads.
    void add(MutationObserver* o) { if (o) members_.push_back(o); }
    bool empty() const { return members_.empty(); }

    void on_set(const std::string& key, const std::string& value, std::optional<int64_t> ttl_ms) override {
        for (auto* m : members_) m->on_set(key, value, ttl_ms);
    }
    void on_del(const std::string& key) override {
        for (auto* m : members_) m->on_del(key);
    }
    void on_expire(const std::string& key, int64_t ttl_ms) override {
        for (auto* m : members_) m->on_expire(key, ttl_ms);
    }
    void on_persist(const std::string& key) override {
        for (auto* m : members_) m->on_persist(key);
    }
    void on_flush() override {
        for (auto* m : members_) m->on_flush();
    }

private:
    std::vector<MutationObserver*> members_;
};
