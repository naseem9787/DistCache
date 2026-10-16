#include "repl_log.hpp"

#include <chrono>
#include <cstdio>
#include <random>

namespace {

int64_t system_wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

void put_bulk(std::string& out, const std::string& s) {
    out += '$';
    out += std::to_string(s.size());
    out += "\r\n";
    out += s;
    out += "\r\n";
}

void put_header(std::string& out, size_t argc) {
    out += '*';
    out += std::to_string(argc);
    out += "\r\n";
}

}  // namespace

ReplicationLog::ReplicationLog() : ReplicationLog(Options{}) {}

ReplicationLog::ReplicationLog(Options opts) : opts_(std::move(opts)), replid_(new_replid()) {
    if (!opts_.wall_ms) opts_.wall_ms = system_wall_ms;
}

std::string ReplicationLog::new_replid() {
    static const char* hex = "0123456789abcdef";
    std::random_device rd;
    std::string id(40, '0');
    for (char& c : id) c = hex[rd() & 15];
    return id;
}

std::string ReplicationLog::replid() const {
    std::lock_guard<std::mutex> lock(mu_);
    return replid_;
}

void ReplicationLog::reset(std::string replid, uint64_t last_offset) {
    std::lock_guard<std::mutex> lock(mu_);
    q_.clear();
    bytes_ = 0;
    replid_ = std::move(replid);
    last_.store(last_offset, std::memory_order_release);
    cv_.notify_all();  // waiters re-evaluate against the new lineage
}

uint64_t ReplicationLog::first_offset() const {
    std::lock_guard<std::mutex> lock(mu_);
    return q_.empty() ? last_.load(std::memory_order_relaxed) + 1 : q_.front().offset;
}

// Appends under the lock, trims the front to stay within budget, wakes waiters if any.
uint64_t ReplicationLog::push_locked(std::string payload, uint64_t offset) {
    bytes_ += payload.size() + kRecordOverhead;
    q_.push_back(ReplRecord{offset, std::move(payload)});
    while (bytes_ > opts_.max_bytes && q_.size() > 1) {  // always keep the newest record
        bytes_ -= q_.front().payload.size() + kRecordOverhead;
        q_.pop_front();
        ++trimmed_;
    }
    ++appended_;
    last_.store(offset, std::memory_order_release);
    if (waiters_ > 0) cv_.notify_all();
    return offset;
}

void ReplicationLog::append_local(std::string payload) {
    std::lock_guard<std::mutex> lock(mu_);
    push_locked(std::move(payload), last_.load(std::memory_order_relaxed) + 1);
}

void ReplicationLog::on_set(const std::string& key, const std::string& value, std::optional<int64_t> ttl_ms) {
    std::string p;
    p.reserve(48 + key.size() + value.size());
    put_header(p, ttl_ms ? 5 : 3);
    put_bulk(p, "SET");
    put_bulk(p, key);
    put_bulk(p, value);
    if (ttl_ms) {
        put_bulk(p, "PXAT");
        put_bulk(p, std::to_string(wall() + *ttl_ms));  // absolute: valid whenever and wherever applied
    }
    append_local(std::move(p));
}

void ReplicationLog::on_del(const std::string& key) {
    std::string p;
    p.reserve(24 + key.size());
    put_header(p, 2);
    put_bulk(p, "DEL");
    put_bulk(p, key);
    append_local(std::move(p));
}

void ReplicationLog::on_expire(const std::string& key, int64_t ttl_ms) {
    std::string p;
    p.reserve(48 + key.size());
    put_header(p, 3);
    put_bulk(p, "PEXPIREAT");
    put_bulk(p, key);
    put_bulk(p, std::to_string(wall() + ttl_ms));
    append_local(std::move(p));
}

void ReplicationLog::on_persist(const std::string& key) {
    std::string p;
    p.reserve(24 + key.size());
    put_header(p, 2);
    put_bulk(p, "PERSIST");
    put_bulk(p, key);
    append_local(std::move(p));
}

ReplicationLog::AppendStatus ReplicationLog::append_replicated(uint64_t offset, std::string payload) {
    std::lock_guard<std::mutex> lock(mu_);
    const uint64_t last = last_.load(std::memory_order_relaxed);
    if (offset <= last) return AppendStatus::Duplicate;  // already have it: ignore (idempotent delivery)
    if (offset != last + 1) return AppendStatus::Gap;    // a record is missing: caller must resynchronise
    push_locked(std::move(payload), offset);
    return AppendStatus::Appended;
}

ReplicationLog::ReadStatus ReplicationLog::read_from(uint64_t from, size_t max_bytes, std::vector<ReplRecord>* out) const {
    std::lock_guard<std::mutex> lock(mu_);
    const uint64_t last = last_.load(std::memory_order_relaxed);
    if (from > last + 1) return ReadStatus::Future;           // consumer claims to be ahead of this log
    if (from == last + 1) return ReadStatus::Ok;              // caught up: nothing to send
    if (q_.empty() || from < q_.front().offset) return ReadStatus::TooOld;
    const size_t idx = static_cast<size_t>(from - q_.front().offset);  // offsets are consecutive (I1)
    size_t taken = 0;
    for (size_t i = idx; i < q_.size(); ++i) {
        if (taken > 0 && taken + q_[i].payload.size() > max_bytes) break;
        out->push_back(q_[i]);
        taken += q_[i].payload.size();
    }
    return ReadStatus::Ok;
}

bool ReplicationLog::wait_beyond(uint64_t after, int timeout_ms) {
    std::unique_lock<std::mutex> lock(mu_);
    if (last_.load(std::memory_order_relaxed) > after) return true;
    if (closed_) return false;
    ++waiters_;
    cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&] {
        return closed_ || last_.load(std::memory_order_relaxed) > after;
    });
    --waiters_;
    return last_.load(std::memory_order_relaxed) > after;
}

void ReplicationLog::close() {
    std::lock_guard<std::mutex> lock(mu_);
    closed_ = true;
    cv_.notify_all();
}

ReplicationLog::Stats ReplicationLog::stats() const {
    std::lock_guard<std::mutex> lock(mu_);
    return Stats{appended_, trimmed_, q_.size(), bytes_};
}
