#pragma once
// Turns a parsed command (["SET","name","Naseem"]) into a RESP reply.
// Kept separate from the network code so it can be unit-tested without sockets.
// Works on any Engine, so the same code serves the single-threaded, global-lock
// and sharded back ends. Thread safety is entirely the Engine's responsibility.
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "engine.hpp"

// Optional extension points the server wires in when persistence is enabled. Keeping them
// behind an interface means the command layer knows nothing about files or logs.
class CommandHooks {
public:
    virtual ~CommandHooks() = default;
    virtual bool save(std::string* error) = 0;   // SAVE: write a snapshot and compact the log
    virtual std::string info() = 0;              // extra INFO section text, starting "# Persistence"
    // Blocking form: returns once this thread's most recent write is durable under fsync=always
    // (immediately for other policies). Kept for callers that have nothing else to do meanwhile.
    virtual void commit() = 0;

    // Non-blocking form used by the server's workers, so a worker keeps serving other
    // connections while a write waits for its fsync (and one fsync is shared by every
    // connection's pending writes, not just by the writes of one thread):
    //   take_pending():  log sequence number this thread's batch must wait for, or 0 when no
    //                    waiting is needed (no write, or a policy other than fsync=always).
    //   durable():       highest sequence number already fdatasync'ed. A reply that depends on
    //                    sequence N may be released only when durable() >= N.
    //   add/remove_durable_listener(): the callback runs on the flusher thread after durable()
    //                    advances. remove returns only after no callback is running or will run.
    virtual uint64_t take_pending() = 0;
    virtual uint64_t durable() = 0;
    virtual int add_durable_listener(std::function<void()> fn) = 0;
    virtual void remove_durable_listener(int id) = 0;
};

std::string execute(Engine& engine, const std::vector<std::string>& args, CommandHooks* hooks = nullptr);
