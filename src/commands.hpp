#pragma once
// Turns a parsed command (["SET","name","Naseem"]) into a RESP reply.
// Kept separate from the network code so it can be unit-tested without sockets.
// Works on any Engine, so the same code serves the single-threaded, global-lock
// and sharded back ends. Thread safety is entirely the Engine's responsibility.
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
    // Called by a server worker after it has executed a batch of commands and BEFORE it sends
    // the replies. Under fsync=always this blocks until this thread's writes are on disk.
    virtual void commit() = 0;
};

std::string execute(Engine& engine, const std::vector<std::string>& args, CommandHooks* hooks = nullptr);
