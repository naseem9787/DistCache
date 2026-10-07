#pragma once
// Turns a parsed command (["SET","name","Naseem"]) into a RESP reply.
// Kept separate from the network code so it can be unit-tested without sockets.
// Works on any Engine, so the same code serves the single-threaded, global-lock
// and sharded back ends. Thread safety is entirely the Engine's responsibility.
#include <string>
#include <vector>

#include "engine.hpp"

std::string execute(Engine& engine, const std::vector<std::string>& args);
