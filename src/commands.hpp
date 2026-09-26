#pragma once
// Turns a parsed command (["SET","name","Naseem"]) into a RESP reply.
// Kept separate from the network code so it can be unit-tested without sockets.
#include <string>
#include <vector>

#include "store.hpp"

std::string execute(Store& store, const std::vector<std::string>& args);
