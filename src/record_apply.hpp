#pragma once
// Applying one LOGICAL mutation record to an engine.
//
// A logical record is the RESP command the observers emit for every state change:
//     SET key value [PXAT unix_ms]    DEL key    PEXPIREAT key unix_ms    PERSIST key
// Absolute deadlines (PXAT / PEXPIREAT) make every record idempotent and independent of when it
// is applied: replaying it later, twice, or on another machine yields the same final state.
//
// This one function is shared by crash recovery (WAL replay) and replication (a replica applying
// the primary's stream), so the two cannot drift apart.
#include <cstdint>
#include <string>
#include <vector>

#include "engine.hpp"

enum class ApplyResult {
    Applied,         // the record changed (or re-asserted) engine state
    ExpiredSkipped,  // a SET/PEXPIREAT whose deadline had already passed: the key was removed instead
    Invalid,         // not a recognised record (unknown command or malformed arguments)
};

// `now_ms` is the wall clock used to turn an absolute deadline into a remaining TTL.
// The engine's observer (if any) fires exactly as for a client write, so a replica's own WAL
// and replication log see the applied records.
ApplyResult apply_logical_record(Engine& engine, const std::vector<std::string>& args, int64_t now_ms);
