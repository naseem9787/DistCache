#include "commands.hpp"

#include <algorithm>
#include <cctype>

#include "resp.hpp"

namespace {

std::string upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); });
    return s;
}

std::string wrong_args(const std::string& cmd) {
    return resp_error("ERR wrong number of arguments for '" + cmd + "' command");
}

}  // namespace

std::string execute(Store& store, const std::vector<std::string>& args) {
    if (args.empty()) return "";  // blank line: nothing to say
    const std::string cmd = upper(args[0]);

    if (cmd == "PING") {
        if (args.size() == 1) return resp_simple("PONG");
        if (args.size() == 2) return resp_bulk(args[1]);
        return wrong_args("ping");
    }
    if (cmd == "ECHO") {
        return args.size() == 2 ? resp_bulk(args[1]) : wrong_args("echo");
    }
    if (cmd == "SET") {
        // EX/PX/NX options arrive in Phase 2.
        if (args.size() != 3) return args.size() < 3 ? wrong_args("set") : resp_error("ERR syntax error");
        store.set(args[1], args[2]);
        return resp_simple("OK");
    }
    if (cmd == "GET") {
        if (args.size() != 2) return wrong_args("get");
        auto v = store.get(args[1]);
        return v ? resp_bulk(*v) : resp_null();
    }
    if (cmd == "DEL") {
        if (args.size() < 2) return wrong_args("del");
        long long removed = 0;
        for (size_t i = 1; i < args.size(); ++i) removed += store.del(args[i]) ? 1 : 0;
        return resp_integer(removed);
    }
    if (cmd == "DBSIZE") {
        return resp_integer(static_cast<long long>(store.size()));
    }
    if (cmd == "COMMAND") {
        return "*0\r\n";  // redis-cli asks for this on startup; an empty list is fine
    }
    return resp_error("ERR unknown command '" + args[0] + "'");
}
