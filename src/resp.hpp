#pragma once
// RESP (REdis Serialization Protocol) parsing and encoding.
//
// A client command arrives as an array of bulk strings, e.g. SET name Naseem:
//   *3\r\n$3\r\nSET\r\n$4\r\nname\r\n$6\r\nNaseem\r\n
// TCP is a byte stream: one recv() may hold half a command or five commands,
// so the parser reports Incomplete and lets the caller wait for more bytes.
#include <string>
#include <string_view>
#include <vector>

enum class ParseStatus { Ok, Incomplete, Error };

struct ParseResult {
    ParseStatus status = ParseStatus::Incomplete;
    size_t consumed = 0;             // bytes to drop from the buffer when status == Ok
    std::vector<std::string> args;   // args[0] is the command name
    std::string error;               // set when status == Error
};

// Parses one command from the front of `buf`. Also accepts "inline" commands
// (plain text like `PING\r\n`) so you can test with nc/telnet.
ParseResult parse_command(std::string_view buf);

// Reply encoders. Each returns the exact bytes to send back to the client.
std::string resp_simple(std::string_view s);   // +OK\r\n
std::string resp_error(std::string_view s);    // -ERR message\r\n
std::string resp_integer(long long n);         // :1\r\n
std::string resp_bulk(std::string_view s);     // $6\r\nNaseem\r\n
std::string resp_null();                       // $-1\r\n  (key not found)
