#pragma once
// A small blocking RESP client: one TCP connection, one outstanding request stream.
// Used by the router (to forward commands to nodes) and by key migration.
//
// Two ways to read a reply:
//   read_raw   appends the exact bytes of one complete reply (nested arrays included) to a
//              buffer, without interpreting them. This is what a proxy wants: relay the
//              node's answer to the client unchanged.
//   read_reply decodes the reply into a Reply tree.
// Every I/O call has a timeout (SO_RCVTIMEO / SO_SNDTIMEO), so a node that accepts the
// connection and then never answers fails the call instead of hanging a router thread.
#include <string>
#include <vector>

struct Reply {
    char type = 0;               // '+' simple, '-' error, ':' integer, '$' bulk, '*' array
    bool null = false;           // $-1 or *-1
    long long integer = 0;       // for ':'
    std::string str;             // payload of '+', '-', '$'
    std::vector<Reply> array;    // elements of '*'

    bool is_error() const { return type == '-'; }
    bool ok() const { return type == '+' && str == "OK"; }
};

class RespConn {
public:
    RespConn() = default;
    ~RespConn() { close(); }
    RespConn(const RespConn&) = delete;
    RespConn& operator=(const RespConn&) = delete;

    // io_timeout_ms bounds connect() and every later send/recv.
    bool connect(const std::string& host, int port, int io_timeout_ms, std::string* error = nullptr);
    void close();
    bool is_open() const { return fd_ >= 0; }
    int fd() const { return fd_; }
    void set_io_timeout(int ms);  // change the send/recv timeout of an open connection
    // Take over an already connected socket (the connection is closed with this object).
    bool adopt(int fd, int io_timeout_ms);
    // True when a reply byte is already buffered or readable without blocking.
    bool has_pending_input();

    bool send_all(const std::string& bytes);
    bool read_raw(std::string& out);
    bool read_reply(Reply& reply);
    // Sends one command and decodes its reply. A transport failure closes the connection.
    bool call(const std::vector<std::string>& args, Reply& reply);

private:
    bool fill();
    bool read_line(std::string& line);
    bool raw_one(std::string& out, int depth);
    bool decode(Reply& r, int depth);
    bool take(size_t n, std::string& out);  // n payload bytes, then discards the CRLF
    void compact();

    int fd_ = -1;
    std::string buf_;
    size_t pos_ = 0;
};

// "*N\r\n$len\r\narg\r\n..." for a command (same encoding as resp_command in resp.hpp).
std::string encode_command(const std::vector<std::string>& args);
