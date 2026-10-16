#include "resp_client.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>

namespace {

constexpr int kMaxDepth = 8;  // a reply nested deeper than this is treated as a protocol error

void set_timeouts(int fd, int ms) {
    timeval tv{};
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
}

}  // namespace

std::string encode_command(const std::vector<std::string>& args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& a : args) {
        out += '$';
        out += std::to_string(a.size());
        out += "\r\n";
        out += a;
        out += "\r\n";
    }
    return out;
}

bool RespConn::connect(const std::string& host, int port, int io_timeout_ms, std::string* error) {
    close();
    auto fail = [&](const std::string& why) {
        if (error) *error = why;
        close();
        return false;
    };
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &a.sin_addr) != 1) {
        // not a dotted quad: resolve (localhost etc.)
        addrinfo hints{}, *res = nullptr;
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        if (getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || !res) return fail("cannot resolve " + host);
        a.sin_addr = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr;
        freeaddrinfo(res);
    }
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) return fail("socket() failed");
    // Non-blocking connect with a deadline, then back to blocking with I/O timeouts.
    const int flags = fcntl(fd_, F_GETFL, 0);
    fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
    int rc = ::connect(fd_, reinterpret_cast<sockaddr*>(&a), sizeof a);
    if (rc != 0 && errno != EINPROGRESS) return fail(std::string("connect: ") + std::strerror(errno));
    if (rc != 0) {
        pollfd p{fd_, POLLOUT, 0};
        const int pr = poll(&p, 1, io_timeout_ms);
        if (pr <= 0) return fail("connect timed out");
        int soerr = 0;
        socklen_t len = sizeof soerr;
        getsockopt(fd_, SOL_SOCKET, SO_ERROR, &soerr, &len);
        if (soerr != 0) return fail(std::string("connect: ") + std::strerror(soerr));
    }
    fcntl(fd_, F_SETFL, flags);
    int one = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    set_timeouts(fd_, io_timeout_ms);
    buf_.clear();
    pos_ = 0;
    return true;
}

bool RespConn::adopt(int fd, int io_timeout_ms) {
    close();
    fd_ = fd;
    int one = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    set_timeouts(fd_, io_timeout_ms);
    return true;
}

void RespConn::set_io_timeout(int ms) {
    if (fd_ >= 0) set_timeouts(fd_, ms);
}

bool RespConn::has_pending_input() {
    if (fd_ < 0) return false;
    if (pos_ < buf_.size()) return true;
    pollfd p{fd_, POLLIN, 0};
    return ::poll(&p, 1, 0) > 0;
}

void RespConn::close() {
    if (fd_ >= 0) ::close(fd_);
    fd_ = -1;
    buf_.clear();
    pos_ = 0;
}

bool RespConn::send_all(const std::string& bytes) {
    if (fd_ < 0) return false;
    size_t off = 0;
    while (off < bytes.size()) {
        ssize_t n = ::send(fd_, bytes.data() + off, bytes.size() - off, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { close(); return false; }
        off += static_cast<size_t>(n);
    }
    return true;
}

bool RespConn::fill() {
    if (fd_ < 0) return false;
    compact();
    char tmp[65536];
    for (;;) {
        ssize_t n = ::recv(fd_, tmp, sizeof tmp, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { close(); return false; }
        buf_.append(tmp, static_cast<size_t>(n));
        return true;
    }
}

void RespConn::compact() {
    if (pos_ == 0) return;
    if (pos_ == buf_.size()) { buf_.clear(); pos_ = 0; }
    else if (pos_ >= 65536) { buf_.erase(0, pos_); pos_ = 0; }
}

bool RespConn::read_line(std::string& line) {
    size_t scan = pos_;
    for (;;) {
        const size_t p = buf_.find("\r\n", scan);
        if (p != std::string::npos) {
            line.assign(buf_, pos_, p - pos_);
            pos_ = p + 2;
            return true;
        }
        scan = buf_.size() > 0 ? buf_.size() - 1 : 0;  // a CRLF may straddle two reads
        if (scan < pos_) scan = pos_;
        if (!fill()) return false;
        // fill() may have compacted the buffer: positions shift, so rescan from the start of the line
        scan = pos_;
    }
}

bool RespConn::take(size_t n, std::string& out) {
    while (buf_.size() - pos_ < n + 2) {
        if (!fill()) return false;
    }
    out.assign(buf_, pos_, n);
    pos_ += n + 2;
    return true;
}

bool RespConn::raw_one(std::string& out, int depth) {
    if (depth > kMaxDepth) { close(); return false; }
    std::string line;
    if (!read_line(line) || line.empty()) return false;
    out += line;
    out += "\r\n";
    switch (line[0]) {
        case '+': case '-': case ':':
            return true;
        case '$': {
            const long n = std::strtol(line.c_str() + 1, nullptr, 10);
            if (n < 0) return true;
            while (buf_.size() - pos_ < static_cast<size_t>(n) + 2) {
                if (!fill()) return false;
            }
            out.append(buf_, pos_, static_cast<size_t>(n) + 2);
            pos_ += static_cast<size_t>(n) + 2;
            return true;
        }
        case '*': {
            const long n = std::strtol(line.c_str() + 1, nullptr, 10);
            for (long i = 0; i < n; ++i)
                if (!raw_one(out, depth + 1)) return false;
            return true;
        }
        default:
            close();
            return false;
    }
}

bool RespConn::read_raw(std::string& out) { return raw_one(out, 0); }

bool RespConn::decode(Reply& r, int depth) {
    if (depth > kMaxDepth) { close(); return false; }
    std::string line;
    if (!read_line(line) || line.empty()) return false;
    r = Reply{};
    r.type = line[0];
    switch (line[0]) {
        case '+': case '-':
            r.str.assign(line, 1, std::string::npos);
            return true;
        case ':':
            r.integer = std::strtoll(line.c_str() + 1, nullptr, 10);
            return true;
        case '$': {
            const long n = std::strtol(line.c_str() + 1, nullptr, 10);
            if (n < 0) { r.null = true; return true; }
            return take(static_cast<size_t>(n), r.str);
        }
        case '*': {
            const long n = std::strtol(line.c_str() + 1, nullptr, 10);
            if (n < 0) { r.null = true; return true; }
            r.array.resize(static_cast<size_t>(n));
            for (auto& e : r.array)
                if (!decode(e, depth + 1)) return false;
            return true;
        }
        default:
            close();
            return false;
    }
}

bool RespConn::read_reply(Reply& reply) { return decode(reply, 0); }

bool RespConn::call(const std::vector<std::string>& args, Reply& reply) {
    return send_all(encode_command(args)) && read_reply(reply);
}
