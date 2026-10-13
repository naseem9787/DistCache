#pragma once
// Minimal blocking RESP client for tests. Replies are returned as the raw first line for + - :
// and as the payload for bulk strings ("$-1" -> "(nil)"); "<closed>" if the server hung up.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <string>
#include <vector>

class Client {
public:
    explicit Client(int port) {
        fd_ = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_port = htons(static_cast<uint16_t>(port));
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        timeval tv{5, 0};  // a hung server fails the test instead of hanging it
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        if (connect(fd_, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) { close(fd_); fd_ = -1; }
    }
    ~Client() { if (fd_ >= 0) close(fd_); }
    bool ok() const { return fd_ >= 0; }

    static std::string encode(const std::vector<std::string>& args) {
        std::string s = "*" + std::to_string(args.size()) + "\r\n";
        for (const auto& a : args) s += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
        return s;
    }

    void send_raw(const std::string& s) {
        size_t off = 0;
        while (off < s.size()) {
            ssize_t n = send(fd_, s.data() + off, s.size() - off, MSG_NOSIGNAL);
            if (n <= 0) return;
            off += static_cast<size_t>(n);
        }
    }

    std::string cmd(const std::vector<std::string>& args) {
        send_raw(encode(args));
        return read_reply();
    }

    std::string read_reply() {
        std::string line;
        if (!read_line(line) || line.empty()) return "<closed>";
        if (line[0] == '$') {
            long len = std::stol(line.substr(1));
            if (len < 0) return "(nil)";
            std::string data;
            if (!read_exact(static_cast<size_t>(len) + 2, data)) return "<closed>";
            data.resize(static_cast<size_t>(len));
            return data;
        }
        return line;  // +OK / -ERR ... / :123
    }

private:
    bool fill() {
        char tmp[4096];
        ssize_t n = recv(fd_, tmp, sizeof tmp, 0);
        if (n <= 0) return false;
        buf_.append(tmp, static_cast<size_t>(n));
        return true;
    }
    bool read_line(std::string& out) {
        for (;;) {
            size_t p = buf_.find("\r\n");
            if (p != std::string::npos) { out = buf_.substr(0, p); buf_.erase(0, p + 2); return true; }
            if (!fill()) return false;
        }
    }
    bool read_exact(size_t n, std::string& out) {
        while (buf_.size() < n) if (!fill()) return false;
        out = buf_.substr(0, n);
        buf_.erase(0, n);
        return true;
    }
    int fd_ = -1;
    std::string buf_;
};
