// distcache server: single-threaded epoll event loop speaking RESP.
//
// One thread watches many sockets. epoll tells us which ones are ready, so we
// never block on a slow client. Each connection has an input buffer (bytes
// received but not yet parsed) and an output buffer (replies not yet sent).
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>

#include "commands.hpp"
#include "resp.hpp"
#include "store.hpp"

namespace {

constexpr size_t kMaxInputBuffer = 64 * 1024 * 1024;

struct Conn {
    std::string in;
    std::string out;
    bool close_after_flush = false;
};

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void epoll_set(int ep, int fd, uint32_t events, int op) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    epoll_ctl(ep, op, fd, &ev);
}

class Server {
public:
    explicit Server(int port) { listen_on(port); }

    void run() {
        epoll_event events[256];
        for (;;) {
            // Wake up at least every 100 ms even if no client is talking, so expired keys get swept.
            int n = epoll_wait(ep_, events, 256, 100);
            if (n < 0) {
                if (errno == EINTR) continue;
                perror("epoll_wait");
                return;
            }
            store_.sweep();  // active expiration (bounded work per call)
            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;
                if (fd == listen_fd_) { accept_all(); continue; }
                if (events[i].events & (EPOLLERR | EPOLLHUP)) { close_conn(fd); continue; }
                if (events[i].events & EPOLLIN) on_readable(fd);
                if (conns_.count(fd) && (events[i].events & EPOLLOUT)) flush(fd);
            }
        }
    }

private:
    void listen_on(int port) {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0 ||
            listen(listen_fd_, 511) < 0) {
            perror("bind/listen");
            std::exit(1);
        }
        set_nonblocking(listen_fd_);
        ep_ = epoll_create1(0);
        epoll_set(ep_, listen_fd_, EPOLLIN, EPOLL_CTL_ADD);
    }

    void accept_all() {
        for (;;) {
            int fd = accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) return;  // EAGAIN: no more pending connections
            set_nonblocking(fd);
            int one = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);  // don't batch small replies
            conns_[fd] = Conn{};
            epoll_set(ep_, fd, EPOLLIN, EPOLL_CTL_ADD);
        }
    }

    void on_readable(int fd) {
        Conn& c = conns_[fd];
        char buf[16384];
        for (;;) {
            ssize_t n = read(fd, buf, sizeof buf);
            if (n > 0) {
                c.in.append(buf, static_cast<size_t>(n));
                if (c.in.size() > kMaxInputBuffer) { close_conn(fd); return; }
            } else if (n == 0) {
                close_conn(fd);  // client closed
                return;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else if (errno != EINTR) {
                close_conn(fd);
                return;
            }
        }
        process_input(c);
        flush(fd);
    }

    // Parse as many complete commands as the buffer holds (pipelining), leaving
    // any partial command in place until more bytes arrive.
    void process_input(Conn& c) {
        size_t offset = 0;
        while (offset < c.in.size() && !c.close_after_flush) {
            ParseResult r = parse_command(std::string_view(c.in).substr(offset));
            if (r.status == ParseStatus::Incomplete) break;
            if (r.status == ParseStatus::Error) {
                c.out += resp_error("ERR Protocol error: " + r.error);
                c.close_after_flush = true;
                break;
            }
            c.out += execute(store_, r.args);
            offset += r.consumed;
        }
        c.in.erase(0, offset);
    }

    void flush(int fd) {
        auto it = conns_.find(fd);
        if (it == conns_.end()) return;
        Conn& c = it->second;
        while (!c.out.empty()) {
            ssize_t n = write(fd, c.out.data(), c.out.size());
            if (n > 0) {
                c.out.erase(0, static_cast<size_t>(n));
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                epoll_set(ep_, fd, EPOLLIN | EPOLLOUT, EPOLL_CTL_MOD);  // wake us when writable
                return;
            } else if (errno != EINTR) {
                close_conn(fd);
                return;
            }
        }
        if (c.close_after_flush) { close_conn(fd); return; }
        epoll_set(ep_, fd, EPOLLIN, EPOLL_CTL_MOD);
    }

    void close_conn(int fd) {
        epoll_ctl(ep_, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        conns_.erase(fd);
    }

    int listen_fd_ = -1;
    int ep_ = -1;
    Store store_;
    std::unordered_map<int, Conn> conns_;
};

}  // namespace

int main(int argc, char** argv) {
    int port = argc > 1 ? std::atoi(argv[1]) : 6380;
    std::signal(SIGPIPE, SIG_IGN);  // writing to a closed socket returns EPIPE instead of killing us
    std::printf("distcache listening on port %d\n", port);
    std::fflush(stdout);
    Server(port).run();
}
