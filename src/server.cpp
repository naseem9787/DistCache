#include "server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <algorithm>
#include <cstring>
#include <deque>
#include <mutex>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "commands.hpp"
#include "diag.hpp"
#include "resp.hpp"
#include "store.hpp"

namespace {

constexpr size_t kMaxInputBuffer = 64 * 1024 * 1024;
constexpr int kLoopTimeoutMs = 100;     // also the active-expiration tick and the stop latency
constexpr int64_t kSweepEveryMs = 100;

// A batch of replies that must not leave until log record `seq` is durable (fsync=always).
struct Held {
    uint64_t seq;
    std::string bytes;
};

struct Conn {
    std::string in;   // bytes received but not yet parsed
    std::string out;  // replies cleared to leave: durable (or not dependent on durability)
    std::deque<Held> held;  // replies waiting for durability, in order; empty unless fsync=always
    bool close_after_flush = false;
};

void epoll_set(int ep, int fd, uint32_t events, int op) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    epoll_ctl(ep, op, fd, &ev);
}

}  // namespace

// ---------------------------------------------------------------------------
// One worker = one thread = one epoll instance = a private set of connections.
// ---------------------------------------------------------------------------
class Server::Worker {
public:
    Worker(Engine& engine, CommandHooks* hooks, const std::atomic<bool>& running)
        : engine_(engine), hooks_(hooks), running_(running) {}
    ~Worker() {
        for (auto& [fd, c] : conns_) close(fd);
        for (int fd : pending_) close(fd);
        if (wake_ >= 0) close(wake_);
        if (ep_ >= 0) close(ep_);
    }

    bool init() {
        ep_ = epoll_create1(EPOLL_CLOEXEC);
        wake_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (ep_ < 0 || wake_ < 0) return false;
        epoll_set(ep_, wake_, EPOLLIN, EPOLL_CTL_ADD);
        return true;
    }

    void start() { thread_ = std::thread([this] { run(); }); }
    void join() { if (thread_.joinable()) thread_.join(); }

    // Called from the acceptor thread. The only cross-thread entry point into a worker:
    // a mutex-protected queue plus an eventfd that wakes the worker's epoll_wait.
    void add(int fd) {
        {
            std::lock_guard<std::mutex> g(pending_mu_);
            pending_.push_back(fd);
        }
        notify();
    }

    // Wakes the worker's epoll_wait. Called by the acceptor (new connection) and by the log
    // flusher thread (durable sequence advanced). Safe from any thread.
    void notify() {
        uint64_t one = 1;
        ssize_t r = write(wake_, &one, sizeof one);
        (void)r;
    }

private:
    void run() {
        pthread_setname_np(pthread_self(), "dc-worker");
        DC_TAG_THREAD("worker");
        epoll_event events[256];
        int64_t next_sweep = Store::real_now_ms() + kSweepEveryMs;
        while (running_.load(std::memory_order_acquire)) {
            int n = epoll_wait(ep_, events, 256, kLoopTimeoutMs);
            if (n < 0) {
                if (errno == EINTR) continue;
                break;
            }
            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;
                if (fd == wake_) { adopt_pending(); continue; }
                if (events[i].events & (EPOLLERR | EPOLLHUP)) { close_conn(fd); continue; }
                if (events[i].events & EPOLLIN) on_readable(fd);
                if (conns_.count(fd) && (events[i].events & EPOLLOUT)) flush(fd);
            }
            // Replies parked behind an fsync are released here. The eventfd wakes us promptly when
            // the flusher publishes a new durable sequence number; checking every iteration also
            // makes a missed wake-up cost at most one loop timeout instead of a stuck reply.
            if (!waiting_.empty()) release_waiting();
            // Active expiration. Every worker may call this; the Engine serializes it.
            // (With the unsynchronized "single" engine there is exactly one worker.)
            int64_t t = Store::real_now_ms();
            if (t >= next_sweep) {
                engine_.sweep();
                next_sweep = t + kSweepEveryMs;
            }
        }
    }

    void adopt_pending() {
        uint64_t counter;
        ssize_t r = read(wake_, &counter, sizeof counter);  // reset the eventfd
        (void)r;
        std::vector<int> fds;
        {
            std::lock_guard<std::mutex> g(pending_mu_);
            fds.swap(pending_);
        }
        for (int fd : fds) {
            conns_[fd] = Conn{};
            epoll_set(ep_, fd, EPOLLIN, EPOLL_CTL_ADD);
        }
    }

    void on_readable(int fd) {
        auto it = conns_.find(fd);
        if (it == conns_.end()) return;
        Conn& c = it->second;
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
        settle(fd, c);
        flush(fd);
    }

    // The durability gate. Under fsync=always, the replies this batch produced are moved from
    // `out` into `held` tagged with the log sequence number they depend on. They enter `out`
    // (and so can be written to the socket) only when durable() has reached that number.
    //
    // INVARIANT: no reply that depends on write N is in `out` unless durable() >= N. Replies
    // that follow a held reply on the same connection are held behind it, because replies on a
    // connection must stay in order; hence "!c.held.empty()" also forces queueing.
    void gate(Conn& c, size_t batch_start) {
        const uint64_t seq = hooks_ ? hooks_->take_pending() : 0;
        if ((seq != 0 || !c.held.empty()) && c.out.size() > batch_start) {
            c.held.push_back(Held{seq, c.out.substr(batch_start)});
            c.out.resize(batch_start);
        }
    }

    // Moves every held batch whose sequence number is durable into `out`.
    void settle(int fd, Conn& c) {
        if (c.held.empty()) return;
        const uint64_t d = hooks_ ? hooks_->durable() : UINT64_MAX;  // read AFTER enqueue: no lost wake-up
        while (!c.held.empty() && c.held.front().seq <= d) {
            c.out += c.held.front().bytes;
            c.held.pop_front();
        }
        if (c.held.empty()) waiting_.erase(fd); else waiting_.insert(fd);
    }

    void release_waiting() {
        std::vector<int> fds(waiting_.begin(), waiting_.end());
        for (int fd : fds) {
            auto it = conns_.find(fd);
            if (it == conns_.end()) { waiting_.erase(fd); continue; }
            settle(fd, it->second);
            flush(fd);  // may close the connection
        }
    }

    // Parse as many complete commands as the buffer holds (pipelining), leaving
    // any partial command in place until more bytes arrive.
    void process_input(Conn& c) {
        const size_t batch_start = c.out.size();
        size_t offset = 0;
        while (offset < c.in.size() && !c.close_after_flush) {
            ParseResult r = parse_command(std::string_view(c.in).substr(offset));
            if (r.status == ParseStatus::Incomplete) break;
            if (r.status == ParseStatus::Error) {
                c.out += resp_error("ERR Protocol error: " + r.error);
                c.close_after_flush = true;
                break;
            }
            c.out += execute(engine_, r.args, hooks_);  // <-- the only place workers touch shared state
            offset += r.consumed;
        }
        c.in.erase(0, offset);
        gate(c, batch_start);
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
        if (c.close_after_flush && c.held.empty()) { close_conn(fd); return; }  // not before held replies went out
        epoll_set(ep_, fd, EPOLLIN, EPOLL_CTL_MOD);
    }

    void close_conn(int fd) {
        waiting_.erase(fd);
        epoll_ctl(ep_, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        conns_.erase(fd);
    }

    Engine& engine_;
    CommandHooks* hooks_;
    const std::atomic<bool>& running_;
    int ep_ = -1;
    int wake_ = -1;
    std::thread thread_;
    std::unordered_map<int, Conn> conns_;  // touched ONLY by this worker's thread
    std::unordered_set<int> waiting_;      // connections with replies held for durability (same thread only)
    std::mutex pending_mu_;                // guards pending_ (acceptor -> worker handoff)
    std::vector<int> pending_;
};

// ---------------------------------------------------------------------------

Server::Server(Engine& engine, ServerOptions opts) : engine_(engine), opts_(opts) {}
Server::~Server() { stop(); }

bool Server::start(std::string* error) {
    auto fail = [&](const std::string& what) {
        if (error) *error = what + ": " + std::strerror(errno);
        if (listen_fd_ >= 0) { close(listen_fd_); listen_fd_ = -1; }
        workers_.clear();
        return false;
    };

    listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) return fail("socket");
    int one = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(opts_.port));
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0) return fail("bind");
    if (listen(listen_fd_, 511) < 0) return fail("listen");
    socklen_t len = sizeof addr;
    getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);

    running_.store(true, std::memory_order_release);
    for (int i = 0; i < std::max(1, opts_.workers); ++i) {
        workers_.push_back(std::make_unique<Worker>(engine_, opts_.hooks, running_));
        if (!workers_.back()->init()) { running_ = false; return fail("worker init"); }
    }
    if (opts_.hooks) {  // wake each worker when the log's durable sequence advances
        for (auto& w : workers_) {
            Worker* wp = w.get();
            listener_ids_.push_back(opts_.hooks->add_durable_listener([wp] { wp->notify(); }));
        }
    }
    for (auto& w : workers_) w->start();
    acceptor_ = std::thread([this] { accept_loop(); });
    started_ = true;
    return true;
}

void Server::accept_loop() {
    pthread_setname_np(pthread_self(), "dc-accept");
    DC_TAG_THREAD("accept");
    pollfd pfd{listen_fd_, POLLIN, 0};
    size_t next = 0;
    while (running_.load(std::memory_order_acquire)) {
        if (poll(&pfd, 1, kLoopTimeoutMs) <= 0) continue;
        for (;;) {
            int fd = accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0) break;  // EAGAIN: nothing more pending
            int one = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);  // don't batch small replies
            workers_[next++ % workers_.size()]->add(fd);                 // round-robin balancing
        }
    }
}

void Server::stop() {
    if (!started_) return;
    started_ = false;
    // Unregister first: after this returns the flusher can no longer touch a Worker we destroy.
    if (opts_.hooks) for (int id : listener_ids_) opts_.hooks->remove_durable_listener(id);
    listener_ids_.clear();
    running_.store(false, std::memory_order_release);
    if (acceptor_.joinable()) acceptor_.join();
    for (auto& w : workers_) w->join();
    workers_.clear();  // destructors close every remaining socket
    if (listen_fd_ >= 0) { close(listen_fd_); listen_fd_ = -1; }
}
