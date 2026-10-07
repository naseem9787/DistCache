#pragma once
// Multi-threaded RESP server.
//
//   acceptor thread ──accept()──► hands each new socket to a worker, round robin
//        worker 0..N-1: its OWN epoll loop + its OWN set of connections
//
// A connection belongs to exactly one worker for its whole life, so connection state
// (input/output buffers) is never shared and needs no lock. The only state shared
// between workers is the Engine, which is where all synchronization lives.
// A worker never blocks on one slow client: sockets are non-blocking and epoll tells
// it which ones are ready.
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "engine.hpp"

struct ServerOptions {
    int port = 6380;      // 0 = let the OS pick (see Server::port())
    int workers = 4;      // number of worker (epoll) threads
};

class Server {
public:
    Server(Engine& engine, ServerOptions opts);
    ~Server();
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    bool start(std::string* error = nullptr);  // bind, listen, spawn threads
    void stop();                               // idempotent; joins all threads
    int port() const { return port_; }

private:
    class Worker;
    void accept_loop();

    Engine& engine_;
    ServerOptions opts_;
    int listen_fd_ = -1;
    int port_ = 0;
    bool started_ = false;
    std::atomic<bool> running_{false};
    std::thread acceptor_;
    std::vector<std::unique_ptr<Worker>> workers_;
};
