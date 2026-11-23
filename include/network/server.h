#pragma once
#include "concurrency/thread_pool.h"
#include "storage/wal.h"
#include <string>
#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

// Global WAL writer — opened once at startup, shared across all client threads.
extern WalWriter g_wal;

// Event-driven TCP server.
//
//   event thread (run):  epoll_wait over the listening socket + every client
//                        socket (all non-blocking). Reads arriving bytes into
//                        the client's buffer; once a full statement (ending in
//                        ';') is there, submits ONE query job to the pool.
//   pool workers:        parse → [write gate + WAL] → execute → send the reply
//                        → hand the socket back (done queue + eventfd), and the
//                        event thread re-arms it. A worker is never tied to a
//                        connection, so idle clients cost no thread.
//
// Client sockets are registered with EPOLLONESHOT: while a client's query runs
// its socket is disarmed, so one client's statements run one at a time and its
// replies come back in order. It is re-armed after the reply is sent.
class TcpServer {
public:
    TcpServer(int port, int num_threads);
    ~TcpServer();

    // Runs the event loop on the calling thread. Blocks until request_stop()
    // or stop() is called; before returning it finishes the queued queries and
    // closes every client connection.
    void run();

    // Full shutdown (waits for run() to return, joins threads, final snapshot).
    // Idempotent: safe to call twice (main calls it, then the destructor).
    // Must NOT be called from a signal handler — use request_stop() there.
    void stop();

    // Async-signal-safe: only atomic stores/loads and write() to an eventfd
    // (both on the POSIX async-signal-safe list). Wakes epoll_wait so run()
    // exits; main() then calls stop() in normal context.
    void request_stop() noexcept;

private:
    // One client connection. `in` is touched only by the event thread; `fd` is
    // fixed for the object's lifetime. Only the event thread closes `fd`, and
    // never while a query for it is running (its socket is disarmed then).
    struct Conn {
        explicit Conn(int f) : fd(f) {}
        int         fd;
        std::string in;      // bytes received but not yet a complete statement
    };

    void accept_clients(int listen_fd);                          // event thread
    void handle_readable(const std::shared_ptr<Conn>& c);         // event thread
    void close_conn(int fd);                                      // event thread
    void serve_query(const std::shared_ptr<Conn>& c, std::string sql);  // worker
    void rearm(int fd);                                           // event thread

    // ── Periodic snapshots ────────────────────────────────────────────────────
    // A background thread checks once per second and snapshots when
    //   WAL >= hard limit (256 MB)                       — always, bounds recovery
    //   WAL >= soft limit (32 MB) AND no writes for 5 s  — opportunistic, when quiet
    // The snapshot is written by a fork()ed child (copy-on-write), so clients
    // are paused only for the fork itself.
    void snapshot_loop();
    bool periodic_snapshot();

    int              port_;
    std::atomic<int> server_fd_{-1};
    std::atomic<int> wake_fd_{-1};        // eventfd: request_stop() writes to it
    int              epfd_ = -1;
    std::atomic_bool stop_req_{false};    // set by request_stop(); read by run()
    std::atomic_bool stopped_{false};     // stop() already ran
    std::mutex       run_mtx_;            // held by run(); stop() waits on it
    ThreadPool       pool_;

    std::unordered_map<int, std::shared_ptr<Conn>> conns_;   // event thread only
    std::vector<char> rbuf_;                                  // event thread read buffer

    // Sockets whose query finished: workers push here and ping wake_fd_; the
    // event thread re-arms them. Only the event thread touches epoll
    // registrations or closes client sockets.
    std::mutex       done_mtx_;
    std::vector<int> done_fds_;

    std::thread      snap_thread_;
    std::atomic_bool snap_running_{false};
};
