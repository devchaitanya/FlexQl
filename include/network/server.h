#pragma once
#include "concurrency/thread_pool.h"
#include "storage/wal.h"
#include <string>
#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_set>

// Global WAL writer — opened once at startup, shared across all client threads.
extern WalWriter g_wal;

class TcpServer {
public:
    TcpServer(int port, int num_threads);
    ~TcpServer();

    // Blocks until request_stop()/stop() is called.
    void run();

    // Full shutdown (join threads, final snapshot). Idempotent: safe to call
    // twice (main calls it, then the destructor calls it again).
    // Must NOT be called from a signal handler — use request_stop() there.
    void stop();

    // Async-signal-safe: only an atomic store and ::shutdown() (both on the
    // POSIX async-signal-safe list). Makes the blocked accept() return so
    // run() exits; main() then calls stop() in normal context.
    void request_stop() noexcept;

private:
    void handle_client(int client_fd);

    // ── Periodic snapshots ────────────────────────────────────────────────────
    // A background thread checks once per second and snapshots when
    //   WAL >= hard limit (256 MB)                       — always, bounds recovery
    //   WAL >= soft limit (32 MB) AND no writes for 5 s  — opportunistic, when quiet
    // The snapshot is written by a fork()ed child (copy-on-write), so clients
    // are paused only for the fork itself.
    void snapshot_loop();
    bool periodic_snapshot();

    int              port_;
    std::atomic<int> server_fd_{-1};      // atomic: read by the signal handler
    std::atomic_bool running_{false};
    std::atomic_bool stopped_{false};     // stop() already ran
    ThreadPool       pool_;

    // Connected client sockets, so stop() can wake workers blocked in recv().
    // A fd is removed under the same mutex BEFORE it is closed, so stop()
    // never touches a fd number that was closed and reused.
    std::mutex              clients_mtx_;
    std::unordered_set<int> clients_;

    std::thread      snap_thread_;
    std::atomic_bool snap_running_{false};
};
