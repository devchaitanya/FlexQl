#pragma once
#include "concurrency/thread_pool.h"
#include "storage/wal.h"
#include <string>
#include <atomic>

// Global WAL writer — opened once at startup, shared across all client threads.
extern WalWriter g_wal;

class TcpServer {
public:
    TcpServer(int port, int num_threads);
    ~TcpServer();

    // Blocks until stop() is called.
    void run();
    void stop();

private:
    void handle_client(int client_fd);

    int              port_;
    int              server_fd_ = -1;
    std::atomic_bool running_{false};
    ThreadPool       pool_;
};
