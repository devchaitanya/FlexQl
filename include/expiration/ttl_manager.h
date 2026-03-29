#pragma once
#include <thread>
#include <atomic>
#include <chrono>

// Background thread that periodically compacts all tables,
// physically removing deleted and expired rows.
class TtlManager {
public:
    // sweep_interval_sec: how often to run compaction (default 60s).
    explicit TtlManager(int sweep_interval_sec = 60);
    ~TtlManager();

    void start();
    void stop();

private:
    void run();

    int              interval_sec_;
    std::thread      thread_;
    std::atomic_bool running_{false};
};
