#include "expiration/ttl_manager.h"
#include "storage/database.h"
#include <iostream>

TtlManager::TtlManager(int sweep_interval_sec)
    : interval_sec_(sweep_interval_sec) {}

TtlManager::~TtlManager() { stop(); }

void TtlManager::start() {
    running_ = true;
    thread_  = std::thread(&TtlManager::run, this);
}

void TtlManager::stop() {
    running_ = false;
    if (thread_.joinable()) thread_.join();
}

void TtlManager::run() {
    while (running_) {
        // Sleep in 1-second increments so we respond to stop() promptly.
        for (int i = 0; i < interval_sec_ && running_; ++i)
            std::this_thread::sleep_for(std::chrono::seconds(1));

        if (running_) {
            // compact_all() takes the write gate itself, only around the
            // phases that modify a table (so a snapshot never forks mid-change).
            size_t touched = Database::instance().compact_all();
            if (touched > 0)
                std::cout << "[ttl] compacted " << touched << " table(s)\n";
        }
    }
}
