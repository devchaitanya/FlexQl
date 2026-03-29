#pragma once
#include <thread>
#include <vector>
#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <atomic>

class ThreadPool {
public:
    explicit ThreadPool(size_t num_threads);
    ~ThreadPool();

    // Submit a task; returns immediately.
    void submit(std::function<void()> task);

    void shutdown();

private:
    void worker_loop();

    std::vector<std::thread>          workers_;
    std::queue<std::function<void()>> queue_;
    std::mutex                        mtx_;
    std::condition_variable           cv_;
    std::atomic_bool                  stop_{false};
};
