#include "concurrency/thread_pool.h"

ThreadPool::ThreadPool(size_t num_threads) {
    workers_.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
        workers_.emplace_back(&ThreadPool::worker_loop, this);
}

ThreadPool::~ThreadPool() { shutdown(); }

void ThreadPool::submit(std::function<void()> task) {
    {
        std::lock_guard lock(mtx_);
        queue_.push(std::move(task));
    }
    cv_.notify_one();
}

void ThreadPool::shutdown() {
    stop_ = true;
    cv_.notify_all();
    for (auto& t : workers_)
        if (t.joinable()) t.join();
    workers_.clear();
}

void ThreadPool::worker_loop() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock lock(mtx_);
            cv_.wait(lock, [this]{ return stop_ || !queue_.empty(); });
            if (stop_ && queue_.empty()) return;
            task = std::move(queue_.front());
            queue_.pop();
        }
        task();
    }
}
