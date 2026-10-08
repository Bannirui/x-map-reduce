#include"net/thread_pool.h"

#include<stdexcept>
#include<utility>

namespace xmr::net {
    ThreadPool::ThreadPool(std::size_t threads) {
        if (threads == 0) {
            throw std::runtime_error("thread pool needs at least one thread");
        }
        workers_.reserve(threads);
        // 创建线程
        for (std::size_t i = 0; i < threads; ++i) {
            workers_.emplace_back([this] { run(); });
        }
    }

    ThreadPool::~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        ready_.notify_all();
        for (std::thread& worker : workers_) {
            worker.join();
        }
    }

    void ThreadPool::submit(std::function<void()> job) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                throw std::runtime_error("thread pool is stopping");
            }
            jobs_.push(std::move(job));
        }
        ready_.notify_one();
    }

    void ThreadPool::run() {
        while (true) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                ready_.wait(lock, [this] { return stopping_ || !jobs_.empty(); });
                if (jobs_.empty()) {
                    if (stopping_) {
                        return;
                    }
                    continue;
                }
                job = std::move(jobs_.front());
                jobs_.pop();
            }
            // 执行任务
            job();
        }
    }
} // namespace xmr::net
