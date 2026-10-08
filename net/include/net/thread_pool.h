#pragma once

#include<condition_variable>
#include<cstddef>
#include<functional>
#include<mutex>
#include<queue>
#include<thread>
#include<vector>

namespace xmr::net {
    // 线程池
    class ThreadPool {
    public:
        /**
         * @param threads 线程池配备多少个线程
         */
        explicit ThreadPool(std::size_t threads);

        ~ThreadPool();

        ThreadPool(const ThreadPool&) = delete;

        ThreadPool& operator=(const ThreadPool&) = delete;

        /**
         * @param job 给线程池提交个任务
         */
        void submit(std::function<void()> job);

        std::size_t size() const {
            return workers_.size();
        }

    private:
        void run();

    private:
        // 活跃线程
        std::vector<std::thread> workers_;
        // 任务队列
        std::queue<std::function<void()> > jobs_;
        std::mutex mutex_;
        // 有任务提交进来就得通知线程有活要干了
        std::condition_variable ready_;
        // 表示线程池状态
        bool stopping_ = false;
    };
} // namespace xmr::net
