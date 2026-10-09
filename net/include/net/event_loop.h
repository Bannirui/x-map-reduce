#pragma once

#include"net/notifier.h"
#include"net/timer.h"

#include<atomic>
#include<cstdint>
#include<exception>
#include<functional>
#include<mutex>
#include<queue>
#include<thread>
#include<unordered_map>
#include<vector>

#include<sys/epoll.h>

namespace xmr::net {
    inline constexpr std::uint32_t kReadable = EPOLLIN;
    inline constexpr std::uint32_t kWritable = EPOLLOUT;
    inline constexpr std::uint32_t kBroken = EPOLLHUP | EPOLLERR;

    struct Event {
        int fd = -1;
        std::uint32_t events = 0;
    };

    // 封装的系统的多路复用器
    class Poller {
    public:
        Poller();

        ~Poller();

        Poller(const Poller&) = delete;

        Poller& operator=(const Poller&) = delete;

        /// @brief 让底层selector监听fd的可读还是可写
        /// @param fd 监听哪个socket
        /// @param events 监听socket的可读还是可写
        void add(int fd, std::uint32_t events);

        void modify(int fd, std::uint32_t events);

        void remove(int fd);

        std::vector<Event> wait(int timeoutMs);

    private:
        int epollFd_ = -1;
    };

    // 模仿的Netty 利用系统多路复用器的线程模型
    class EventLoop {
    public:
        using Handler = std::function<void(std::uint32_t events)>;

        EventLoop();

        ~EventLoop();

        EventLoop(const EventLoop&) = delete;

        EventLoop& operator=(const EventLoop&) = delete;

        void run();

        void stop();

        /// @brief 给事件循环器提交个任务 要是提交任务的线程就是当前事件循环器线程就立马执行 要是其他线程也就是外界提交进来的就先缓存到队列里面等待合适的时机
        /// @param task 要提交的任务
        void runInLoop(std::function<void()> task);

        /// @brief 把外面提交进来的任务先缓存起来
        void queueInLoop(std::function<void()> task);

        /// @brief 注册个网络IO事件
        /// @param fd 哪个socket
        /// @param events 关注的是可读还是可写
        /// @param handler 底层slector读写事件就绪了回调函数
        void add(int fd, std::uint32_t events, Handler handler);

        void modify(int fd, std::uint32_t events);

        void remove(int fd);

        TimerQueue::TimerId addTimer(TimerQueue::Duration delay, TimerQueue::Callback callback);

        TimerQueue::TimerId addInterval(TimerQueue::Duration interval, TimerQueue::Callback callback);

        bool cancelTimer(TimerQueue::TimerId id);

        bool isInLoopThread() const;

        void wakeup();

        std::exception_ptr error() const {
            return error_;
        }

    private:
        void doPending();
        // selector
        Poller poller_;
        TimerQueue timers_;
        Notifier notifier_;
        std::unordered_map<int, Handler> handlers_;
        std::mutex mutex_;
        // 外面提交进来的任务先缓存起来 合适的时候执行
        std::queue<std::function<void()> > pending_;
        std::atomic<bool> stopping_{false};
        std::atomic<std::thread::id> threadId_{};
        std::exception_ptr error_;
    };
} // namespace xmr::net
