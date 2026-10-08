#pragma once

#include"net/notifier.h"
#include"net/timer.h"

#include<atomic>
#include<cstdint>
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

    class Poller {
    public:
        Poller();

        ~Poller();

        Poller(const Poller&) = delete;

        Poller& operator=(const Poller&) = delete;

        void add(int fd, std::uint32_t events);

        void modify(int fd, std::uint32_t events);

        void remove(int fd);

        std::vector<Event> wait(int timeoutMs);

    private:
        int epollFd_ = -1;
    };

    class EventLoop {
    public:
        using Handler = std::function<void(std::uint32_t events)>;

        EventLoop();

        ~EventLoop();

        EventLoop(const EventLoop&) = delete;

        EventLoop& operator=(const EventLoop&) = delete;

        void run();

        void stop();

        void runInLoop(std::function<void()> task);

        void queueInLoop(std::function<void()> task);

        void add(int fd, std::uint32_t events, Handler handler);

        void modify(int fd, std::uint32_t events);

        void remove(int fd);

        TimerQueue::TimerId addTimer(TimerQueue::Duration delay, TimerQueue::Callback callback);

        TimerQueue::TimerId addInterval(TimerQueue::Duration interval, TimerQueue::Callback callback);

        bool cancelTimer(TimerQueue::TimerId id);

        bool isInLoopThread() const;

        void wakeup();

    private:
        void doPending();

        Poller poller_;
        TimerQueue timers_;
        Notifier notifier_;
        std::unordered_map<int, Handler> handlers_;
        std::mutex mutex_;
        std::queue<std::function<void()> > pending_;
        std::atomic<bool> stopping_{false};
        std::thread::id threadId_;
    };
} // namespace xmr::net
