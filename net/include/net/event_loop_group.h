#pragma once

#include"net/event_loop.h"

#include<atomic>
#include<cstddef>
#include<memory>
#include<thread>
#include<vector>

// 仿的Netty
class EventLoopGroup {
public:
    explicit EventLoopGroup(std::size_t loops);

    ~EventLoopGroup();

    EventLoopGroup(const EventLoopGroup&) = delete;

    EventLoopGroup& operator=(const EventLoopGroup&) = delete;

    void start();

    void stop();

    /// @brief 从线程池取个线程用
    EventLoop* next();

    std::size_t size() const {
        return loops_.size();
    }

private:
    // 线程池的线程
    std::vector<std::unique_ptr<EventLoop> > loops_;
    std::vector<std::thread> threads_;
    // 线程选择器
    std::atomic<std::size_t> next_{0};
};
