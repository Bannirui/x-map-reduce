#pragma once

#include"net/event_loop.h"

#include<atomic>
#include<cstddef>
#include<memory>
#include<thread>
#include<vector>

namespace xmr::net {
    class EventLoopGroup {
    public:
        explicit EventLoopGroup(std::size_t loops);

        ~EventLoopGroup();

        EventLoopGroup(const EventLoopGroup&) = delete;

        EventLoopGroup& operator=(const EventLoopGroup&) = delete;

        void start();

        EventLoop* next();

        std::size_t size() const {
            return loops_.size();
        }

    private:
        std::vector<std::unique_ptr<EventLoop> > loops_;
        std::vector<std::thread> threads_;
        std::atomic<std::size_t> next_{0};
    };
} // namespace xmr::net
