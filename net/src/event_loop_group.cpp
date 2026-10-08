#include"net/event_loop_group.h"

#include<stdexcept>

namespace xmr::net {
    EventLoopGroup::EventLoopGroup(std::size_t loops) {
        if (loops == 0) {
            throw std::runtime_error("event loop group needs at least one loop");
        }
        loops_.reserve(loops);
        for (std::size_t i = 0; i < loops; ++i) {
            loops_.emplace_back(std::make_unique<EventLoop>());
        }
    }

    EventLoopGroup::~EventLoopGroup() {
        stop();
    }

    void EventLoopGroup::start() {
        threads_.reserve(loops_.size());
        for (auto& loop : loops_) {
            EventLoop* raw = loop.get();
            threads_.emplace_back([raw] { raw->run(); });
        }
    }

    void EventLoopGroup::stop() {
        for (auto& loop : loops_) {
            loop->stop();
        }
        for (std::thread& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    EventLoop* EventLoopGroup::next() {
        const std::size_t index = next_.fetch_add(1);
        return loops_[index % loops_.size()].get();
    }
} // namespace xmr::net
