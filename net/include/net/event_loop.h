#pragma once

#include<cstdint>
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
} // namespace xmr::net
