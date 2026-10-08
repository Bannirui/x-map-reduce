#include"net/event_loop.h"

#include<cerrno>
#include<cstring>
#include<stdexcept>
#include<string>

#include<unistd.h>

namespace xmr::net {
    namespace {
        std::runtime_error systemError(const std::string& what) {
            return std::runtime_error(what + ": " + std::strerror(errno));
        }
    } // namespace

    Poller::Poller() {
        epollFd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epollFd_ < 0) {
            throw systemError("epoll_create1 failed");
        }
    }

    Poller::~Poller() {
        if (epollFd_ >= 0) {
            ::close(epollFd_);
        }
    }

    void Poller::add(int fd, std::uint32_t events) {
        epoll_event event{};
        event.events = events;
        event.data.fd = fd;
        if (::epoll_ctl(epollFd_, EPOLL_CTL_ADD, fd, &event) != 0) {
            throw systemError("epoll_ctl(ADD) failed");
        }
    }

    void Poller::modify(int fd, std::uint32_t events) {
        epoll_event event{};
        event.events = events;
        event.data.fd = fd;
        if (::epoll_ctl(epollFd_, EPOLL_CTL_MOD, fd, &event) != 0) {
            throw systemError("epoll_ctl(MOD) failed");
        }
    }

    void Poller::remove(int fd) {
        if (::epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr) != 0) {
            throw systemError("epoll_ctl(DEL) failed");
        }
    }

    std::vector<Event> Poller::wait(int timeoutMs) {
        std::vector<epoll_event> events(64);
        while (true) {
            const int ready = ::epoll_wait(epollFd_, events.data(), static_cast<int>(events.size()), timeoutMs);
            if (ready < 0) {
                if (errno == EINTR) {
                    return {};
                }
                throw systemError("epoll_wait failed");
            }
            std::vector<Event> result;
            result.reserve(static_cast<std::size_t>(ready));
            for (int i = 0; i < ready; ++i) {
                result.push_back(Event{events[i].data.fd, events[i].events});
            }
            return result;
        }
    }
} // namespace xmr::net
