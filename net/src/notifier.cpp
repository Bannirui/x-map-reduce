#include"net/notifier.h"

#include<cerrno>
#include<cstdint>
#include<cstring>
#include<stdexcept>
#include<string>

#include<sys/eventfd.h>
#include<unistd.h>

namespace xmr::net {
    Notifier::Notifier() {
        fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (fd_ < 0) {
            throw std::runtime_error("eventfd failed: " + std::string(std::strerror(errno)));
        }
    }

    Notifier::~Notifier() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    void Notifier::notify() noexcept {
        const std::uint64_t one = 1;
        const ssize_t written = ::write(fd_, &one, sizeof(one));
        (void) written;
    }

    void Notifier::drain() {
        std::uint64_t value = 0;
        while (true) {
            const ssize_t read = ::read(fd_, &value, sizeof(value));
            if (read == static_cast<ssize_t>(sizeof(value))) {
                continue;
            }
            if (read < 0 && errno == EINTR) {
                continue;
            }
            break;
        }
    }
} // namespace xmr::net
