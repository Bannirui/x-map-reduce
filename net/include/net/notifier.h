#pragma once

namespace xmr::net {
    class Notifier {
    public:
        Notifier();

        ~Notifier();

        Notifier(const Notifier&) = delete;

        Notifier& operator=(const Notifier&) = delete;

        int fd() const noexcept {
            return fd_;
        }

        void notify() noexcept;

        void drain();

    private:
        int fd_ = -1;
    };
} // namespace xmr::net
