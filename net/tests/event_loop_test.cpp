#include "net/event_loop.h"
#include "net/event_loop_group.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <string>
#include <thread>

#include <sys/socket.h>
#include <unistd.h>

namespace {

std::atomic<int> failures{0};

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

}  // namespace

int main() {
    using std::chrono::milliseconds;

    try {
        {
            EventLoop loop;
            std::thread thread([&] { loop.run(); });
            std::promise<std::thread::id> promise;
            std::future<std::thread::id> future = promise.get_future();
            loop.runInLoop([&] { promise.set_value(std::this_thread::get_id()); });
            check(future.get() != std::this_thread::get_id(), "runInLoop runs on the loop thread");
            loop.stop();
            thread.join();
        }

        {
            int fds[2];
            check(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0, "socketpair created");
            EventLoop loop;
            std::thread thread([&] { loop.run(); });
            std::promise<std::uint32_t> promise;
            std::future<std::uint32_t> future = promise.get_future();
            loop.add(fds[0], kReadable, [&](std::uint32_t events) {
                char byte = 0;
                ::read(fds[0], &byte, 1);
                promise.set_value(events);
            });
            const char ping = 'x';
            check(::write(fds[1], &ping, 1) == 1, "peer write succeeds");
            const std::uint32_t events = future.get();
            check((events & kReadable) != 0, "fd handler sees readable");
            loop.stop();
            thread.join();
            ::close(fds[0]);
            ::close(fds[1]);
        }

        {
            EventLoop loop;
            std::thread thread([&] { loop.run(); });
            std::promise<void> promise;
            std::future<void> future = promise.get_future();
            std::atomic<bool> fired{false};
            loop.runInLoop([&] {
                loop.addInterval(milliseconds(10), [&] {
                    if (!fired.exchange(true)) {
                        promise.set_value();
                    }
                });
            });
            check(future.wait_for(std::chrono::seconds(1)) == std::future_status::ready,
                  "interval timer fires on the loop");
            loop.stop();
            thread.join();
        }

        {
            EventLoopGroup group(3);
            check(group.size() == 3, "group has the requested loops");
            group.start();
            EventLoop* first = group.next();
            EventLoop* second = group.next();
            EventLoop* third = group.next();
            EventLoop* fourth = group.next();
            check(first != second && second != third && first == fourth,
                  "next() round-robins over the loops");

            std::promise<void> promise;
            std::future<void> future = promise.get_future();
            group.next()->runInLoop([&] { promise.set_value(); });
            check(future.wait_for(std::chrono::seconds(1)) == std::future_status::ready,
                  "task runs on a group loop");
        }
    } catch (const std::exception& error) {
        std::cerr << "unexpected exception: " << error.what() << '\n';
        ++failures;
    }

    if (failures != 0) {
        std::cerr << failures.load() << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
