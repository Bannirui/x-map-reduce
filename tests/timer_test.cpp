#include "net/timer.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

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
    using namespace xmr::net;
    using Clock = TimerQueue::Clock;
    using std::chrono::milliseconds;

    try {
        const Clock::time_point t0{};

        {
            TimerQueue timers;
            check(timers.timeoutMs(t0) == -1, "empty queue has no timeout");
            timers.fire(t0);
            check(timers.empty(), "firing an empty queue is a no-op");
        }

        {
            TimerQueue timers;
            int fired = 0;
            timers.addAfter(milliseconds(100), [&] { ++fired; }, t0);
            check(timers.timeoutMs(t0) == 100, "timeout equals the only deadline");
            timers.fire(t0 + milliseconds(99));
            check(fired == 0, "one-shot does not fire early");
            timers.fire(t0 + milliseconds(100));
            check(fired == 1, "one-shot fires at its deadline");
            timers.fire(t0 + milliseconds(200));
            check(fired == 1, "one-shot fires only once");
            check(timers.timeoutMs(t0 + milliseconds(200)) == -1, "queue drains after one-shot");
        }

        {
            TimerQueue timers;
            std::vector<int> order;
            timers.addAfter(milliseconds(10), [&] { order.push_back(1); }, t0);
            timers.addAfter(milliseconds(5), [&] { order.push_back(2); }, t0);
            timers.fire(t0 + milliseconds(10));
            check((order == std::vector<int>{2, 1}), "timers fire in deadline order");
        }

        {
            TimerQueue timers;
            int count = 0;
            const auto id = timers.addInterval(milliseconds(10), [&] { ++count; }, t0);
            timers.fire(t0 + milliseconds(10));
            check(count == 1, "interval timer fires at first deadline");
            timers.fire(t0 + milliseconds(15));
            check(count == 1, "interval timer waits for the next period");
            timers.fire(t0 + milliseconds(20));
            check(count == 2, "interval timer fires again");
            check(timers.cancel(id), "cancelling a live timer succeeds");
            timers.fire(t0 + milliseconds(100));
            check(count == 2, "cancelled interval timer stops firing");
            check(!timers.cancel(id), "cancelling twice reports failure");
        }

        {
            TimerQueue timers;
            int fired = 0;
            const auto id = timers.addAfter(milliseconds(5), [&] { ++fired; }, t0);
            check(timers.timeoutMs(t0) == 5, "pending one-shot contributes a timeout");
            timers.cancel(id);
            check(timers.timeoutMs(t0) == -1, "cancelled timer stops contributing a timeout");
            timers.fire(t0 + milliseconds(50));
            check(fired == 0, "cancelled one-shot never fires");
        }

        {
            TimerQueue timers;
            int first = 0;
            int second = 0;
            timers.addAfter(milliseconds(5), [&] {
                ++first;
                timers.addAfter(milliseconds(5), [&] { ++second; }, t0 + milliseconds(5));
            }, t0);
            timers.fire(t0 + milliseconds(5));
            check(first == 1 && second == 0, "callback can schedule a new timer");
            timers.fire(t0 + milliseconds(10));
            check(second == 1, "timer scheduled from a callback fires later");
        }

        {
            TimerQueue timers;
            int count = 0;
            const auto id = timers.addInterval(milliseconds(100), [&] { ++count; }, t0);
            timers.addAfter(milliseconds(10), [&] { timers.cancel(id); }, t0);
            timers.fire(t0 + milliseconds(10));
            check(count == 0, "one-shot cancels an interval timer before it fires");
            timers.fire(t0 + milliseconds(200));
            check(count == 0, "interval cancelled by another callback never fires");
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
