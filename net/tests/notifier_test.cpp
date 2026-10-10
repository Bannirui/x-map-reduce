#include "net/event_loop.h"
#include "net/notifier.h"

#include <atomic>
#include <iostream>
#include <string>

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

    try {
        Notifier notifier;
        Poller poller;
        poller.add(notifier.fd(), kReadable);

        check(poller.wait(0).empty(), "no event before notify");

        notifier.notify();
        const auto events = poller.wait(1000);
        check(events.size() == 1 && events[0].fd == notifier.fd(), "notify wakes the poller");

        notifier.drain();
        check(poller.wait(0).empty(), "drained notifier is quiet");

        notifier.notify();
        notifier.notify();
        notifier.drain();
        check(poller.wait(0).empty(), "multiple notifies coalesce into one drain");
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
