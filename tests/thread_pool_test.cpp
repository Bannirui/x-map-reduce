#include "net/thread_pool.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <latch>
#include <string>
#include <thread>

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

    try {
        {
            ThreadPool pool(4);
            check(pool.size() == 4, "pool spawns the requested threads");
            std::atomic<int> count{0};
            constexpr int kN = 200;
            std::latch done(kN);
            for (int i = 0; i < kN; ++i) {
                pool.submit([&] {
                    ++count;
                    done.count_down();
                });
            }
            done.wait();
            check(count == kN, "every submitted job runs");
        }

        {
            std::atomic<int> ran{0};
            constexpr int kN = 50;
            {
                ThreadPool pool(3);
                for (int i = 0; i < kN; ++i) {
                    pool.submit([&] {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        ++ran;
                    });
                }
            }
            check(ran == kN, "destructor drains queued jobs");
        }

        {
            bool threw = false;
            try {
                ThreadPool pool(0);
            } catch (const std::exception&) {
                threw = true;
            }
            check(threw, "zero threads is rejected");
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
