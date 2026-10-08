#include "runtime/worker_registry.h"

#include <atomic>
#include <chrono>
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
    using namespace xmr;
    using std::chrono::milliseconds;

    try {
        const WorkerRegistry::TimePoint t0{};

        {
            bool threw = false;
            try {
                WorkerRegistry registry(milliseconds(0), milliseconds(100));
            } catch (const std::exception&) {
                threw = true;
            }
            check(threw, "non-positive heartbeat timeout is rejected");

            threw = false;
            try {
                WorkerRegistry registry(milliseconds(100), milliseconds(0));
            } catch (const std::exception&) {
                threw = true;
            }
            check(threw, "non-positive task timeout is rejected");
        }

        {
            WorkerRegistry registry(milliseconds(100), milliseconds(1000));
            check(registry.size() == 0, "registry starts empty");
            check(registry.nextTimeoutMs(t0) == -1, "empty registry has no timeout");
            check(registry.add("w1", t0), "adding a worker succeeds");
            check(!registry.add("w1", t0), "adding a duplicate worker fails");
            check(registry.contains("w1"), "added worker is present");
            check(registry.size() == 1, "size tracks workers");
            check(registry.nextTimeoutMs(t0) == 100, "watchdog deadline drives the timeout");
            check(registry.remove("w1"), "removing a worker succeeds");
            check(!registry.remove("w1"), "removing twice fails");
            check(registry.nextTimeoutMs(t0) == -1, "remove cancels the watchdog");
        }

        {
            WorkerRegistry registry(milliseconds(100), milliseconds(1000));
            registry.add("w1", t0);
            check(registry.touch("w1", t0 + milliseconds(90)), "touch refreshes a live worker");
            check(registry.poll(t0 + milliseconds(150)).workers.empty(),
                  "touched worker is not expired at the old deadline");
            const auto expired = registry.poll(t0 + milliseconds(190));
            check(expired.workers.size() == 1 && expired.workers[0] == "w1",
                  "worker expires at the refreshed deadline");
            check(registry.state("w1") == WorkerState::Lost, "expired worker is marked Lost");
            check(!registry.markIdle("w1"), "lost worker cannot go idle");
            check(registry.nextTimeoutMs(t0 + milliseconds(200)) == -1,
                  "no pending watchdog after expiry");
            check(registry.touch("w1", t0 + milliseconds(200)), "activity revives a lost worker");
            check(registry.state("w1") == WorkerState::Registered, "revived worker is Registered");
            check(registry.nextTimeoutMs(t0 + milliseconds(200)) == 100,
                  "revived worker gets a fresh watchdog");
            check(registry.markIdle("w1"), "revived worker can go idle");
            check(registry.remove("w1"), "lost worker can still be removed");
        }

        {
            WorkerRegistry registry(milliseconds(100), milliseconds(1000));
            registry.add("w1", t0);
            registry.add("w2", t0);
            registry.touch("w2", t0 + milliseconds(50));
            check(registry.nextTimeoutMs(t0) == 100, "timeout follows the earliest watchdog");
            check(registry.poll(t0 + milliseconds(100)).workers.size() == 1,
                  "only the stale worker expires first");
            check(registry.state("w1") == WorkerState::Lost, "earliest deadline worker is lost");
            check(registry.state("w2") == WorkerState::Registered, "refreshed worker stays live");
            check(registry.nextTimeoutMs(t0 + milliseconds(100)) == 50,
                  "remaining watchdog keeps contributing");
        }

        {
            WorkerRegistry registry(milliseconds(1000), milliseconds(1000));
            registry.add("w1", t0);
            registry.add("w2", t0);
            check(!registry.hasIdle(), "registered workers are not idle");
            check(!registry.assignNext(Task{}, t0).has_value(), "no idle worker to assign");

            check(registry.markIdle("w1"), "worker can become idle");
            check(registry.hasIdle(), "idle worker is visible");
            Task task;
            task.kind = TaskKind::Map;
            task.id = 7;
            const auto assigned = registry.assignNext(task, t0);
            check(assigned.has_value() && *assigned == "w1", "idle worker is picked for the task");
            check(registry.state("w1") == WorkerState::Busy, "assigned worker is busy");
            check(!registry.hasIdle(), "assigned worker is no longer idle");
            const auto held = registry.taskOf("w1");
            check(held.has_value() && held->id == 7, "assigned task is recorded");

            check(registry.complete("w1"), "completing a task succeeds");
            check(registry.state("w1") == WorkerState::Registered, "completed worker returns to Registered");
            check(!registry.taskOf("w1").has_value(), "task is cleared on completion");
        }

        {
            WorkerRegistry registry(milliseconds(1000), milliseconds(1000));
            registry.add("w1", t0);
            registry.markIdle("w1");
            Task task;
            task.kind = TaskKind::Map;
            task.id = 5;
            const auto assigned = registry.assignNext(task, t0);
            check(assigned.has_value() && *assigned == "w1", "worker takes the task");
            const auto reclaimed = registry.reclaim("w1");
            check(reclaimed.has_value() && reclaimed->id == 5, "reclaim returns the held task");
            check(!registry.taskOf("w1").has_value(), "reclaim clears the held task");
            check(!registry.reclaim("w1").has_value(), "reclaim twice returns nothing");
            check(!registry.reclaim("missing").has_value(), "reclaim of unknown worker returns nothing");
        }

        {
            WorkerRegistry registry(milliseconds(10000), milliseconds(100));
            registry.add("w1", t0);
            registry.markIdle("w1");
            Task task;
            task.kind = TaskKind::Map;
            task.id = 3;
            check(registry.assignNext(task, t0).has_value(), "worker takes the task");
            check(registry.nextTimeoutMs(t0) == 100, "task watchdog contributes a timeout");
            check(registry.poll(t0 + milliseconds(99)).tasks.empty(), "task is not timed out early");
            const auto timedOut = registry.poll(t0 + milliseconds(100));
            check(timedOut.tasks.size() == 1 && timedOut.tasks[0].id == 3,
                  "task timeout is reported for retry");
            check(timedOut.workers.empty(), "task timeout does not mark the worker lost");
            check(registry.state("w1") == WorkerState::Busy, "worker stays Busy after a task timeout");
            check(!registry.taskOf("w1").has_value(), "timed-out task is detached from the worker");
            check(registry.complete("w1"), "late completion still frees the worker");
            check(registry.state("w1") == WorkerState::Registered, "worker returns to Registered");
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
