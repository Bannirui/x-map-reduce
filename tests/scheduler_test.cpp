#include "runtime/scheduler.h"

#include <atomic>
#include <exception>
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

    try {
        {
            bool threw = false;
            try {
                Scheduler scheduler("job", {"a"}, 0);
            } catch (const std::exception&) {
                threw = true;
            }
            check(threw, "reducers=0 is rejected");

            threw = false;
            try {
                Scheduler scheduler("job", {}, 1);
            } catch (const std::exception&) {
                threw = true;
            }
            check(threw, "empty inputs is rejected");

            threw = false;
            try {
                Scheduler scheduler("job", {"a"}, 1, 0);
            } catch (const std::exception&) {
                threw = true;
            }
            check(threw, "maxAttempts=0 is rejected");
        }

        {
            Scheduler scheduler("job", {"in"}, 1, 3);
            const auto map = scheduler.takeTask();
            check(map && map->kind == TaskKind::Map && map->id == 0 && map->attempt == 1,
                  "first map task is id 0 attempt 1");
            check(!scheduler.takeTask().has_value(), "no more map tasks in map phase");
            check(scheduler.phase() == Phase::Map, "phase stays Map until all maps done");
            check(!scheduler.markDone(TaskKind::Map, 0, 2), "wrong attempt is not counted");
            check(scheduler.markDone(TaskKind::Map, 0, 1), "correct attempt completes the map");
            check(!scheduler.markDone(TaskKind::Map, 0, 1), "duplicate done is ignored");
            check(scheduler.phase() == Phase::Reduce, "phase advances to Reduce after the barrier");
            const auto reduce = scheduler.takeTask();
            check(reduce && reduce->kind == TaskKind::Reduce && reduce->id == 0 && reduce->attempt == 1,
                  "reduce task is issued after the barrier");
            check(scheduler.markDone(TaskKind::Reduce, 0, 1), "reduce done is accepted");
            check(scheduler.finished(), "job finishes when all reduce tasks are done");
            check(!scheduler.failed(), "finished job is not failed");
        }

        {
            Scheduler scheduler("job", {"a", "b"}, 1, 3);
            const auto map0 = scheduler.takeTask();
            check(map0 && map0->id == 0 && map0->attempt == 1, "map0 attempt 1");
            check(scheduler.retry(*map0), "retry puts an in-flight task back");
            check(!scheduler.retry(*map0), "retry is idempotent for one attempt");
            const auto map1 = scheduler.takeTask();
            check(map1 && map1->id == 1 && map1->attempt == 1, "map1 is issued before the retried map0");
            const auto map0b = scheduler.takeTask();
            check(map0b && map0b->id == 0 && map0b->attempt == 2, "retried map0 is issued as attempt 2");
            check(!scheduler.markDone(TaskKind::Map, 0, 1), "stale attempt 1 result is ignored");
            check(scheduler.markDone(TaskKind::Map, 0, 2), "attempt 2 result is accepted");
            check(scheduler.markDone(TaskKind::Map, 1, 1), "map1 result is accepted");
            check(scheduler.phase() == Phase::Reduce, "reduce begins after both maps complete");
        }

        {
            Scheduler scheduler("job", {"a"}, 1, 2);
            const auto first = scheduler.takeTask();
            check(first && first->attempt == 1, "attempt 1 issued");
            check(scheduler.retry(*first), "retry within the attempt budget succeeds");
            const auto second = scheduler.takeTask();
            check(second && second->attempt == 2, "attempt 2 issued");
            check(!scheduler.retry(*second), "retry past maxAttempts is refused");
        }

        {
            Scheduler scheduler("job", {"a"}, 1, 3);
            check(scheduler.attemptOf(TaskKind::Map, 0) == 0, "pending task has attempt 0");
            const auto task = scheduler.takeTask();
            check(scheduler.attemptOf(TaskKind::Map, 0) == 1, "in-flight task has attempt 1");
            check(task->attempt == scheduler.attemptOf(TaskKind::Map, 0), "attemptOf matches the issued task");
        }

        {
            Scheduler scheduler("job", {"a"}, 1, 3);
            const auto map = scheduler.takeTask();
            check(scheduler.markDone(TaskKind::Map, 0, 1), "map completes");
            check(scheduler.phase() == Phase::Reduce, "phase advances after the barrier");
            check(scheduler.invalidate(TaskKind::Map, 0), "a completed map can be invalidated");
            check(scheduler.phase() == Phase::Map, "phase falls back to Map after invalidation");
            const auto redo = scheduler.takeTask();
            check(redo && redo->kind == TaskKind::Map && redo->id == 0 && redo->attempt == 2,
                  "invalidated map is re-issued as attempt 2");
            check(!scheduler.invalidate(TaskKind::Map, 0), "invalidating a non-done map fails");
            check(scheduler.markDone(TaskKind::Map, 0, 2), "re-run map completes");
            check(scheduler.phase() == Phase::Reduce, "phase advances again");
        }

        {
            Scheduler scheduler("job", {"a"}, 1, 3);
            const auto first = scheduler.takeTask();
            check(first && first->attempt == 1, "attempt 1 issued");
            const auto second = scheduler.speculate(TaskKind::Map, 0);
            check(second && second->attempt == 2, "speculative attempt 2 issued");
            const auto third = scheduler.speculate(TaskKind::Map, 0);
            check(third && third->attempt == 3, "speculative attempt 3 issued");
            check(!scheduler.speculate(TaskKind::Map, 0).has_value(), "no speculation past maxAttempts");
            check(scheduler.markDone(TaskKind::Map, 0, 2), "first completion (attempt 2) wins");
            check(!scheduler.markDone(TaskKind::Map, 0, 1), "losing attempt 1 is rejected");
            check(!scheduler.markDone(TaskKind::Map, 0, 3), "losing attempt 3 is rejected");
            check(scheduler.phase() == Phase::Reduce, "phase advances after the winning attempt");
        }

        {
            Scheduler scheduler("job", {"a"}, 1, 4);
            const auto first = scheduler.takeTask();
            check(scheduler.speculate(TaskKind::Map, 0).has_value(), "speculation adds an in-flight attempt");
            check(scheduler.retry(*first), "retry removes one in-flight attempt");
            check(!scheduler.retry(*first), "retrying the same attempt again fails");
            const auto again = scheduler.takeTask();
            check(again && again->id == 0 && again->attempt == 3,
                  "retried map is re-issued as attempt 3");
        }

        {
            Scheduler scheduler("job", {"a"}, 1, 3);
            const auto task = scheduler.takeTask();
            scheduler.markFailed(TaskKind::Map, task->id, "boom");
            check(scheduler.failed(), "markFailed sets the Failed phase");
            check(scheduler.error().find("boom") != std::string::npos, "failure reason is recorded");
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
