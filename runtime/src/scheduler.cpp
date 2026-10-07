#include"runtime/scheduler.h"

#include<stdexcept>
#include<utility>

namespace xmr {
    Scheduler::Scheduler(std::string job, std::vector<std::string> inputs, std::size_t reducers)
        : job_(std::move(job)), inputs_(std::move(inputs)), reducers_(reducers) {
        if (reducers_ == 0) {
            throw std::runtime_error("reducers must be > 0");
        }
        if (inputs_.empty()) {
            throw std::runtime_error("at least one input is required");
        }
    }

    std::optional<Task> Scheduler::takeTask() {
        // map阶段派发map任务
        if (phase_ == Phase::Map && nextMap_ < inputs_.size()) {
            Task task;
            task.kind = TaskKind::Map;
            task.id = nextMap_;
            task.job = job_;
            task.reducers = reducers_;
            task.maps = inputs_.size();
            task.input = inputs_[nextMap_];
            ++nextMap_;
            return task;
        }
        // reduce阶段派发reduce任务
        if (phase_ == Phase::Reduce && nextReduce_ < reducers_) {
            Task task;
            task.kind = TaskKind::Reduce;
            task.id = nextReduce_;
            task.job = job_;
            task.reducers = reducers_;
            task.maps = inputs_.size();
            ++nextReduce_;
            return task;
        }
        /**
         * 没有任务可以派发
         * 1 当前阶段都派发出去了 比如map阶段map任务都派发出去了 reduce阶段reduce任务都派发出去了
         * 2 整个任务执行成功了
         * 3 整个任务执行失败了
         * 4 处在reduce阶段和map阶段的屏障期间 reduce要等所有map执行结束
         */
        return std::nullopt;
    }

    void Scheduler::markDone(TaskKind kind) {
        if (kind == TaskKind::Map) {
            ++mapDone_;
        } else {
            ++reduceDone_;
        }
        advanceIfPhaseComplete();
    }

    void Scheduler::markFailed(TaskKind kind, std::size_t id, std::string reason) {
        phase_ = Phase::Failed;
        error_ = std::string(kind == TaskKind::Map ? "map" : "reduce")
                 + " task " + std::to_string(id) + " failed: " + std::move(reason);
    }

    void Scheduler::advanceIfPhaseComplete() {
        if (phase_ == Phase::Map && mapDone_ == inputs_.size()) {
            phase_ = Phase::Reduce;
        }
        if (phase_ == Phase::Reduce && reduceDone_ == reducers_) {
            phase_ = Phase::Done;
        }
    }
} // namespace xmr