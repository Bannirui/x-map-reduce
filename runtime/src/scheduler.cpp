#include"runtime/scheduler.h"

#include<stdexcept>
#include<utility>

namespace xmr {
    Scheduler::Scheduler(std::string job, std::vector<std::string> inputs, std::size_t reducers,
                         std::uint32_t maxAttempts)
        : job_(std::move(job)), inputs_(std::move(inputs)), reducers_(reducers), maxAttempts_(maxAttempts) {
        if (reducers_ == 0) {
            throw std::runtime_error("reducers must be > 0");
        }
        if (inputs_.empty()) {
            throw std::runtime_error("at least one input is required");
        }
        if (maxAttempts_ == 0) {
            throw std::runtime_error("maxAttempts must be > 0");
        }
        mapEntries_.resize(inputs_.size());
        reduceEntries_.resize(reducers_);
        for (std::size_t id = 0; id < inputs_.size(); ++id) {
            mapPending_.push_back(id);
        }
        for (std::size_t id = 0; id < reducers_; ++id) {
            reducePending_.push_back(id);
        }
    }

    std::optional<Task> Scheduler::takeTask() {
        // map阶段派发map任务
        if (phase_ == Phase::Map && !mapPending_.empty()) {
            const std::size_t id = mapPending_.front();
            mapPending_.pop_front();
            Entry& entry = mapEntries_[id];
            ++entry.attempt;
            entry.state = Entry::State::InFlight;
            return makeTask(TaskKind::Map, id, entry.attempt);
        }
        // reduce阶段派发reduce任务
        if (phase_ == Phase::Reduce && !reducePending_.empty()) {
            const std::size_t id = reducePending_.front();
            reducePending_.pop_front();
            Entry& entry = reduceEntries_[id];
            ++entry.attempt;
            entry.state = Entry::State::InFlight;
            return makeTask(TaskKind::Reduce, id, entry.attempt);
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

    bool Scheduler::retry(const Task& task) {
        // 找到这个任务
        Entry& entry = entryOf(task.kind, task.id);
        if (entry.state != Entry::State::InFlight || task.attempt != entry.attempt || entry.attempt >= maxAttempts_) {
            return false;
        }
        // 状态更新 可以下一次重新派发
        entry.state = Entry::State::Pending;
        if (task.kind == TaskKind::Map) {
            mapPending_.push_back(task.id);
        } else {
            reducePending_.push_back(task.id);
        }
        return true;
    }

    bool Scheduler::markDone(TaskKind kind, std::size_t id, std::uint32_t attempt) {
        // 找到任务
        Entry& entry = entryOf(kind, id);
        if (entry.state != Entry::State::InFlight || attempt != entry.attempt) {
            return false;
        }
        // 标记任务状态
        entry.state = Entry::State::Done;
        if (kind == TaskKind::Map) {
            ++mapDone_;
        } else {
            ++reduceDone_;
        }
        advanceIfPhaseComplete();
        return true;
    }

    void Scheduler::markFailed(TaskKind kind, std::size_t id, std::string reason) {
        phase_ = Phase::Failed;
        error_ = std::string(kind == TaskKind::Map ? "map" : "reduce")
                 + " task " + std::to_string(id) + " failed: " + std::move(reason);
    }

    bool Scheduler::invalidate(TaskKind kind, std::size_t id) {
        Entry& entry = entryOf(kind, id);
        if (entry.state != Entry::State::Done) {
            return false;
        }
        // 已完成的任务作废 退回待派发 重新执行(比如它的输出所在worker死了)
        entry.state = Entry::State::Pending;
        if (kind == TaskKind::Map) {
            --mapDone_;
            mapPending_.push_back(id);
            if (phase_ == Phase::Reduce) {
                phase_ = Phase::Map;
            }
        } else {
            --reduceDone_;
            reducePending_.push_back(id);
            if (phase_ == Phase::Done) {
                phase_ = Phase::Reduce;
            }
        }
        return true;
    }

    std::uint32_t Scheduler::attemptOf(TaskKind kind, std::size_t id) const {        return entryOf(kind, id).attempt;
    }

    Scheduler::Entry& Scheduler::entryOf(TaskKind kind, std::size_t id) {
        auto& entries = kind == TaskKind::Map ? mapEntries_ : reduceEntries_;
        if (id >= entries.size()) {
            throw std::runtime_error("task id out of range");
        }
        return entries[id];
    }

    const Scheduler::Entry& Scheduler::entryOf(TaskKind kind, std::size_t id) const {
        const auto& entries = kind == TaskKind::Map ? mapEntries_ : reduceEntries_;
        if (id >= entries.size()) {
            throw std::runtime_error("task id out of range");
        }
        return entries[id];
    }

    Task Scheduler::makeTask(TaskKind kind, std::size_t id, std::uint32_t attempt) const {
        Task task;
        task.kind = kind;
        task.id = id;
        task.attempt = attempt;
        task.job = job_;
        task.reducers = reducers_;
        task.maps = inputs_.size();
        if (kind == TaskKind::Map) {
            task.input = inputs_[id];
        }
        return task;
    }

    void Scheduler::advanceIfPhaseComplete() {
        if (phase_ == Phase::Map && mapDone_ == inputs_.size()) {
            // map任务全部执行完滚动到reduce阶段
            phase_ = Phase::Reduce;
        }
        if (phase_ == Phase::Reduce && reduceDone_ == reducers_) {
            // 所有任务都执行完了
            phase_ = Phase::Done;
        }
    }
} // namespace xmr
