#include"runtime/scheduler.h"

#include<algorithm>
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
            ++entry.nextAttempt;
            entry.inFlight.push_back(entry.nextAttempt);
            return makeTask(TaskKind::Map, id, entry.nextAttempt);
        }
        // reduce阶段派发reduce任务
        if (phase_ == Phase::Reduce && !reducePending_.empty()) {
            const std::size_t id = reducePending_.front();
            reducePending_.pop_front();
            Entry& entry = reduceEntries_[id];
            ++entry.nextAttempt;
            entry.inFlight.push_back(entry.nextAttempt);
            return makeTask(TaskKind::Reduce, id, entry.nextAttempt);
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

    std::optional<Task> Scheduler::speculate(TaskKind kind, std::size_t id) {
        Entry& entry = entryOf(kind, id);
        // 必须在跑、没完成、还有尝试次数
        if (entry.done || entry.inFlight.empty() || entry.nextAttempt >= maxAttempts_) {
            return std::nullopt;
        }
        ++entry.nextAttempt;
        entry.inFlight.push_back(entry.nextAttempt);
        return makeTask(kind, id, entry.nextAttempt);
    }

    bool Scheduler::retry(const Task& task) {
        // 找到这个任务
        Entry& entry = entryOf(task.kind, task.id);
        if (entry.done || entry.nextAttempt >= maxAttempts_) {
            return false;
        }
        const auto it = std::find(entry.inFlight.begin(), entry.inFlight.end(), task.attempt);
        if (it == entry.inFlight.end()) {
            return false;
        }
        // 这个attempt作废 任务放回待派发
        entry.inFlight.erase(it);
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
        if (entry.done) {
            return false;
        }
        const auto it = std::find(entry.inFlight.begin(), entry.inFlight.end(), attempt);
        if (it == entry.inFlight.end()) {
            return false;
        }
        // first-完成wins 其它in-flight的attempt都作废
        entry.done = true;
        entry.inFlight.clear();
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
        // master曾经派发出去的任务
        Entry& entry = entryOf(kind, id);
        if (!entry.done) {
            return false;
        }
        // worker心跳丢了 master判定worker挂了 曾经派发给它的任务都要作废准备重新派发
        entry.done = false;
        entry.inFlight.clear();
        /**
         * master发现worker挂了和worker真正挂了 中间可能隔着一段时间的 也就是说master可能发现得不及时 稳妥的做法是阶段回退
         * 现在是reduce阶段就回退到map阶段
         * 现在是job完结阶段就回退到reduce阶段
         * 情愿把时间多往回退一点 无非就是woker可能多执行点任务 保证幂等就可以
         */
        if (kind == TaskKind::Map) {
            // 回收的是map任务
            --mapDone_;
            mapPending_.push_back(id);
            if (phase_ == Phase::Reduce) {
                phase_ = Phase::Map;
            }
        } else {
            // 回收的是reduce任务
            --reduceDone_;
            reducePending_.push_back(id);
            if (phase_ == Phase::Done) {
                phase_ = Phase::Reduce;
            }
        }
        return true;
    }

    std::uint32_t Scheduler::attemptOf(TaskKind kind, std::size_t id) const {
        return entryOf(kind, id).nextAttempt;
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
