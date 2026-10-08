#include"runtime/worker_registry.h"

#include<stdexcept>
#include<utility>

namespace xmr {
    WorkerRegistry::WorkerRegistry(std::chrono::milliseconds heartbeatTimeout,
                                   std::chrono::milliseconds taskTimeout)
        : heartbeatTimeout_(heartbeatTimeout), taskTimeout_(taskTimeout) {
        if (heartbeatTimeout_ <= std::chrono::milliseconds::zero()) {
            throw std::runtime_error("heartbeat timeout must be positive");
        }
        if (taskTimeout_ <= std::chrono::milliseconds::zero()) {
            throw std::runtime_error("task timeout must be positive");
        }
    }

    bool WorkerRegistry::add(const std::string& id, TimePoint now) {
        // 看看是不是重复注册
        if (workers_.count(id) != 0) {
            return false;
        }
        // map不存在的时候默认构造对象
        Entry& entry = workers_[id];
        entry.state = WorkerState::Registered;
        arm(id, entry, now);
        return true;
    }

    bool WorkerRegistry::remove(const std::string& id) {
        // 看看自己有没有管理过这个worker
        const auto it = workers_.find(id);
        if (it == workers_.end()) {
            return false;
        }
        // 删除对worker的心跳看门狗
        timers_.cancel(it->second.watchdog);
        timers_.cancel(it->second.taskWatchdog);
        // 注册列表中移除
        workers_.erase(it);
        return true;
    }

    bool WorkerRegistry::touch(const std::string& id, TimePoint now) {
        const auto it = workers_.find(id);
        if (it == workers_.end()) {
            return false;
        }
        // 假死的worker重新冒泡 说明它其实还活着 撤销主观下线判定
        if (it->second.state == WorkerState::Lost) {
            it->second.state = WorkerState::Registered;
        }
        // 刷新心跳看门狗
        arm(id, it->second, now);
        return true;
    }

    bool WorkerRegistry::markIdle(const std::string& id) {
        const auto it = workers_.find(id);
        if (it == workers_.end() || it->second.state == WorkerState::Lost) {
            return false;
        }
        // 空闲状态
        it->second.state = WorkerState::Idle;
        // worker空闲了 也就是没有任务了
        it->second.task.reset();
        return true;
    }

    std::optional<std::string> WorkerRegistry::assignNext(const Task& task, TimePoint now) {
        for (auto& [id, entry] : workers_) {
            if (entry.state != WorkerState::Idle) {
                // 找到空闲的worker给它派任务
                continue;
            }
            const std::string workerId = id;
            entry.state = WorkerState::Busy;
            entry.task = task;
            // master给任务安一个超时看门狗 任务超时了worker还没上报完成 master就要重新派发任务
            entry.taskWatchdog = timers_.addAfter(taskTimeout_, [this, workerId] { onTaskTimeout(workerId); }, now);
            return id;
        }
        return std::nullopt;
    }

    bool WorkerRegistry::complete(const std::string& id) {
        const auto it = workers_.find(id);
        if (it == workers_.end() || it->second.state == WorkerState::Lost) {
            return false;
        }
        timers_.cancel(it->second.taskWatchdog);
        it->second.taskWatchdog = net::TimerQueue::kInvalidId;
        it->second.state = WorkerState::Registered;
        it->second.task.reset();
        return true;
    }

    WorkerRegistry::Expired WorkerRegistry::poll(TimePoint now) {
        lost_.clear();
        timedOutTasks_.clear();
        // 看看现在有没有看门狗到期了执行一下
        timers_.fire(now);
        Expired expired;
        expired.workers.swap(lost_);
        expired.tasks.swap(timedOutTasks_);
        return expired;
    }

    int WorkerRegistry::nextTimeoutMs(TimePoint now) {
        return timers_.timeoutMs(now);
    }

    bool WorkerRegistry::contains(const std::string& id) const {
        return workers_.count(id) != 0;
    }

    WorkerState WorkerRegistry::state(const std::string& id) const {
        const auto it = workers_.find(id);
        if (it == workers_.end()) {
            throw std::runtime_error("unknown worker: " + id);
        }
        return it->second.state;
    }

    std::optional<Task> WorkerRegistry::taskOf(const std::string& id) const {
        const auto it = workers_.find(id);
        if (it == workers_.end()) {
            return std::nullopt;
        }
        return it->second.task;
    }

    std::optional<Task> WorkerRegistry::reclaim(const std::string& id) {
        const auto it = workers_.find(id);
        if (it == workers_.end()) {
            return std::nullopt;
        }
        timers_.cancel(it->second.taskWatchdog);
        it->second.taskWatchdog = net::TimerQueue::kInvalidId;
        std::optional<Task> task = it->second.task;
        it->second.task.reset();
        return task;
    }

    std::size_t WorkerRegistry::size() const {
        return workers_.size();
    }

    bool WorkerRegistry::hasIdle() const {
        for (const auto& [id, entry] : workers_) {
            if (entry.state == WorkerState::Idle) {
                return true;
            }
        }
        return false;
    }

    void WorkerRegistry::arm(const std::string& id, Entry& entry, TimePoint now) {
        // master准备激活对worker心跳的看门狗服务 防止已经有过一个看门狗 先尝试删除再添加
        timers_.cancel(entry.watchdog);
        entry.lastSeen = now;
        entry.watchdog = timers_.addAfter(heartbeatTimeout_, [this, id] { onTimeout(id); }, now);
    }

    void WorkerRegistry::onTimeout(const std::string& id) {
        const auto it = workers_.find(id);
        if (it == workers_.end() || it->second.state == WorkerState::Lost) {
            return;
        }
        // 心跳看门狗到期了 说明整个心跳阈值期间没有收到woker的心跳 判定它下线了
        it->second.state = WorkerState::Lost;
        // 心跳看门狗的定时任务编号抹成哨兵值
        it->second.watchdog = net::TimerQueue::kInvalidId;
        // 任务超时看门狗不在这里处理 任务留在entry里等master回收重发
        timers_.cancel(it->second.taskWatchdog);
        it->second.taskWatchdog = net::TimerQueue::kInvalidId;
        // 判定主观下线
        lost_.push_back(id);
    }

    void WorkerRegistry::onTaskTimeout(const std::string& id) {
        const auto it = workers_.find(id);
        if (it == workers_.end() || !it->second.task) {
            return;
        }
        // 任务超时了 worker还活着 把任务摘出来交给master重发
        timedOutTasks_.push_back(*it->second.task);
        // 这个worker没有按照要求时间完成 已经不配拥有这个任务了
        it->second.task.reset();
        it->second.taskWatchdog = net::TimerQueue::kInvalidId;
    }
} // namespace xmr
