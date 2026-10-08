#include"runtime/worker_registry.h"

#include<stdexcept>
#include<utility>

namespace xmr {
    WorkerRegistry::WorkerRegistry(std::chrono::milliseconds heartbeatTimeout)
        : heartbeatTimeout_(heartbeatTimeout) {
        if (heartbeatTimeout_ <= std::chrono::milliseconds::zero()) {
            throw std::runtime_error("heartbeat timeout must be positive");
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
        // 注册列表中移除
        workers_.erase(it);
        return true;
    }

    bool WorkerRegistry::touch(const std::string& id, TimePoint now) {
        const auto it = workers_.find(id);
        if (it == workers_.end() || it->second.state == WorkerState::Lost) {
            return false;
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

    std::optional<std::string> WorkerRegistry::assignNext(const Task& task) {
        for (auto& [id, entry] : workers_) {
            if (entry.state != WorkerState::Idle) {
                // 找到空闲的worker给它派任务
                continue;
            }
            entry.state = WorkerState::Busy;
            entry.task = task;
            return id;
        }
        return std::nullopt;
    }

    bool WorkerRegistry::complete(const std::string& id) {
        const auto it = workers_.find(id);
        if (it == workers_.end() || it->second.state == WorkerState::Lost) {
            return false;
        }
        it->second.state = WorkerState::Registered;
        it->second.task.reset();
        return true;
    }

    std::vector<std::string> WorkerRegistry::pollExpired(TimePoint now) {
        // 因为下面看门狗服务可能调用操作缓存 所以先清空这个缓存
        lost_.clear();
        // 看看现在有没有看门狗到期了执行一下 一旦有看门狗定时任务到期了 就会有worker被判定下线被放到workers里面
        timers_.fire(now);
        std::vector<std::string> expired;
        expired.swap(lost_);
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
        // 判定主观下线
        lost_.push_back(id);
    }
} // namespace xmr
