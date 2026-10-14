#include "mapreduce/mapreduce.h"
#include "protocol/frame_codec.h"
#include "protocol/framing.h"
#include "protocol/messages.h"
#include "runtime/endpoint.h"
#include "runtime/exit_code.h"
#include "runtime/scheduler.h"
#include "runtime/task.h"
#include "runtime/task_codec.h"
#include "runtime/worker_registry.h"

#include <xnet/bootstrap.h>
#include <xnet/channel.h>
#include <xnet/channel_handler.h>
#include <xnet/channel_handler_context.h>
#include <xnet/channel_pipeline.h>
#include <xnet/event_loop.h>
#include <xnet/event_loop_group.h>
#include <xnet/net.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <fstream>
#include <future>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "master/coordinator.h"

namespace xmr::master {
    void Coordinator::tick() {
        if (stopping_.load()) {
            return;
        }
        tryPlaceBlocks();
        const auto now = std::chrono::steady_clock::now();
        const auto expired = registry_.poll(now);
        // 没有按时发送心跳的worker
        for (const auto& workerId : expired.workers) {
            // 这些worker被master判定挂了 要把worker已经完成的任务回收过来重新派发给其他worker
            forgetWorker(workerId);
            recoverWorker(workerId);
        }
        for (const auto& task : expired.tasks) {
            // 任务超时了 重新派发
            retryOrFail(task);
        }
        // 副本数不够的block从存活副本重复制
        reReplicate();
        if (!job_) {
            return;
        }
        // 当前master的job在跑
        dispatch();
        speculate();
        if (job_->scheduler.finished() || job_->scheduler.failed()) {
            finishJob();
        }
    }

    std::vector<std::string> Coordinator::fetchPlan() const {
        std::vector<std::string> locations;
        for (std::size_t mapTask = 0; mapTask < job_->maps; ++mapTask) {
            const auto owner = mapOwner_.find(mapTask);
            if (owner == mapOwner_.end()) {
                continue;
            }
            const auto address = blocks_.addressOf(owner->second);
            if (!address) {
                continue;
            }
            locations.push_back(std::to_string(mapTask) + "," + address->first + "," +
                                std::to_string(address->second));
        }
        return locations;
    }

    void Coordinator::sendTask(const std::string& workerId, const xmr::Task& task) {
        const auto cid = connections_.connOf(workerId);
        if (!cid) {
            return;
        }
        const auto it = connections_.get(*cid);
        if (!it) {
            return;
        }
        // master向worker派发任务 用控制端口的TCP发个消息
        auto message = xmr::toTaskAssignment(task);
        if (task.kind == xmr::TaskKind::Reduce) {
            // map中间结果按照R分区了 所以如果派发的是reduce任务 还得告诉它去哪些worker上的什么地方接数据
            // 也就是woker的数据端口
            message.locations = fetchPlan();
        } else if (task.id < job_->mapBlocks.size()) {
            message.blocks = job_->mapBlocks[task.id];
        }
        connections_.send(*cid, it->lastRequest, message);
    }

    std::optional<std::string> Coordinator::assignLocal(const xmr::Task& task, TimePoint now) {
        if (task.id >= job_->mapBlocks.size()) {
            return std::nullopt;
        }
        std::unordered_map<std::string, int> localCount;
        for (const auto& entry : job_->mapBlocks[task.id]) {
            const auto comma = entry.find(',');
            const std::string blockId = comma == std::string::npos ? entry : entry.substr(0, comma);
            for (const auto& holder : blocks_.heldWorkers()) {
                if (holder.second.count(blockId) != 0 && registry_.isIdle(holder.first)) {
                    ++localCount[holder.first];
                }
            }
        }
        std::string best;
        int bestCount = 0;
        for (const auto& entry : localCount) {
            if (best.empty() || entry.second > bestCount || (entry.second == bestCount && entry.first < best)) {
                best = entry.first;
                bestCount = entry.second;
            }
        }
        if (best.empty()) {
            return std::nullopt;
        }
        return registry_.assignTo(best, task, now);
    }

    void Coordinator::dispatch() {
        if (connections_.workerCount() == 0) {
            return;
        }
        // 用当前已连接的worker：等它们都就绪再派
        if (!plugins_.empty() && plugins_.ready() < connections_.workerCount()) {
            return;
        }
        // 找到在等任务的worker
        while (registry_.hasIdle()) {
            // 创建个任务
            auto task = job_->scheduler.takeTask();
            if (!task) {
                break;
            }
            const auto now = std::chrono::steady_clock::now();
            // 任务执行倒计时 worker得在限定时间内执行完
            std::optional<std::string> workerId;
            if (task->kind == xmr::TaskKind::Map) {
                workerId = assignLocal(*task, now);
            }
            if (!workerId) {
                workerId = registry_.assignNext(*task, now);
            }
            if (!workerId) {
                break;
            }
            // 记录第一次派发时间 用于推测长尾任务
            auto& starts = task->kind == xmr::TaskKind::Map ? mapStarted_ : reduceStarted_;
            starts.emplace(task->id, now);
            // 给worker派发任务
            sendTask(*workerId, *task);
        }
    }

    void Coordinator::speculate() {
        if (!job_ || !registry_.hasIdle()) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        std::chrono::steady_clock::duration threshold = kMinSpeculate;
        if (fastestSet_) {
            threshold = std::max(threshold, fastest_ * 2);
        }
        for (const auto& entry : connections_.workers()) {
            const auto task = registry_.taskOf(entry.first);
            if (!task) {
                continue;
            }
            const auto& starts = task->kind == xmr::TaskKind::Map ? mapStarted_ : reduceStarted_;
            const auto it = starts.find(task->id);
            if (it == starts.end() || now - it->second < threshold) {
                continue;
            }
            const auto extra = job_->scheduler.speculate(task->kind, task->id);
            if (!extra) {
                continue;
            }
            const auto workerId = registry_.assignNext(*extra, now);
            if (!workerId) {
                break;
            }
            sendTask(*workerId, *extra);
            break;
        }
    }

    void Coordinator::cancelLosers(xmr::TaskKind kind, std::size_t id, const std::string& winner) {
        xmr::protocol::Cancel cancel;
        cancel.kind = xmr::wireKind(kind);
        cancel.taskId = id;
        for (const auto& entry : connections_.workers()) {
            if (entry.first == winner) {
                continue;
            }
            const auto task = registry_.taskOf(entry.first);
            if (task && task->kind == kind && task->id == id) {
                connections_.send(entry.second, 0, cancel);
            }
        }
    }

    /// @brief master用控制端口给TCP回复消息 可能是传给了client 也可能是传给了worker
    /// @param id 控制端口的TCP
    /// @param type 消息类型
    /// @param requestId 回复的是哪个请求
    /// @param body 消息内容
    /// @param flags
}  // namespace xmr::master
