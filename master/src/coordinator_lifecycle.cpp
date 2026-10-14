#include "mapreduce/mapreduce.h"
#include "master/coordinator.h"
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

namespace xmr::master {
    void Coordinator::setLoop(EventLoop* loop) {
        loop_ = loop;
    }

    void Coordinator::setOnShutdown(std::function<void()> onShutdown) {
        onShutdown_ = std::move(onShutdown);
    }

    void Coordinator::setDataAddress(std::string host, std::uint16_t port) {
        dataHost_ = std::move(host);
        dataPort_ = port;
    }

    void Coordinator::start() {
        loop_->addInterval(kTick, [this] {
            tick();
        });
    }

    void Coordinator::onConnect(std::shared_ptr<ConnState> conn) {
        connections_.add(std::move(conn));
    }

    bool Coordinator::hasError() const {
        return !error_.empty();
    }

    std::string Coordinator::error() const {
        return error_;
    }

    std::unordered_map<ConnId, std::shared_ptr<ConnState> >& Coordinator::conns() {
        return connections_.all();
    }

    void Coordinator::fail(std::string reason) {
        error_ = std::move(reason);
        requestShutdown();
    }

    void Coordinator::requeueTask(const std::string& workerId) {
        if (!job_) {
            registry_.reclaim(workerId);
            return;
        }
        const auto task = registry_.reclaim(workerId);
        if (task) {
            retryOrFail(*task);
        }
    }

    void Coordinator::recoverWorker(const std::string& workerId) {
        // woker没有心跳 被master判定下线了 回收派发给它的任务
        requeueTask(workerId);
        if (!job_) {
            return;
        }
        // 该worker上已完成的任务需要回收 master再派发给其他worker
        for (auto it = mapOwner_.begin(); it != mapOwner_.end();) {
            if (it->second == workerId) {
                // 回收任务
                job_->scheduler.invalidate(xmr::TaskKind::Map, it->first);
                it = mapOwner_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // 重发任务 次数用尽才判整个job失败
    void Coordinator::retryOrFail(const xmr::Task& task) {
        if (!job_) {
            return;
        }
        // 重试的attempt作废 重新计时
        auto& starts = task.kind == xmr::TaskKind::Map ? mapStarted_ : reduceStarted_;
        starts.erase(task.id);
        if (!job_->scheduler.retry(task)) {
            job_->scheduler.markFailed(task.kind, task.id, "attempts exhausted");
        }
    }

    // client向master提交job的时候指定了--shutdown语义是任务执行完后关停master
    void Coordinator::requestShutdown() {
        if (!stopping_.exchange(true) && onShutdown_) {
            onShutdown_();
        }
    }
}  // namespace xmr::master
