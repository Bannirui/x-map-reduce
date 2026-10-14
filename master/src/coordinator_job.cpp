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
    void Coordinator::startJob(ConnId client, const xmr::protocol::Submit& submit,
                               std::vector<std::vector<std::string> > mapBlocks) {
        job_ = std::make_unique<JobState>(submit.job, submit.inputs, submit.reducers, std::move(mapBlocks),
                                          submit.output, client);
        mapOwner_.clear();
        plugins_.reset(submit.pluginHash);
        // 有插件的话分发给还没加载的worker
        if (!plugins_.empty()) {
            for (const auto& entry : connections_.workers()) {
                const auto conn = connections_.get(entry.second);
                if (conn && conn->plugins.count(plugins_.hash()) != 0) {
                    plugins_.markLoaded();
                } else {
                    sendPlugin(entry.second);
                }
            }
        }
    }

    void Coordinator::maybeStartJob() {
        if (!pending_) {
            return;
        }
        if (!uploadDone_) {
            return;
        }
        const std::string& hash = pending_->submit.pluginHash;
        if (!hash.empty() && !plugins_.has(hash)) {
            return;
        }
        const ConnId client = pending_->client;
        const xmr::protocol::Submit submit = pending_->submit;
        std::vector<std::vector<std::string> > mapBlocks = buildMapBlocks();
        pending_.reset();
        uploadDone_ = false;
        startJob(client, submit, std::move(mapBlocks));
    }

    void Coordinator::sendPlugin(ConnId id) {
        // 让worker自己去master数据面拉插件
        if (plugins_.empty() || !job_) {
            return;
        }
        xmr::protocol::FetchPlugin need;
        need.hash = plugins_.hash();
        need.job = job_->name;
        connections_.send(id, 0, need);
    }

    void Coordinator::finishJob() {
        const bool ok = !job_->scheduler.failed();
        // 通知worker本job结束 复位准备接下一个job
        xmr::protocol::JobFinished stop;
        for (const auto& [name, cid] : connections_.workers()) {
            connections_.send(cid, 0, stop);
        }

        xmr::protocol::SubmitResult result;
        if (ok) {
            auto& merged = job_->merged;
            std::stable_sort(merged.begin(), merged.end(), [](const KeyValue& a, const KeyValue& b) {
                return a.first < b.first;
            });
            try {
                WriteKeyValues(job_->output, merged);
                result.statusCode = xmr::protocol::StatusCode::Ok;
                result.output = job_->output;
            } catch (const std::exception& error) {
                result.statusCode = xmr::protocol::StatusCode::Internal;
                result.reason = error.what();
            }
        } else {
            result.statusCode = xmr::protocol::StatusCode::Internal;
            result.reason = job_->scheduler.error();
        }
        if (job_->client != 0) {
            connections_.send(job_->client, 0, result);
        }
        // job结束 回收本次job的block 让worker删掉它
        std::unordered_set<std::string> jobBlocks;
        for (const auto& perInput : job_->mapBlocks) {
            for (const auto& entry : perInput) {
                const auto comma = entry.find(',');
                jobBlocks.insert(comma == std::string::npos ? entry : entry.substr(0, comma));
            }
        }
        for (const auto& workerEntry : blocks_.heldWorkers()) {
            const auto byName = connections_.connOf(workerEntry.first);
            if (!byName) {
                continue;
            }
            for (const auto& blockId : workerEntry.second) {
                if (jobBlocks.count(blockId) == 0) {
                    continue;
                }
                xmr::protocol::DeleteBlock request;
                request.blockId = blockId;
                connections_.send(*byName, 0, request);
            }
        }
        blocks_.clear();
        job_.reset();
    }
}  // namespace xmr::master
