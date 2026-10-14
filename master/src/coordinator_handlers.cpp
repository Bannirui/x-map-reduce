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
    void Coordinator::onSubmit(ConnState& conn, const xmr::protocol::Frame& frame) {
        // todo master不支持同时多个job在跑
        if (job_ || pending_) {
            xmr::protocol::SubmitAck ack;
            ack.statusCode = xmr::protocol::StatusCode::Unavailable;
            ack.reason = "a job is already running";
            connections_.send(conn.id, frame.header.requestId, ack);
            return;
        }
        // client给master提交的job
        const auto submit = xmr::protocol::Submit::decode(frame.body);
        xmr::protocol::SubmitAck ack;
        // master的数据端口 让client传job插件
        ack.dataHost = dataHost_;
        ack.dataPort = dataPort_;
        if (submit.job.empty() || submit.output.empty() || submit.inputs.empty() || submit.reducers == 0) {
            ack.statusCode = xmr::protocol::StatusCode::InvalidArgument;
            ack.reason = "invalid submit";
            connections_.send(conn.id, frame.header.requestId, ack);
            return;
        }
        // 提交的inputs是DFS路径 从命名空间解析出每个文件的block清单
        std::vector<std::vector<std::string> > inputs;
        inputs.reserve(submit.inputs.size());
        for (const auto& path : submit.inputs) {
            const auto it = namespace_.find(path);
            if (it == namespace_.end()) {
                ack.statusCode = xmr::protocol::StatusCode::NotFound;
                ack.reason = "unknown dfs path: " + path;
                connections_.send(conn.id, frame.header.requestId, ack);
                return;
            }
            inputs.push_back(it->second);
        }
        // 标识master已经开始受理client的job了 现在开始等job插件(如果client带了)
        pending_ = Pending{conn.id, submit, frame.header.requestId};
        pendingInputBlocks_ = std::move(inputs);
        ack.statusCode = xmr::protocol::StatusCode::Ok;
        connections_.send(conn.id, frame.header.requestId, ack);
        maybeStartJob();
    }

    void Coordinator::onPluginChunk(ConnState& conn, const xmr::protocol::Frame& frame) {
        // client在数据端口把插件字节传上来
        const auto chunk = xmr::protocol::Plugin::decode(frame.body);
        if (chunk.offset != conn.uploadBuffer.size()) {
            fail("bad plugin upload");
            return;
        }
        conn.uploadBuffer.insert(conn.uploadBuffer.end(), chunk.payload.begin(), chunk.payload.end());
        if ((frame.header.flags & static_cast<std::uint16_t>(xmr::protocol::Flag::More)) != 0) {
            return;
        }
        if (xmr::protocol::contentHash(conn.uploadBuffer) != chunk.hash) {
            if (pending_ && pending_->submit.pluginHash == chunk.hash) {
                xmr::protocol::SubmitAck error;
                error.statusCode = xmr::protocol::StatusCode::Internal;
                error.reason = "plugin hash mismatch";
                connections_.send(pending_->client, pending_->requestId, error);
                pending_.reset();
            }
            conn.uploadBuffer.clear();
            return;
        }
        plugins_.store(chunk.hash, std::move(conn.uploadBuffer));
        conn.uploadBuffer.clear();
        maybeStartJob();
    }

    void Coordinator::onPullPlugin(ConnState& conn, const xmr::protocol::Frame& frame) {
        // worker从数据端口拉插件
        const auto request = xmr::protocol::PullPlugin::decode(frame.body);
        // 把插件用TCP发给worker
        if (const auto* bytes = plugins_.find(request.hash)) {
            const std::string blob(bytes->begin(), bytes->end());
            connections_.sendBlob(conn.id, frame.header.requestId, blob);
        }
    }

    void Coordinator::onPluginAck(ConnState& conn, const xmr::protocol::Frame& frame) {
        const auto ack = xmr::protocol::PluginAck::decode(frame.body);
        if (plugins_.empty() || ack.hash != plugins_.hash()) {
            return;
        }
        if (!ack.ok) {
            fail("worker failed to load plugin: " + ack.reason);
            return;
        }
        if (conn.plugins.insert(ack.hash).second) {
            plugins_.markLoaded();
        }
    }

    void Coordinator::onHello(ConnState& conn, const xmr::protocol::Frame& frame) {
        conn.workerId = xmr::protocol::Hello::decode(frame.body).workerId;
        registry_.add(conn.workerId, std::chrono::steady_clock::now());
        connections_.bindWorker(conn.workerId, conn.id);
        {
            // 告诉worker master数据端口 供它拉输入/插件
            xmr::protocol::MasterData data;
            data.port = dataPort_;
            connections_.send(conn.id, 0, data);
        }
        if (job_ && !plugins_.empty() && conn.plugins.count(plugins_.hash()) == 0) {
            sendPlugin(conn.id);
        }
    }

    void Coordinator::onDataAddress(ConnState& conn, const xmr::protocol::Frame& frame) {
        // worker上报自己的数据监听端口 供其它worker拉中间结果
        const auto address = xmr::protocol::DataAddress::decode(frame.body);
        blocks_.setWorkerAddress(conn.workerId, conn.host, static_cast<std::uint16_t>(address.port));
        if (address.blockPort != 0) {
            blocks_.setWorkerBlockPort(conn.workerId, static_cast<std::uint16_t>(address.blockPort));
        }
        tryPlaceBlocks();
    }

    void Coordinator::onBlockManifest(ConnState& conn, const xmr::protocol::Frame& frame) {
        // client(dfs put)把切好的block清单报上来 请master按副本策略分配worker
        auto manifest = xmr::protocol::BlockManifest::decode(frame.body);
        blocks_.queueManifest(conn.id, frame.header.requestId, std::move(manifest));
        tryPlaceBlocks();
    }

    void Coordinator::onCommitFile(ConnState& conn, const xmr::protocol::Frame& frame) {
        // client把一个文件对应的block清单提交上来 建立命名空间 path -> [blockId]
        auto commit = xmr::protocol::CommitFile::decode(frame.body);
        xmr::protocol::CommitAck ack;
        if (commit.path.empty() || commit.blocks.empty()) {
            ack.statusCode = xmr::protocol::StatusCode::InvalidArgument;
            ack.reason = "invalid commit";
            connections_.send(conn.id, frame.header.requestId, ack);
            return;
        }
        namespace_[commit.path] = std::move(commit.blocks);
        ack.statusCode = xmr::protocol::StatusCode::Ok;
        connections_.send(conn.id, frame.header.requestId, ack);
    }

    void Coordinator::onBlockReport(ConnState& conn, const xmr::protocol::Frame& frame) {
        // worker上报本地持有的block清单 master据此重建locator
        const auto report = xmr::protocol::BlockReport::decode(frame.body);
        blocks_.onReport(conn.workerId, report.blocks);
    }

    void Coordinator::onRequestTask(ConnState& conn, const xmr::protocol::Frame& frame) {
        // worker告诉master它空闲了 希望master给它派任务
        registry_.markIdle(conn.workerId);
        conn.lastRequest = frame.header.requestId;
    }

    void Coordinator::onDone(ConnState& conn, const xmr::protocol::Frame& frame) {
        if (!job_) {
            return;
        }
        const auto done = xmr::protocol::Done::decode(frame.body);
        const auto held = registry_.taskOf(conn.workerId);
        if (held && held->kind == xmr::taskKind(done.kind) && held->id == done.taskId) {
            const bool won = job_->scheduler.markDone(held->kind, held->id, held->attempt);
            if (won) {
                // 记下耗时 作为推测阈值参考
                auto& starts = held->kind == xmr::TaskKind::Map ? mapStarted_ : reduceStarted_;
                const auto it = starts.find(held->id);
                if (it != starts.end()) {
                    const auto span = std::chrono::steady_clock::now() - it->second;
                    if (!fastestSet_ || span < fastest_) {
                        fastest_ = span;
                        fastestSet_ = true;
                    }
                    starts.erase(it);
                }
                // 记下这个map的结果在哪个worker上 供reduce去拉
                if (held->kind == xmr::TaskKind::Map) {
                    mapOwner_[held->id] = conn.workerId;
                }
                // first-完成wins 取消同任务的其它重复attempt
                cancelLosers(held->kind, held->id, conn.workerId);
            }
        }
        registry_.complete(conn.workerId);
    }

    void Coordinator::onFail(ConnState& conn, const xmr::protocol::Frame& frame) {
        if (!job_) {
            return;
        }
        const auto fail = xmr::protocol::Fail::decode(frame.body);
        const auto held = registry_.taskOf(conn.workerId);
        if (held && held->kind == xmr::taskKind(fail.kind) && held->id == fail.taskId) {
            // 任务失败按重试处理 次数用尽才判死整个job
            retryOrFail(*held);
        }
        registry_.complete(conn.workerId);
    }

    void Coordinator::onReduceResult(const xmr::protocol::Frame& frame) {
        if (!job_) {
            return;
        }
        // worker执行完了reduce 把最终结果给到了master
        const auto result = xmr::protocol::ReduceResult::decode(frame.body);
        auto pairs = xmr::deserializeKeyValues(std::string(result.payload.begin(), result.payload.end()));
        job_->merged.insert(job_->merged.end(), std::make_move_iterator(pairs.begin()),
                            std::make_move_iterator(pairs.end()));
    }

    void Coordinator::onPing(ConnState& conn, const xmr::protocol::Frame& frame) {
        // worker周期心跳 master回Pong
        const auto ping = xmr::protocol::Ping::decode(frame.body);
        xmr::protocol::Pong pong;
        pong.nonce = ping.nonce;
        connections_.send(conn.id, frame.header.requestId, pong);
    }
}  // namespace xmr::master
