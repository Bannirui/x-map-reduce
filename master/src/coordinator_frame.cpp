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
    /// @brief 有人给master发消息了 master对不同的消息处理
    /// @param id 别人用哪个TCP发来的消息
    /// @param frame 发来的消息是什么
    void Coordinator::onFrame(ConnId id, const xmr::protocol::Frame& frame) {
        auto connPtr = connections_.get(id);
        if (!connPtr) {
            return;
        }
        // TCP连接
        ConnState& conn = *connPtr;
        try {
            switch (frame.header.type) {
                case xmr::protocol::MessageType::Hello:
                    onHello(conn, frame);
                    break;
                case xmr::protocol::MessageType::Ping:
                    onPing(conn, frame);
                    break;
                case xmr::protocol::MessageType::Shutdown:
                    requestShutdown();
                    break;
                case xmr::protocol::MessageType::DataAddress:
                    onDataAddress(conn, frame);
                    break;
                case xmr::protocol::MessageType::RequestTask:
                    onRequestTask(conn, frame);
                    break;
                case xmr::protocol::MessageType::Done:
                    onDone(conn, frame);
                    break;
                case xmr::protocol::MessageType::Fail:
                    onFail(conn, frame);
                    break;
                case xmr::protocol::MessageType::Progress:
                    // worker上报任务进度 目前用于观测/推测参考
                    break;
                case xmr::protocol::MessageType::ReduceResult:
                    onReduceResult(frame);
                    break;
                case xmr::protocol::MessageType::Plugin:
                    onPluginChunk(conn, frame);
                    break;
                case xmr::protocol::MessageType::PluginAck:
                    onPluginAck(conn, frame);
                    break;
                case xmr::protocol::MessageType::PullInput:
                    break;
                case xmr::protocol::MessageType::PullPlugin:
                    onPullPlugin(conn, frame);
                    break;
                case xmr::protocol::MessageType::Submit:
                    onSubmit(conn, frame);
                    break;
                case xmr::protocol::MessageType::BlockManifest:
                    onBlockManifest(conn, frame);
                    break;
                case xmr::protocol::MessageType::CommitFile:
                    onCommitFile(conn, frame);
                    break;
                case xmr::protocol::MessageType::BlockReport:
                    onBlockReport(conn, frame);
                    break;
                default:
                    throw std::runtime_error("unexpected control message");
            }
            if (registry_.contains(conn.workerId)) {
                registry_.touch(conn.workerId, std::chrono::steady_clock::now());
            }
        } catch (const std::exception& error) {
            fail(error.what());
        }
    }

    /// @brief 处理 master控制端口有连接断开了
    /// @param id 哪个连接
    void Coordinator::onDisconnect(ConnId id) {
        const std::shared_ptr<ConnState> conn = connections_.get(id);
        if (!conn) {
            return;
        }
        if (!conn->workerId.empty()) {
            if (!plugins_.empty() && conn->plugins.count(plugins_.hash()) != 0) {
                plugins_.unmarkLoaded();
            }
            recoverWorker(conn->workerId);
            registry_.remove(conn->workerId);
            connections_.unbindWorker(conn->workerId);
            blocks_.eraseWorker(conn->workerId);
            reReplicate();
        }
        connections_.remove(id);
    }
}  // namespace xmr::master
