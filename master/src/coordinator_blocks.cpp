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
    void Coordinator::tryPlaceBlocks() {
        auto result = blocks_.tryPlace([this](const std::string& workerId) {
            return isLive(workerId);
        });
        if (result) {
            connections_.send(result->client, result->requestId, result->message);
        }
    }

    std::vector<std::vector<std::string> > Coordinator::buildMapBlocks() const {
        return blocks_.buildMapBlocks(pendingInputBlocks_);
    }

    bool Coordinator::isLive(const std::string& workerId) const {
        if (!connections_.hasWorker(workerId)) {
            return false;
        }
        if (!registry_.contains(workerId)) {
            return false;
        }
        return registry_.state(workerId) != xmr::WorkerState::Lost;
    }

    void Coordinator::forgetWorker(const std::string& workerId) {
        blocks_.forgetWorker(workerId);
    }

    void Coordinator::reReplicate() {
        auto commands = blocks_.reReplicate(
            [this](const std::string& workerId) {
                return isLive(workerId);
            },
            [this](const std::string& workerId) -> std::optional<ConnId> {
                return connections_.connOf(workerId);
            });
        for (const auto& command : commands) {
            connections_.send(command.target, 0, command.request);
        }
    }
}  // namespace xmr::master
