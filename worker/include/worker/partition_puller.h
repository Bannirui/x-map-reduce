#pragma once

#include "mapreduce/types.h"
#include "protocol/protocol.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace xmr::worker {
    // reduce要去哪个worker拉哪个map任务的分区
    struct Location {
        std::size_t mapTask = 0;
        std::string host;
        std::uint16_t port = 0;
    };

    Location parseLocation(const std::string& text);

    std::string pullStream(const std::string& host, std::uint16_t port, protocol::MessageType type,
                           const std::vector<std::uint8_t>& body);

    std::vector<KeyValue> pullPartition(const std::string& host, std::uint16_t port, std::size_t mapTask,
                                        std::size_t partition);
}  // namespace xmr::worker
