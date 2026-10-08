#pragma once

#include"protocol/framing.h"
#include"protocol/protocol.h"

#include<cstdint>
#include<vector>

namespace xmr {
    void sendFrame(int fd, protocol::MessageType type, std::uint32_t requestId,
                   const std::vector<std::uint8_t>& body, std::uint16_t flags = 0);

    protocol::Frame receiveFrame(int fd);
} // namespace xmr
