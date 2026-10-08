#include"runtime/wire.h"

#include"net/net.h"

#include<utility>

namespace xmr {
    void sendFrame(int fd, protocol::MessageType type, std::uint32_t requestId,
                   const std::vector<std::uint8_t>& body, std::uint16_t flags) {
        const auto frame = protocol::makeFrame(type, requestId, body, flags);
        net::sendAll(fd, frame.data(), frame.size());
    }

    protocol::Frame receiveFrame(int fd) {
        std::uint8_t headerBytes[protocol::kHeaderSize];
        net::recvAll(fd, headerBytes, sizeof(headerBytes));
        const protocol::Header header = protocol::decodeHeader(headerBytes, sizeof(headerBytes));
        std::vector<std::uint8_t> body(header.payloadLen);
        if (header.payloadLen > 0) {
            net::recvAll(fd, body.data(), body.size());
        }
        return protocol::Frame{header, std::move(body)};
    }
} // namespace xmr
