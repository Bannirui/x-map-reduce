#include"protocol/framing.h"

#include"net/buffer.h"

namespace xmr::protocol {
    std::size_t FrameDecoder::buffered() const noexcept {
        return buffer_.size();
    }

    std::optional<Frame> FrameDecoder::next() {
        if (buffer_.size() < kHeaderSize) {
            return std::nullopt;
        }
        // 协议头
        const Header header = decodeHeader(buffer_.data(), buffer_.size());
        const std::size_t total = kHeaderSize + static_cast<std::size_t>(header.payloadLen);
        if (buffer_.size() < total) {
            return std::nullopt;
        }
        buffer_.consume(kHeaderSize);
        Frame frame;
        frame.header = header;
        frame.body = buffer_.take(header.payloadLen);
        return frame;
    }
} // namespace xmr::protocol
