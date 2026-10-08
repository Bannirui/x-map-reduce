#pragma once

#include"protocol/protocol.h"

#include<cstddef>
#include<optional>
#include<vector>

namespace xmr::net {
    // 前向声明
    class ByteBuffer;
} // namespace xmr::net

namespace xmr::protocol {
    struct Frame {
        Header header;
        std::vector<std::uint8_t> body;
    };

    class FrameDecoder {
    public:
        explicit FrameDecoder(net::ByteBuffer& buffer) : buffer_(buffer) {
        }

        std::optional<Frame> next();

        std::size_t buffered() const noexcept;

    private:
        net::ByteBuffer& buffer_;
    };
} // namespace xmr::protocol
