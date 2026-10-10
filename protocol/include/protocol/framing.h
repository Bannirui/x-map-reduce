#pragma once

#include"protocol/protocol.h"

#include<cstddef>
#include<optional>
#include<vector>

// 前向声明
class ByteBuffer;

namespace xmr::protocol {
    // 网络传输的消息
    struct Frame {
        // 消息头
        Header header;
        // 消息
        std::vector<std::uint8_t> body;
    };

    class FrameParser {
    public:
        /// @param buffer 用TCP传过来的数据
        explicit FrameParser(ByteBuffer& buffer) : buffer_(buffer) {
        }

        std::optional<Frame> next();

        std::size_t buffered() const noexcept;

    private:
        // 用TCP传输过来的数据 它接的是TCP流式数据 会源源不断进来数据 拆包
        ByteBuffer& buffer_;
    };
} // namespace xmr::protocol
