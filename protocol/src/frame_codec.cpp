#include "protocol/frame_codec.h"

#include "net/buffer.h"
#include "protocol/protocol.h"

#include <any>
#include <cstdint>
#include <utility>
#include <vector>

namespace xmr::protocol {
    FrameDecoder::FrameDecoder()
        : LengthFieldBasedFrameDecoder(kHeaderSize - 4, 4, 0, 0, kMaxPayloadBytes + kHeaderSize) {
    }

    bool FrameDecoder::decode(std::any& out) {
        std::any raw;
        if (!LengthFieldBasedFrameDecoder::decode(raw)) {
            return false;
        }
        ByteBuffer* buffer = std::any_cast<ByteBuffer>(&raw);
        Frame frame;
        frame.header = decodeHeader(buffer->data(), buffer->size());
        frame.body.assign(buffer->data() + kHeaderSize, buffer->data() + buffer->size());
        out = std::move(frame);
        return true;
    }

    void FrameEncoder::encode(const Frame& frame, ByteBuffer& out) {
        const std::vector<std::uint8_t> bytes = encodeFrame(frame.header, frame.body);
        out.append(bytes.data(), bytes.size());
    }
} // namespace xmr::protocol
