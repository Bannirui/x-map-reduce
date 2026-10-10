#pragma once

#include "net/codec/length_field_frame_decoder.h"
#include "net/codec/message_to_byte_encoder.h"
#include "protocol/framing.h"

#include <any>

namespace xmr::protocol {
    class FrameDecoder : public LengthFieldBasedFrameDecoder {
    public:
        FrameDecoder();

    protected:
        bool decode(std::any& out) override;
    };

    class FrameEncoder : public MessageToByteEncoder<Frame> {
    protected:
        void encode(const Frame& frame, ByteBuffer& out) override;
    };
} // namespace xmr::protocol
