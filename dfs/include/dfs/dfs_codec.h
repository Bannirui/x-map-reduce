#pragma once

#include "dfs/dfs_messages.h"

#include <xnet/codec/length_field_frame_decoder.h>
#include <xnet/codec/message_to_byte_encoder.h>

#include <any>

class DfsFrameDecoder : public LengthFieldBasedFrameDecoder {
public:
    DfsFrameDecoder();

protected:
    bool decode(std::any& out) override;
};

class DfsFrameEncoder : public MessageToByteEncoder<DfsFrame> {
protected:
    void encode(const DfsFrame& frame, ByteBuffer& out) override;
};
