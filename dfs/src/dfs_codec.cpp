#include"dfs/dfs_codec.h"

#include<xnet/buffer.h>

#include<any>
#include<utility>

DfsFrameDecoder::DfsFrameDecoder()
    : LengthFieldBasedFrameDecoder(kDfsHeaderSize - 4, 4, 0, 0, kDfsMaxPayloadBytes + kDfsHeaderSize) {
}

bool DfsFrameDecoder::decode(std::any& out) {
    std::any raw;
    if (!LengthFieldBasedFrameDecoder::decode(raw)) {
        return false;
    }
    ByteBuffer* buffer = std::any_cast<ByteBuffer>(&raw);
    DfsFrame frame;
    frame.header = decodeDfsHeader(buffer->data(), buffer->size());
    frame.body.assign(buffer->data() + kDfsHeaderSize, buffer->data() + buffer->size());
    out = std::move(frame);
    return true;
}

void DfsFrameEncoder::encode(const DfsFrame& frame, ByteBuffer& out) {
    const std::vector<std::uint8_t> bytes = encodeDfsFrame(frame.header, frame.body);
    out.append(bytes.data(), bytes.size());
}
