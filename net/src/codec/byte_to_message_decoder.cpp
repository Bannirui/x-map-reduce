#include "net/codec/byte_to_message_decoder.h"
#include "net/channel_handler_context.h"

#include <any>

void ByteToMessageDecoder::channelRead(ChannelHandlerContext& ctx, std::any& message) {
    ByteBuffer* buffer = std::any_cast<ByteBuffer>(&message);
    if (buffer == nullptr) {
        ctx.fireChannelRead(message);
        return;
    }
    cumulation_.append(buffer->data(), buffer->size());
    buffer->consume(buffer->size());
    while (true) {
        std::any decoded;
        if (!decode(decoded)) {
            break;
        }
        ctx.fireChannelRead(decoded);
    }
}
