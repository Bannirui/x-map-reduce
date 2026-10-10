#pragma once

#include "net/buffer.h"
#include "net/channel_handler.h"
#include "net/channel_handler_context.h"

#include <any>
#include <utility>

template <typename T>
class MessageToByteEncoder : public ChannelOutboundHandler {
public:
    void write(ChannelHandlerContext& ctx, std::any& message) override {
        T* typed = std::any_cast<T>(&message);
        if (typed == nullptr) {
            ctx.write(message);
            return;
        }
        ByteBuffer buffer;
        encode(*typed, buffer);
        std::any encoded = std::move(buffer);
        ctx.write(encoded);
    }

protected:
    virtual void encode(const T& message, ByteBuffer& out) = 0;
};
