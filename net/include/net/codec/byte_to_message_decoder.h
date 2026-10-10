#pragma once

#include "net/buffer.h"
#include "net/channel_handler.h"

#include <any>

class ChannelHandlerContext;

class ByteToMessageDecoder : public ChannelInboundHandler {
public:
    void channelRead(ChannelHandlerContext& ctx, std::any& message) override;

protected:
    virtual bool decode(std::any& out) = 0;

    ByteBuffer& cumulation() noexcept {
        return cumulation_;
    }

    const ByteBuffer& cumulation() const noexcept {
        return cumulation_;
    }

private:
    ByteBuffer cumulation_;
};
