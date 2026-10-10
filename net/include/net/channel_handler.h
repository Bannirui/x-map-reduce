#pragma once

#include <any>
#include <exception>

class ChannelHandlerContext;

class ChannelHandler {
public:
    virtual ~ChannelHandler() = default;
};

class ChannelInboundHandler : public virtual ChannelHandler {
public:
    virtual void channelActive(ChannelHandlerContext& ctx) {
    }

    virtual void channelRead(ChannelHandlerContext& ctx, std::any& message) {
    }

    virtual void channelReadComplete(ChannelHandlerContext& ctx) {
    }

    virtual void channelInactive(ChannelHandlerContext& ctx) {
    }

    virtual void channelWritabilityChanged(ChannelHandlerContext& ctx) {
    }

    virtual void userEventTriggered(ChannelHandlerContext& ctx, std::any& event) {
    }

    virtual void exceptionCaught(ChannelHandlerContext& ctx, const std::exception& error) {
    }
};

class ChannelOutboundHandler : public virtual ChannelHandler {
public:
    virtual void write(ChannelHandlerContext& ctx, std::any& message);

    virtual void flush(ChannelHandlerContext& ctx);

    virtual void close(ChannelHandlerContext& ctx);
};
