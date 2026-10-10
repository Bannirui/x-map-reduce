#pragma once

#include "net/channel_handler.h"
#include "net/event_loop.h"

#include <any>
#include <chrono>

class ChannelHandlerContext;

enum class IdleState {
    ReaderIdle,
    WriterIdle,
    AllIdle,
};

class IdleStateHandler : public ChannelInboundHandler, public ChannelOutboundHandler {
public:
    IdleStateHandler(std::chrono::milliseconds readerIdle,
                     std::chrono::milliseconds writerIdle,
                     std::chrono::milliseconds allIdle);

    void channelActive(ChannelHandlerContext& ctx) override;

    void channelRead(ChannelHandlerContext& ctx, std::any& message) override;

    void channelInactive(ChannelHandlerContext& ctx) override;

    void write(ChannelHandlerContext& ctx, std::any& message) override;

private:
    void check(ChannelHandlerContext& ctx);

    std::chrono::milliseconds readerIdle_;
    std::chrono::milliseconds writerIdle_;
    std::chrono::milliseconds allIdle_;
    std::chrono::steady_clock::time_point lastRead_;
    std::chrono::steady_clock::time_point lastWrite_;
    EventLoop* loop_ = nullptr;
    TimerQueue::TimerId timerId_ = TimerQueue::kInvalidId;
};
