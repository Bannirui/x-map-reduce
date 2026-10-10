#include "net/idle_state_handler.h"
#include "net/channel.h"
#include "net/channel_handler_context.h"

#include <algorithm>
#include <any>

IdleStateHandler::IdleStateHandler(std::chrono::milliseconds readerIdle,
                                   std::chrono::milliseconds writerIdle,
                                   std::chrono::milliseconds allIdle)
    : readerIdle_(readerIdle), writerIdle_(writerIdle), allIdle_(allIdle) {
}

void IdleStateHandler::channelActive(ChannelHandlerContext& ctx) {
    loop_ = ctx.channel().loop();
    const auto now = std::chrono::steady_clock::now();
    lastRead_ = now;
    lastWrite_ = now;
    std::chrono::milliseconds interval{0};
    if (readerIdle_.count() > 0) {
        interval = readerIdle_;
    }
    if (writerIdle_.count() > 0) {
        interval = interval.count() == 0 ? writerIdle_ : std::min(interval, writerIdle_);
    }
    if (allIdle_.count() > 0) {
        interval = interval.count() == 0 ? allIdle_ : std::min(interval, allIdle_);
    }
    if (interval.count() > 0) {
        timerId_ = loop_->addInterval(interval, [this, &ctx] { check(ctx); });
    }
    ctx.fireChannelActive();
}

void IdleStateHandler::channelRead(ChannelHandlerContext& ctx, std::any& message) {
    lastRead_ = std::chrono::steady_clock::now();
    ctx.fireChannelRead(message);
}

void IdleStateHandler::channelInactive(ChannelHandlerContext& ctx) {
    if (loop_ != nullptr && timerId_ != TimerQueue::kInvalidId) {
        loop_->cancelTimer(timerId_);
        timerId_ = TimerQueue::kInvalidId;
    }
    ctx.fireChannelInactive();
}

void IdleStateHandler::write(ChannelHandlerContext& ctx, std::any& message) {
    lastWrite_ = std::chrono::steady_clock::now();
    ctx.write(message);
}

void IdleStateHandler::check(ChannelHandlerContext& ctx) {
    const auto now = std::chrono::steady_clock::now();
    const bool readerIdle = readerIdle_.count() > 0 && now - lastRead_ >= readerIdle_;
    const bool writerIdle = writerIdle_.count() > 0 && now - lastWrite_ >= writerIdle_;
    const bool allIdle = allIdle_.count() > 0
            && now - lastRead_ >= allIdle_ && now - lastWrite_ >= allIdle_;
    if (readerIdle) {
        lastRead_ = now;
        std::any event = IdleState::ReaderIdle;
        ctx.fireUserEventTriggered(event);
    }
    if (writerIdle) {
        lastWrite_ = now;
        std::any event = IdleState::WriterIdle;
        ctx.fireUserEventTriggered(event);
    }
    if (allIdle) {
        std::any event = IdleState::AllIdle;
        ctx.fireUserEventTriggered(event);
    }
}
