#pragma once

#include "net/channel_handler.h"
#include "net/channel_handler_context.h"

#include <any>
#include <cstddef>
#include <exception>
#include <memory>
#include <utility>
#include <vector>

class Channel;

class ChannelPipeline {
public:
    void attach(Channel& channel);

    Channel& channel() const noexcept;

    void addLast(std::shared_ptr<ChannelHandler> handler);

    std::size_t size() const noexcept;

    void fireChannelActive();

    void fireChannelRead(std::any& message);

    void fireChannelReadComplete();

    void fireChannelInactive();

    void fireChannelWritabilityChanged();

    void fireUserEventTriggered(std::any& event);

    void fireExceptionCaught(const std::exception& error);

    void fireChannelActiveFrom(std::size_t index);

    void fireChannelReadFrom(std::size_t index, std::any& message);

    void fireChannelReadCompleteFrom(std::size_t index);

    void fireChannelInactiveFrom(std::size_t index);

    void fireChannelWritabilityChangedFrom(std::size_t index);

    void fireUserEventTriggeredFrom(std::size_t index, std::any& event);

    void fireExceptionCaughtFrom(std::size_t index, const std::exception& error);

    void write(std::any& message);

    void writeFrom(std::size_t index, std::any& message);

    void flush();

    void flushFrom(std::size_t index);

    void close();

    void closeFrom(std::size_t index);

private:
    struct Entry {
        std::shared_ptr<ChannelHandler> handler;
        std::unique_ptr<ChannelHandlerContext> context;
    };

    Channel* channel_ = nullptr;
    std::vector<Entry> entries_;
};
