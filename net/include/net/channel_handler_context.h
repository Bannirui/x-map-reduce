#pragma once

#include <any>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <vector>

class Channel;
class ChannelPipeline;

class ChannelHandlerContext {
public:
    ChannelHandlerContext(ChannelPipeline& pipeline, std::size_t index);

    Channel& channel() const noexcept;

    ChannelPipeline& pipeline() const noexcept;

    std::size_t index() const noexcept;

    void fireChannelActive();

    void fireChannelRead(std::any& message);

    void fireChannelReadComplete();

    void fireChannelInactive();

    void fireChannelWritabilityChanged();

    void fireUserEventTriggered(std::any& event);

    void fireExceptionCaught(const std::exception& error);

    void write(std::any& message);

    void write(const std::vector<std::uint8_t>& data);

    void flush();

    void close();

private:
    ChannelPipeline& pipeline_;
    std::size_t index_;
};
