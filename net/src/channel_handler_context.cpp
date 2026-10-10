#include "net/channel_handler_context.h"
#include "net/buffer.h"
#include "net/channel.h"
#include "net/channel_pipeline.h"

#include <any>
#include <utility>

ChannelHandlerContext::ChannelHandlerContext(ChannelPipeline& pipeline, std::size_t index)
    : pipeline_(pipeline), index_(index) {
}

Channel& ChannelHandlerContext::channel() const noexcept {
    return pipeline_.channel();
}

ChannelPipeline& ChannelHandlerContext::pipeline() const noexcept {
    return pipeline_;
}

std::size_t ChannelHandlerContext::index() const noexcept {
    return index_;
}

void ChannelHandlerContext::fireChannelActive() {
    pipeline_.fireChannelActiveFrom(index_ + 1);
}

void ChannelHandlerContext::fireChannelRead(std::any& message) {
    pipeline_.fireChannelReadFrom(index_ + 1, message);
}

void ChannelHandlerContext::fireChannelReadComplete() {
    pipeline_.fireChannelReadCompleteFrom(index_ + 1);
}

void ChannelHandlerContext::fireChannelInactive() {
    pipeline_.fireChannelInactiveFrom(index_ + 1);
}

void ChannelHandlerContext::fireChannelWritabilityChanged() {
    pipeline_.fireChannelWritabilityChangedFrom(index_ + 1);
}

void ChannelHandlerContext::fireUserEventTriggered(std::any& event) {
    pipeline_.fireUserEventTriggeredFrom(index_ + 1, event);
}

void ChannelHandlerContext::fireExceptionCaught(const std::exception& error) {
    pipeline_.fireExceptionCaughtFrom(index_ + 1, error);
}

void ChannelHandlerContext::write(std::any& message) {
    pipeline_.writeFrom(index_, message);
}

void ChannelHandlerContext::write(const std::vector<std::uint8_t>& data) {
    ByteBuffer buffer;
    buffer.append(data.data(), data.size());
    std::any message = std::move(buffer);
    write(message);
}

void ChannelHandlerContext::flush() {
    pipeline_.flushFrom(index_);
}

void ChannelHandlerContext::close() {
    pipeline_.closeFrom(index_);
}
