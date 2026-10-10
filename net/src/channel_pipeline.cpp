#include "net/channel_pipeline.h"
#include "net/channel.h"

#include <any>

void ChannelPipeline::attach(Channel& channel) {
    channel_ = &channel;
}

Channel& ChannelPipeline::channel() const noexcept {
    return *channel_;
}

void ChannelPipeline::addLast(std::shared_ptr<ChannelHandler> handler) {
    const std::size_t index = entries_.size();
    Entry entry;
    entry.context = std::make_unique<ChannelHandlerContext>(*this, index);
    entry.handler = std::move(handler);
    entries_.push_back(std::move(entry));
}

std::size_t ChannelPipeline::size() const noexcept {
    return entries_.size();
}

void ChannelPipeline::fireChannelActive() {
    fireChannelActiveFrom(0);
}

void ChannelPipeline::fireChannelRead(std::any& message) {
    fireChannelReadFrom(0, message);
}

void ChannelPipeline::fireChannelReadComplete() {
    fireChannelReadCompleteFrom(0);
}

void ChannelPipeline::fireChannelInactive() {
    fireChannelInactiveFrom(0);
}

void ChannelPipeline::fireChannelWritabilityChanged() {
    fireChannelWritabilityChangedFrom(0);
}

void ChannelPipeline::fireUserEventTriggered(std::any& event) {
    fireUserEventTriggeredFrom(0, event);
}

void ChannelPipeline::fireExceptionCaught(const std::exception& error) {
    fireExceptionCaughtFrom(0, error);
}

void ChannelPipeline::fireChannelActiveFrom(std::size_t index) {
    for (std::size_t i = index; i < entries_.size(); ++i) {
        auto* handler = dynamic_cast<ChannelInboundHandler*>(entries_[i].handler.get());
        if (handler != nullptr) {
            handler->channelActive(*entries_[i].context);
            return;
        }
    }
}

void ChannelPipeline::fireChannelReadFrom(std::size_t index, std::any& message) {
    for (std::size_t i = index; i < entries_.size(); ++i) {
        auto* handler = dynamic_cast<ChannelInboundHandler*>(entries_[i].handler.get());
        if (handler != nullptr) {
            handler->channelRead(*entries_[i].context, message);
            return;
        }
    }
}

void ChannelPipeline::fireChannelReadCompleteFrom(std::size_t index) {
    for (std::size_t i = index; i < entries_.size(); ++i) {
        auto* handler = dynamic_cast<ChannelInboundHandler*>(entries_[i].handler.get());
        if (handler != nullptr) {
            handler->channelReadComplete(*entries_[i].context);
            return;
        }
    }
}

void ChannelPipeline::fireChannelInactiveFrom(std::size_t index) {
    for (std::size_t i = index; i < entries_.size(); ++i) {
        auto* handler = dynamic_cast<ChannelInboundHandler*>(entries_[i].handler.get());
        if (handler != nullptr) {
            handler->channelInactive(*entries_[i].context);
            return;
        }
    }
}

void ChannelPipeline::fireChannelWritabilityChangedFrom(std::size_t index) {
    for (std::size_t i = index; i < entries_.size(); ++i) {
        auto* handler = dynamic_cast<ChannelInboundHandler*>(entries_[i].handler.get());
        if (handler != nullptr) {
            handler->channelWritabilityChanged(*entries_[i].context);
            return;
        }
    }
}

void ChannelPipeline::fireUserEventTriggeredFrom(std::size_t index, std::any& event) {
    for (std::size_t i = index; i < entries_.size(); ++i) {
        auto* handler = dynamic_cast<ChannelInboundHandler*>(entries_[i].handler.get());
        if (handler != nullptr) {
            handler->userEventTriggered(*entries_[i].context, event);
            return;
        }
    }
}

void ChannelPipeline::fireExceptionCaughtFrom(std::size_t index, const std::exception& error) {
    for (std::size_t i = index; i < entries_.size(); ++i) {
        auto* handler = dynamic_cast<ChannelInboundHandler*>(entries_[i].handler.get());
        if (handler != nullptr) {
            handler->exceptionCaught(*entries_[i].context, error);
            return;
        }
    }
}

void ChannelPipeline::write(std::any& message) {
    writeFrom(entries_.size(), message);
}

void ChannelPipeline::writeFrom(std::size_t index, std::any& message) {
    while (index > 0) {
        --index;
        auto* handler = dynamic_cast<ChannelOutboundHandler*>(entries_[index].handler.get());
        if (handler != nullptr) {
            handler->write(*entries_[index].context, message);
            return;
        }
    }
    channel_->writeTransport(message);
}

void ChannelPipeline::flush() {
    flushFrom(entries_.size());
}

void ChannelPipeline::flushFrom(std::size_t index) {
    while (index > 0) {
        --index;
        auto* handler = dynamic_cast<ChannelOutboundHandler*>(entries_[index].handler.get());
        if (handler != nullptr) {
            handler->flush(*entries_[index].context);
            return;
        }
    }
    channel_->flushTransport();
}

void ChannelPipeline::close() {
    closeFrom(entries_.size());
}

void ChannelPipeline::closeFrom(std::size_t index) {
    while (index > 0) {
        --index;
        auto* handler = dynamic_cast<ChannelOutboundHandler*>(entries_[index].handler.get());
        if (handler != nullptr) {
            handler->close(*entries_[index].context);
            return;
        }
    }
    channel_->closeTransport();
}
