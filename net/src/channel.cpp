#include "net/channel.h"
#include "net/event_loop.h"

#include <any>
#include <cstdint>
#include <exception>
#include <utility>
#include <vector>

#include <unistd.h>

Channel::Channel(Connection connection, EventLoop* loop)
    : connection_(std::move(connection)), loop_(loop) {
    pipeline_.attach(*this);
}

Channel::~Channel() {
    if (!closed_) {
        closeNow();
    }
}

void Channel::start() {
    setNonBlocking(connection_.fd());
    std::shared_ptr<Channel> self = shared_from_this();
    loop_->runInLoop([self] {
        self->pipeline_.fireChannelActive();
        self->loop_->add(self->connection_.fd(), kReadable,
                         [self](std::uint32_t events) { self->handleEvents(events); });
    });
}

void Channel::write(const void* data, std::size_t size) {
    const auto* bytes = static_cast<const std::uint8_t*>(data);
    write(std::vector<std::uint8_t>(bytes, bytes + size));
}

void Channel::write(const std::vector<std::uint8_t>& data) {
    ByteBuffer buffer;
    buffer.append(data.data(), data.size());
    write(std::any(std::move(buffer)));
}

void Channel::write(std::any message) {
    std::shared_ptr<Channel> self = shared_from_this();
    loop_->runInLoop([self, message = std::move(message)]() mutable {
        if (!self->closed_) {
            self->pipeline_.write(message);
        }
    });
}

void Channel::writeTransport(std::any& message) {
    ByteBuffer* buffer = std::any_cast<ByteBuffer>(&message);
    if (buffer == nullptr) {
        return;
    }
    std::vector<std::uint8_t> bytes(buffer->data(), buffer->data() + buffer->size());
    std::shared_ptr<Channel> self = shared_from_this();
    loop_->runInLoop([self, bytes = std::move(bytes)]() mutable {
        if (self->closed_) {
            return;
        }
        self->outbound_.append(bytes.data(), bytes.size());
        self->updateWritability();
        self->doWrite();
    });
}

void Channel::setWriteBufferWaterMark(std::size_t low, std::size_t high) {
    lowWaterMark_ = low;
    highWaterMark_ = high;
}

void Channel::updateWritability() {
    const std::size_t size = outbound_.size();
    if (writable_ && size >= highWaterMark_) {
        writable_ = false;
        pipeline_.fireChannelWritabilityChanged();
    } else if (!writable_ && size <= lowWaterMark_) {
        writable_ = true;
        pipeline_.fireChannelWritabilityChanged();
    }
}

void Channel::flush() {
    std::shared_ptr<Channel> self = shared_from_this();
    loop_->runInLoop([self] {
        if (!self->closed_) {
            self->pipeline_.flush();
        }
    });
}

void Channel::flushTransport() {
    std::shared_ptr<Channel> self = shared_from_this();
    loop_->runInLoop([self] {
        if (!self->closed_) {
            self->doWrite();
        }
    });
}

void Channel::close() {
    std::shared_ptr<Channel> self = shared_from_this();
    loop_->runInLoop([self] {
        if (!self->closed_) {
            self->pipeline_.close();
        }
    });
}

void Channel::closeTransport() {
    closeInLoop();
}

void Channel::closeInLoop() {
    if (closed_) {
        return;
    }
    if (!outbound_.empty()) {
        closing_ = true;
        enableWriting();
        return;
    }
    closeNow();
}

void Channel::closeNow() {
    if (closed_) {
        return;
    }
    closed_ = true;
    pipeline_.fireChannelInactive();
    // 把fd所有权从Connection里拿出来 交给事件循环 先摘epoll再close 避免在已关闭的fd上删事件
    const int fd = connection_.release();
    if (fd >= 0) {
        EventLoop* loop = loop_;
        loop->queueInLoop([loop, fd] {
            loop->remove(fd);
            ::close(fd);
        });
    }
}

void Channel::handleEvents(std::uint32_t events) {
    if (closed_) {
        return;
    }
    try {
        if ((events & kWritable) != 0) {
            doWrite();
        }
        if ((events & (kReadable | kBroken)) != 0) {
            doRead();
        }
    } catch (const std::exception& error) {
        pipeline_.fireExceptionCaught(error);
        closeNow();
    }
}

void Channel::doRead() {
    if (!reading_) {
        return;
    }
    const IoStatus status = recvInto(connection_.fd(), inbound_);
    if (status == IoStatus::Ok) {
        std::any message = std::move(inbound_);
        pipeline_.fireChannelRead(message);
        pipeline_.fireChannelReadComplete();
    } else if (status == IoStatus::Closed) {
        // 对端不再发数据 半关闭读方向：还有待发数据就先发完再关 否则直接关
        reading_ = false;
        if (outbound_.empty()) {
            closeNow();
        } else {
            closing_ = true;
            writing_ = true;
            updateInterest();
        }
    }
}

void Channel::doWrite() {
    while (!outbound_.empty()) {
        std::size_t sent = 0;
        const IoStatus status =
                sendFrom(connection_.fd(), outbound_.data(), outbound_.size(), sent);
        if (status == IoStatus::Ok) {
            outbound_.consume(sent);
            continue;
        }
        if (status == IoStatus::WouldBlock) {
            enableWriting();
            updateWritability();
            return;
        }
        closeNow();
        return;
    }
    if (closing_) {
        closeNow();
        return;
    }
    disableWriting();
    updateWritability();
}

std::uint32_t Channel::interest() const {
    std::uint32_t events = 0;
    if (reading_) {
        events |= kReadable;
    }
    if (writing_) {
        events |= kWritable;
    }
    return events != 0 ? events : kReadable;
}

void Channel::updateInterest() {
    loop_->modify(connection_.fd(), interest());
}

void Channel::enableWriting() {
    if (writing_) {
        return;
    }
    writing_ = true;
    updateInterest();
}

void Channel::disableWriting() {
    if (!writing_) {
        return;
    }
    writing_ = false;
    updateInterest();
}
