#pragma once

#include "net/buffer.h"
#include "net/channel_pipeline.h"
#include "net/net.h"

#include <any>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

class EventLoop;

class Channel : public std::enable_shared_from_this<Channel> {
public:
    Channel(Connection connection, EventLoop* loop);

    ~Channel();

    Channel(const Channel&) = delete;

    Channel& operator=(const Channel&) = delete;

    int fd() const noexcept {
        return connection_.fd();
    }

    EventLoop* loop() const noexcept {
        return loop_;
    }

    ChannelPipeline& pipeline() noexcept {
        return pipeline_;
    }

    bool closed() const noexcept {
        return closed_;
    }

    void start();

    void write(std::any message);

    void write(const void* data, std::size_t size);

    void write(const std::vector<std::uint8_t>& data);

    void writeTransport(std::any& message);

    void flush();

    void close();

    void flushTransport();

    void closeTransport();

    void setWriteBufferWaterMark(std::size_t low, std::size_t high);

    bool isWritable() const noexcept {
        return writable_;
    }

    std::size_t writeBufferSize() const noexcept {
        return outbound_.size();
    }

private:
    void handleEvents(std::uint32_t events);

    void doRead();

    void doWrite();

    void closeInLoop();

    void closeNow();

    void updateWritability();

    std::uint32_t interest() const;

    void updateInterest();

    void enableWriting();

    void disableWriting();

    Connection connection_;
    EventLoop* loop_ = nullptr;
    ChannelPipeline pipeline_;
    ByteBuffer inbound_;
    ByteBuffer outbound_;
    std::size_t lowWaterMark_ = 32 * 1024;
    std::size_t highWaterMark_ = 64 * 1024;
    bool writable_ = true;
    bool reading_ = true;
    bool writing_ = false;
    bool closing_ = false;
    bool closed_ = false;
};
