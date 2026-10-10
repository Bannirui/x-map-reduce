#include "net/bootstrap.h"
#include "net/buffer.h"
#include "net/channel.h"
#include "net/channel_handler.h"
#include "net/channel_handler_context.h"
#include "net/channel_pipeline.h"
#include "net/codec/length_field_frame_decoder.h"
#include "net/codec/message_to_byte_encoder.h"
#include "net/event_loop_group.h"

#include <any>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {

std::atomic<int> failures{0};

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

class LengthFieldPrepender : public MessageToByteEncoder<ByteBuffer> {
protected:
    void encode(const ByteBuffer& message, ByteBuffer& out) override {
        const auto length = static_cast<std::uint32_t>(message.size());
        const std::uint8_t prefix[4] = {
            static_cast<std::uint8_t>((length >> 24) & 0xff),
            static_cast<std::uint8_t>((length >> 16) & 0xff),
            static_cast<std::uint8_t>((length >> 8) & 0xff),
            static_cast<std::uint8_t>(length & 0xff),
        };
        out.append(prefix, sizeof(prefix));
        out.append(message.data(), message.size());
    }
};

class EchoHandler : public ChannelInboundHandler {
public:
    void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
        if (std::any_cast<ByteBuffer>(&message) == nullptr) {
            return;
        }
        ctx.write(message);
    }
};

class CollectorHandler : public ChannelInboundHandler {
public:
    CollectorHandler(std::size_t want, std::shared_ptr<std::promise<std::string> > promise)
        : want_(want), promise_(std::move(promise)) {
    }

    void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
        ByteBuffer* buffer = std::any_cast<ByteBuffer>(&message);
        if (buffer == nullptr || fulfilled_) {
            return;
        }
        collected_.append(reinterpret_cast<const char*>(buffer->data()), buffer->size());
        if (collected_.size() >= want_) {
            fulfilled_ = true;
            promise_->set_value(collected_);
        }
    }

private:
    std::size_t want_;
    bool fulfilled_ = false;
    std::string collected_;
    std::shared_ptr<std::promise<std::string> > promise_;
};

std::vector<std::uint8_t> toBytes(const std::string& text) {
    return std::vector<std::uint8_t>(text.begin(), text.end());
}

}  // namespace

int main() {
    try {
        const std::string alpha = "alpha";
        const std::string big(100000, 'z');
        const std::string omega = "omega";
        const std::string expected = alpha + big + omega;

        EventLoopGroup serverGroup(2);
        serverGroup.start();

        ServerBootstrap server;
        server.group(serverGroup).childHandler([](Channel& channel) {
            channel.pipeline().addLast(std::make_shared<LengthFieldPrepender>());
            channel.pipeline().addLast(std::make_shared<LengthFieldBasedFrameDecoder>(0, 4, 0, 4, 1u << 20));
            channel.pipeline().addLast(std::make_shared<EchoHandler>());
        });
        server.bind("127.0.0.1", 0);

        EventLoopGroup clientGroup(1);
        clientGroup.start();

        auto promise = std::make_shared<std::promise<std::string> >();
        auto collector = std::make_shared<CollectorHandler>(expected.size(), promise);

        ClientBootstrap client;
        client.group(clientGroup).handler([collector](Channel& channel) {
            channel.pipeline().addLast(std::make_shared<LengthFieldPrepender>());
            channel.pipeline().addLast(std::make_shared<LengthFieldBasedFrameDecoder>(0, 4, 0, 4, 1u << 20));
            channel.pipeline().addLast(collector);
        });
        std::shared_ptr<Channel> channel = client.connect("127.0.0.1", server.port());
        check(channel != nullptr, "codec client connected");

        channel->write(ByteBuffer(toBytes(alpha)));
        channel->write(ByteBuffer(toBytes(big)));
        channel->write(ByteBuffer(toBytes(omega)));

        std::future<std::string> future = promise->get_future();
        if (future.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
            check(future.get() == expected, "length-field codec frames and reassembles the stream");
        } else {
            check(false, "client did not receive the framed echo in time");
        }

        server.close();
        clientGroup.stop();
        serverGroup.stop();
    } catch (const std::exception& error) {
        std::cerr << "unexpected exception: " << error.what() << '\n';
        ++failures;
    }

    if (failures != 0) {
        std::cerr << failures.load() << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
