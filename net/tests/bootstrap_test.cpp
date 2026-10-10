#include "net/bootstrap.h"
#include "net/buffer.h"
#include "net/channel.h"
#include "net/channel_handler.h"
#include "net/channel_handler_context.h"
#include "net/channel_pipeline.h"
#include "net/event_loop_group.h"

#include <any>
#include <atomic>
#include <cctype>
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

class EchoHandler : public ChannelInboundHandler {
public:
    void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
        if (std::any_cast<ByteBuffer>(&message) == nullptr) {
            return;
        }
        ctx.write(message);
    }
};

class UpperOutboundHandler : public ChannelOutboundHandler {
public:
    void write(ChannelHandlerContext& ctx, std::any& message) override {
        ByteBuffer* buffer = std::any_cast<ByteBuffer>(&message);
        if (buffer == nullptr) {
            ctx.write(message);
            return;
        }
        std::vector<std::uint8_t> upper(buffer->data(), buffer->data() + buffer->size());
        for (std::uint8_t& byte : upper) {
            byte = static_cast<std::uint8_t>(std::toupper(byte));
        }
        std::any encoded = ByteBuffer(upper);
        ctx.write(encoded);
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

}  // namespace

int main() {
    try {
        std::string expected = "bootstrap";
        for (char& character : expected) {
            character = static_cast<char>(std::toupper(static_cast<unsigned char>(character)));
        }

        EventLoopGroup serverGroup(2);
        serverGroup.start();

        std::shared_ptr<ChannelHandler> upper = std::make_shared<UpperOutboundHandler>();
        std::shared_ptr<ChannelHandler> echo = std::make_shared<EchoHandler>();

        ServerBootstrap server;
        server.group(serverGroup).childHandler([upper, echo](Channel& channel) {
            channel.pipeline().addLast(upper);
            channel.pipeline().addLast(echo);
        });
        server.bind("127.0.0.1", 0);
        check(server.port() != 0, "server bootstrap binds a real port");

        EventLoopGroup clientGroup(1);
        clientGroup.start();

        auto promise = std::make_shared<std::promise<std::string> >();
        auto collector = std::make_shared<CollectorHandler>(expected.size(), promise);

        ClientBootstrap client;
        client.group(clientGroup).handler([collector](Channel& channel) {
            channel.pipeline().addLast(collector);
        });
        std::shared_ptr<Channel> channel = client.connect("127.0.0.1", server.port());
        check(channel != nullptr && !channel->closed(), "client bootstrap connects a channel");

        const std::string payload = "bootstrap";
        channel->write(payload.data(), payload.size());

        std::future<std::string> future = promise->get_future();
        if (future.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
            check(future.get() == expected, "server bootstrap echoes back through the pipeline");
        } else {
            check(false, "client did not receive the echo in time");
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
