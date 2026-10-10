#include "net/bootstrap.h"
#include "net/channel.h"
#include "net/channel_handler.h"
#include "net/channel_handler_context.h"
#include "net/channel_pipeline.h"
#include "net/event_loop_group.h"
#include "protocol/frame_codec.h"
#include "protocol/framing.h"
#include "protocol/protocol.h"

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

class EchoFrameHandler : public ChannelInboundHandler {
public:
    void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
        if (std::any_cast<xmr::protocol::Frame>(&message) == nullptr) {
            return;
        }
        ctx.write(message);
    }
};

class FrameCollectorHandler : public ChannelInboundHandler {
public:
    explicit FrameCollectorHandler(std::shared_ptr<std::promise<xmr::protocol::Frame> > promise)
        : promise_(std::move(promise)) {
    }

    void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
        xmr::protocol::Frame* frame = std::any_cast<xmr::protocol::Frame>(&message);
        if (frame == nullptr || fulfilled_) {
            return;
        }
        fulfilled_ = true;
        promise_->set_value(*frame);
    }

private:
    bool fulfilled_ = false;
    std::shared_ptr<std::promise<xmr::protocol::Frame> > promise_;
};

}  // namespace

int main() {
    try {
        EventLoopGroup serverGroup(2);
        serverGroup.start();

        ServerBootstrap server;
        server.group(serverGroup).childHandler([](Channel& channel) {
            channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameEncoder>());
            channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameDecoder>());
            channel.pipeline().addLast(std::make_shared<EchoFrameHandler>());
        });
        server.bind("127.0.0.1", 0);

        EventLoopGroup clientGroup(1);
        clientGroup.start();

        auto promise = std::make_shared<std::promise<xmr::protocol::Frame> >();
        auto collector = std::make_shared<FrameCollectorHandler>(promise);

        ClientBootstrap client;
        client.group(clientGroup).handler([collector](Channel& channel) {
            channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameEncoder>());
            channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameDecoder>());
            channel.pipeline().addLast(collector);
        });
        std::shared_ptr<Channel> channel = client.connect("127.0.0.1", server.port());
        check(channel != nullptr, "frame codec client connected");

        xmr::protocol::Frame outgoing;
        outgoing.header.type = xmr::protocol::MessageType::Hello;
        outgoing.header.requestId = 7;
        outgoing.body = {1, 2, 3, 4, 5};
        channel->write(std::any(outgoing));

        std::future<xmr::protocol::Frame> future = promise->get_future();
        if (future.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
            const xmr::protocol::Frame echoed = future.get();
            check(echoed.header.type == xmr::protocol::MessageType::Hello, "echoed message type");
            check(echoed.header.requestId == 7, "echoed request id");
            check(echoed.body == outgoing.body, "echoed body");
        } else {
            check(false, "client did not receive the frame echo in time");
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
