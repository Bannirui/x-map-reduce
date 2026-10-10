#include "net/buffer.h"
#include "net/channel.h"
#include "net/channel_handler.h"
#include "net/channel_handler_context.h"
#include "net/channel_pipeline.h"
#include "net/event_loop.h"
#include "net/idle_state_handler.h"
#include "net/net.h"

#include <any>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

std::atomic<int> failures{0};

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

class WatermarkProbe : public ChannelInboundHandler {
public:
    explicit WatermarkProbe(std::shared_ptr<std::promise<void> > promise)
        : promise_(std::move(promise)) {
    }

    void channelActive(ChannelHandlerContext& ctx) override {
        std::vector<std::uint8_t> big(2 * 1024 * 1024, 'x');
        std::any message = ByteBuffer(big);
        ctx.write(message);
    }

    void channelWritabilityChanged(ChannelHandlerContext& ctx) override {
        if (!ctx.channel().isWritable() && !fired_) {
            fired_ = true;
            promise_->set_value();
        }
    }

private:
    bool fired_ = false;
    std::shared_ptr<std::promise<void> > promise_;
};

class IdleProbe : public ChannelInboundHandler {
public:
    explicit IdleProbe(std::shared_ptr<std::promise<void> > promise)
        : promise_(std::move(promise)) {
    }

    void userEventTriggered(ChannelHandlerContext& ctx, std::any& event) override {
        IdleState* state = std::any_cast<IdleState>(&event);
        if (state != nullptr && *state == IdleState::ReaderIdle && !fired_) {
            fired_ = true;
            promise_->set_value();
        }
    }

private:
    bool fired_ = false;
    std::shared_ptr<std::promise<void> > promise_;
};

}  // namespace

int main() {
    try {
        {
            EventLoop loop;
            std::thread loopThread([&] { loop.run(); });

            Listener listener("127.0.0.1", 0);
            setNonBlocking(listener.fd());

            auto promise = std::make_shared<std::promise<void> >();
            auto probe = std::make_shared<WatermarkProbe>(promise);
            std::vector<std::shared_ptr<Channel> > channels;

            loop.add(listener.fd(), kReadable, [&](std::uint32_t) {
                while (true) {
                    Connection connection;
                    if (listener.acceptNonBlocking(connection) != IoStatus::Ok) {
                        break;
                    }
                    auto channel = std::make_shared<Channel>(std::move(connection), &loop);
                    channel->pipeline().addLast(probe);
                    channels.push_back(channel);
                    channel->start();
                }
            });

            Connection client = connectTo("127.0.0.1", listener.port());
            setNonBlocking(client.fd());

            std::future<void> future = promise->get_future();
            check(future.wait_for(std::chrono::seconds(2)) == std::future_status::ready,
                  "write buffer water mark reports not-writable");

            loop.stop();
            loopThread.join();
        }

        {
            EventLoop loop;
            std::thread loopThread([&] { loop.run(); });

            Listener listener("127.0.0.1", 0);
            setNonBlocking(listener.fd());

            auto promise = std::make_shared<std::promise<void> >();
            auto probe = std::make_shared<IdleProbe>(promise);
            std::vector<std::shared_ptr<Channel> > channels;

            loop.add(listener.fd(), kReadable, [&](std::uint32_t) {
                while (true) {
                    Connection connection;
                    if (listener.acceptNonBlocking(connection) != IoStatus::Ok) {
                        break;
                    }
                    auto channel = std::make_shared<Channel>(std::move(connection), &loop);
                    channel->pipeline().addLast(std::make_shared<IdleStateHandler>(
                        std::chrono::milliseconds(100), std::chrono::milliseconds(0), std::chrono::milliseconds(0)));
                    channel->pipeline().addLast(probe);
                    channels.push_back(channel);
                    channel->start();
                }
            });

            Connection client = connectTo("127.0.0.1", listener.port());
            setNonBlocking(client.fd());

            std::future<void> future = promise->get_future();
            check(future.wait_for(std::chrono::seconds(2)) == std::future_status::ready,
                  "idle state handler fires reader idle");

            loop.stop();
            loopThread.join();
        }
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
