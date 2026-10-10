#include "net/channel.h"
#include "net/channel_handler.h"
#include "net/channel_handler_context.h"
#include "net/channel_pipeline.h"
#include "net/event_loop.h"
#include "net/net.h"

#include <any>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
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

}  // namespace

int main() {
    using std::chrono::milliseconds;

    try {
        EventLoop loop;
        std::thread loopThread([&] { loop.run(); });

        Listener listener("127.0.0.1", 0);
        setNonBlocking(listener.fd());
        const std::uint16_t port = listener.port();
        check(port != 0, "echo listener bound to a real port");

        std::shared_ptr<ChannelHandler> upper = std::make_shared<UpperOutboundHandler>();
        std::shared_ptr<ChannelHandler> echo = std::make_shared<EchoHandler>();
        std::vector<std::shared_ptr<Channel> > channels;

        loop.add(listener.fd(), kReadable, [&](std::uint32_t) {
            while (true) {
                Connection connection;
                const IoStatus status = listener.acceptNonBlocking(connection);
                if (status == IoStatus::WouldBlock) {
                    break;
                }
                if (status != IoStatus::Ok) {
                    break;
                }
                auto channel = std::make_shared<Channel>(std::move(connection), &loop);
                channel->pipeline().addLast(upper);
                channel->pipeline().addLast(echo);
                channels.push_back(channel);
                channel->start();
            }
        });

        Connection client = connectTo("127.0.0.1", port);
        setNonBlocking(client.fd());

        const std::string payload = "hello channel";
        sendAll(client.fd(), payload.data(), payload.size());

        ByteBuffer inbound;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (inbound.size() < payload.size() && std::chrono::steady_clock::now() < deadline) {
            const IoStatus status = recvInto(client.fd(), inbound);
            if (status == IoStatus::Closed) {
                break;
            }
            if (status == IoStatus::WouldBlock) {
                std::this_thread::sleep_for(milliseconds(1));
            }
        }
        std::string expected = payload;
        for (char& character : expected) {
            character = static_cast<char>(std::toupper(static_cast<unsigned char>(character)));
        }
        const std::string echoed(reinterpret_cast<const char*>(inbound.data()), inbound.size());
        check(echoed == expected, "write reaches outbound handlers and is uppercased");

        loop.stop();
        loopThread.join();
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
