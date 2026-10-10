#include "net/buffer.h"
#include "net/channel.h"
#include "net/channel_handler.h"
#include "net/channel_handler_context.h"
#include "net/channel_pipeline.h"
#include "net/event_loop.h"
#include "net/net.h"

#include <any>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <sys/socket.h>

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

}  // namespace

int main() {
    try {
        EventLoop loop;
        std::thread loopThread([&] { loop.run(); });

        Listener listener("127.0.0.1", 0);
        setNonBlocking(listener.fd());

        std::vector<std::shared_ptr<Channel> > channels;

        loop.add(listener.fd(), kReadable, [&](std::uint32_t) {
            while (true) {
                Connection connection;
                if (listener.acceptNonBlocking(connection) != IoStatus::Ok) {
                    break;
                }
                auto channel = std::make_shared<Channel>(std::move(connection), &loop);
                channel->pipeline().addLast(std::make_shared<EchoHandler>());
                channels.push_back(channel);
                channel->start();
            }
        });

        Connection client = connectTo("127.0.0.1", listener.port());
        const std::string payload = "ping";
        sendAll(client.fd(), payload.data(), payload.size());
        // 半关闭写方向 服务端仍应能把回显发完再关
        ::shutdown(client.fd(), SHUT_WR);

        std::string got;
        char buffer[64];
        while (true) {
            const ssize_t n = ::recv(client.fd(), buffer, sizeof(buffer), 0);
            if (n <= 0) {
                break;
            }
            got.append(buffer, static_cast<std::size_t>(n));
        }
        check(got == payload, "server flushes the echo before closing after half-close");

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
