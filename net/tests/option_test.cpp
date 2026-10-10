#include "net/bootstrap.h"
#include "net/channel.h"
#include "net/channel_handler.h"
#include "net/channel_handler_context.h"
#include "net/channel_pipeline.h"
#include "net/event_loop_group.h"
#include "net/net.h"

#include <any>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <string>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace {

std::atomic<int> failures{0};

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

class NodelayProbe : public ChannelInboundHandler {
public:
    explicit NodelayProbe(std::shared_ptr<std::promise<int> > promise)
        : promise_(std::move(promise)) {
    }

    void channelActive(ChannelHandlerContext& ctx) override {
        int value = 0;
        socklen_t length = sizeof(value);
        ::getsockopt(ctx.channel().fd(), IPPROTO_TCP, TCP_NODELAY, &value, &length);
        promise_->set_value(value);
    }

private:
    std::shared_ptr<std::promise<int> > promise_;
};

}  // namespace

int main() {
    try {
        EventLoopGroup serverGroup(1);
        serverGroup.start();

        auto promise = std::make_shared<std::promise<int> >();
        auto probe = std::make_shared<NodelayProbe>(promise);

        ServerBootstrap server;
        server.group(serverGroup)
            .childOption(SocketOption::TcpNoDelay, 1)
            .childHandler([probe](Channel& channel) { channel.pipeline().addLast(probe); });
        server.bind("127.0.0.1", 0);

        EventLoopGroup clientGroup(1);
        clientGroup.start();

        ClientBootstrap client;
        client.group(clientGroup).option(SocketOption::TcpNoDelay, 1).handler([](Channel&) {});
        std::shared_ptr<Channel> channel = client.connect("127.0.0.1", server.port());

        std::future<int> future = promise->get_future();
        if (future.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
            check(future.get() == 1, "childOption(TcpNoDelay) applied to the accepted socket");
        } else {
            check(false, "server child channel active was not observed");
        }

        int clientValue = 0;
        socklen_t length = sizeof(clientValue);
        ::getsockopt(channel->fd(), IPPROTO_TCP, TCP_NODELAY, &clientValue, &length);
        check(clientValue == 1, "option(TcpNoDelay) applied to the client socket");

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
