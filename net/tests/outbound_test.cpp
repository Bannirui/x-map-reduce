#include "net/channel.h"
#include "net/channel_handler.h"
#include "net/channel_handler_context.h"
#include "net/channel_pipeline.h"
#include "net/event_loop.h"
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

#include <sys/socket.h>
#include <unistd.h>

namespace {

std::atomic<int> failures{0};

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

class TrackingOutbound : public ChannelOutboundHandler {
public:
    TrackingOutbound(std::shared_ptr<std::promise<void> > closed,
                     std::shared_ptr<std::atomic<int> > writes,
                     std::shared_ptr<std::atomic<bool> > flushed)
        : closed_(std::move(closed)), writes_(std::move(writes)), flushed_(std::move(flushed)) {
    }

    void write(ChannelHandlerContext& ctx, std::any& message) override {
        ++*writes_;
        ctx.write(message);
    }

    void flush(ChannelHandlerContext& ctx) override {
        *flushed_ = true;
        ctx.flush();
    }

    void close(ChannelHandlerContext& ctx) override {
        closed_->set_value();
        ctx.close();
    }

private:
    std::shared_ptr<std::promise<void> > closed_;
    std::shared_ptr<std::atomic<int> > writes_;
    std::shared_ptr<std::atomic<bool> > flushed_;
};

}  // namespace

int main() {
    try {
        int fds[2];
        check(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0, "socketpair created");

        EventLoop loop;
        std::thread loopThread([&] { loop.run(); });

        auto closed = std::make_shared<std::promise<void> >();
        auto writes = std::make_shared<std::atomic<int> >(0);
        auto flushed = std::make_shared<std::atomic<bool> >(false);

        Connection connection(fds[0]);
        auto channel = std::make_shared<Channel>(std::move(connection), &loop);
        channel->pipeline().addLast(std::make_shared<TrackingOutbound>(closed, writes, flushed));
        channel->start();

        channel->write(std::vector<std::uint8_t>{1, 2, 3});
        channel->flush();
        channel->close();

        std::future<void> future = closed->get_future();
        check(future.wait_for(std::chrono::seconds(2)) == std::future_status::ready,
              "close traverses the outbound handler chain");
        check(writes->load() >= 1, "write traverses the outbound handler chain");
        check(flushed->load(), "flush traverses the outbound handler chain");

        loop.stop();
        loopThread.join();
        ::close(fds[1]);
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
