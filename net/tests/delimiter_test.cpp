#include "net/bootstrap.h"
#include "net/buffer.h"
#include "net/channel.h"
#include "net/channel_handler.h"
#include "net/channel_handler_context.h"
#include "net/channel_pipeline.h"
#include "net/codec/delimiter_based_frame_decoder.h"
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
#include <vector>

namespace {

std::atomic<int> failures{0};

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

class FrameCollector : public ChannelInboundHandler {
public:
    FrameCollector(std::size_t want, std::shared_ptr<std::promise<std::vector<std::string> > > promise)
        : want_(want), promise_(std::move(promise)) {
    }

    void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
        ByteBuffer* buffer = std::any_cast<ByteBuffer>(&message);
        if (buffer == nullptr) {
            return;
        }
        frames_.emplace_back(reinterpret_cast<const char*>(buffer->data()), buffer->size());
        if (frames_.size() >= want_) {
            promise_->set_value(frames_);
        }
    }

private:
    std::size_t want_;
    std::vector<std::string> frames_;
    std::shared_ptr<std::promise<std::vector<std::string> > > promise_;
};

}  // namespace

int main() {
    try {
        EventLoopGroup serverGroup(1);
        serverGroup.start();

        auto promise = std::make_shared<std::promise<std::vector<std::string> > >();
        auto collector = std::make_shared<FrameCollector>(2, promise);

        ServerBootstrap server;
        server.group(serverGroup).childHandler([collector](Channel& channel) {
            channel.pipeline().addLast(std::make_shared<DelimiterBasedFrameDecoder>('\n', 1024));
            channel.pipeline().addLast(collector);
        });
        server.bind("127.0.0.1", 0);

        Connection client = connectTo("127.0.0.1", server.port());
        const std::string payload = "alpha\nbeta\n";
        sendAll(client.fd(), payload.data(), payload.size());

        std::future<std::vector<std::string> > future = promise->get_future();
        if (future.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
            const std::vector<std::string> frames = future.get();
            check(frames.size() == 2, "two frames decoded");
            check(!frames.empty() && frames[0] == "alpha", "first frame");
            check(frames.size() > 1 && frames[1] == "beta", "second frame");
        } else {
            check(false, "delimiter frames were not received in time");
        }

        server.close();
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
