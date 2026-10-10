#include "net/bootstrap.h"
#include "net/channel.h"
#include "net/event_loop.h"
#include "net/event_loop_group.h"
#include "net/net.h"

#include <cstdint>
#include <utility>
#include <vector>

namespace {
    void applyOptions(int fd, const std::vector<std::pair<SocketOption, int> >& options) {
        for (const auto& entry : options) {
            setSocketOption(fd, entry.first, entry.second);
        }
    }
} // namespace

ServerBootstrap& ServerBootstrap::group(EventLoopGroup& group) {
    boss_ = &group;
    worker_ = &group;
    return *this;
}

ServerBootstrap& ServerBootstrap::group(EventLoopGroup& boss, EventLoopGroup& worker) {
    boss_ = &boss;
    worker_ = &worker;
    return *this;
}

ServerBootstrap& ServerBootstrap::childHandler(ChannelInitializer initializer) {
    childInitializer_ = std::move(initializer);
    return *this;
}

ServerBootstrap& ServerBootstrap::backlog(int value) {
    backlog_ = value;
    return *this;
}

ServerBootstrap& ServerBootstrap::option(SocketOption option, int value) {
    parentOptions_.emplace_back(option, value);
    return *this;
}

ServerBootstrap& ServerBootstrap::childOption(SocketOption option, int value) {
    childOptions_.emplace_back(option, value);
    return *this;
}

void ServerBootstrap::bind(const std::string& host, std::uint16_t port) {
    auto listener = std::make_shared<Listener>(host, port, backlog_);
    applyOptions(listener->fd(), parentOptions_);
    setNonBlocking(listener->fd());
    port_ = listener->port();
    listener_ = listener;
    bossLoop_ = boss_->next();
    EventLoopGroup* worker = worker_;
    ChannelInitializer initializer = childInitializer_;
    std::vector<std::pair<SocketOption, int> > childOptions = childOptions_;
    bossLoop_->add(listener->fd(), kReadable, [listener, worker, initializer, childOptions](std::uint32_t) {
        while (true) {
            Connection connection;
            if (listener->acceptNonBlocking(connection) != IoStatus::Ok) {
                break;
            }
            applyOptions(connection.fd(), childOptions);
            auto channel = std::make_shared<Channel>(std::move(connection), worker->next());
            if (initializer) {
                initializer(*channel);
            }
            channel->start();
        }
    });
}

std::uint16_t ServerBootstrap::port() const noexcept {
    return port_;
}

void ServerBootstrap::close() {
    if (!listener_) {
        return;
    }
    auto listener = listener_;
    listener_.reset();
    EventLoop* loop = bossLoop_;
    loop->queueInLoop([loop, listener] {
        loop->remove(listener->fd());
    });
}

ClientBootstrap& ClientBootstrap::group(EventLoopGroup& group) {
    group_ = &group;
    return *this;
}

ClientBootstrap& ClientBootstrap::handler(ChannelInitializer initializer) {
    initializer_ = std::move(initializer);
    return *this;
}

ClientBootstrap& ClientBootstrap::option(SocketOption option, int value) {
    options_.emplace_back(option, value);
    return *this;
}

std::shared_ptr<Channel> ClientBootstrap::connect(const std::string& host, std::uint16_t port) {
    EventLoop* loop = group_->next();
    Connection connection = connectTo(host, port);
    applyOptions(connection.fd(), options_);
    auto channel = std::make_shared<Channel>(std::move(connection), loop);
    if (initializer_) {
        initializer_(*channel);
    }
    channel->start();
    return channel;
}
