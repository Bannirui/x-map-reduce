#pragma once

#include"net/net.h"

#include<cstdint>
#include<functional>
#include<memory>
#include<string>
#include<utility>
#include<vector>

class Channel;
class EventLoop;
class EventLoopGroup;

using ChannelInitializer = std::function<void(Channel&)>;

class ServerBootstrap {
public:
    ServerBootstrap& group(EventLoopGroup& group);

    ServerBootstrap& group(EventLoopGroup& boss, EventLoopGroup& worker);

    ServerBootstrap& childHandler(ChannelInitializer initializer);

    ServerBootstrap& backlog(int value);

    ServerBootstrap& option(SocketOption option, int value);

    ServerBootstrap& childOption(SocketOption option, int value);

    void bind(const std::string& host, std::uint16_t port);

    std::uint16_t port() const noexcept;

    void close();

private:
    EventLoopGroup* boss_ = nullptr;
    EventLoopGroup* worker_ = nullptr;
    EventLoop* bossLoop_ = nullptr;
    ChannelInitializer childInitializer_;
    std::shared_ptr<Listener> listener_;
    std::uint16_t port_ = 0;
    int backlog_ = -1;
    std::vector<std::pair<SocketOption, int> > parentOptions_;
    std::vector<std::pair<SocketOption, int> > childOptions_;
};

class ClientBootstrap {
public:
    ClientBootstrap& group(EventLoopGroup& group);

    ClientBootstrap& handler(ChannelInitializer initializer);

    ClientBootstrap& option(SocketOption option, int value);

    std::shared_ptr<Channel> connect(const std::string& host, std::uint16_t port);

private:
    EventLoopGroup* group_ = nullptr;
    ChannelInitializer initializer_;
    std::vector<std::pair<SocketOption, int> > options_;
};
