#pragma once

#include "dfs/block_store.h"

#include <xnet/bootstrap.h>
#include <xnet/event_loop_group.h>

#include <cstddef>
#include <cstdint>
#include <string>

// 存储节点的存/取接口服务
class BlockServer {
public:
    /// @param store block存储
    /// @param ioThreads 网络模型用了一个reactor组 连接和读写用一个reactor组 多少个EventLoop线程
    explicit BlockServer(BlockStore& store, std::size_t ioThreads = 2);

    ~BlockServer();

    BlockServer(const BlockServer&) = delete;

    BlockServer& operator=(const BlockServer&) = delete;

    /// @brief 把节点的block服务包装成TCP服务开放存取接口
    std::uint16_t start(const std::string& host, std::uint16_t port);

    // 关闭节点上block服务
    void stop();

    // block服务开放在哪个端口上
    std::uint16_t port() const noexcept;

private:
    // 它是存储单元 给它包成TCP服务开放存取接口
    BlockStore& store_;
    // 不是高并发类型 不需要主从reactor 一个reactor就够了
    EventLoopGroup group_;
    // 每个节点是一个BlockStore存储节点 开放存取接口服务
    ServerBootstrap server_;
    // 标识存储节点服务运行状态
    bool started_ = false;
};
