#pragma once

#include "dfs/block_store.h"

#include <xnet/bootstrap.h>
#include <xnet/event_loop_group.h>

#include <cstddef>
#include <cstdint>
#include <string>

class BlockServer {
public:
    explicit BlockServer(BlockStore& store, std::size_t ioThreads = 2);

    ~BlockServer();

    BlockServer(const BlockServer&) = delete;

    BlockServer& operator=(const BlockServer&) = delete;

    std::uint16_t start(const std::string& host, std::uint16_t port);

    void stop();

    std::uint16_t port() const noexcept;

private:
    BlockStore& store_;
    EventLoopGroup group_;
    ServerBootstrap server_;
    bool started_ = false;
};
