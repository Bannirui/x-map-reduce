#pragma once

#include <xnet/channel_handler.h>

#include <functional>

namespace xmr::protocol {
    class Frame;
}

// worker上控制端口
class WorkerControlHandler : public ChannelInboundHandler {
public:
    /// @param onFrame 处理worker控制端口收到master的 解码器接出来的
    /// @param onInactive 处理worker控制端口跟master连接断掉的情况
    WorkerControlHandler(std::function<void(const xmr::protocol::Frame&)> onFrame, std::function<void()> onInactive);

    void channelRead(ChannelHandlerContext& ctx, std::any& message) override;

    void channelInactive(ChannelHandlerContext& ctx) override;

private:
    std::function<void(const xmr::protocol::Frame&)> onFrame_;
    std::function<void()> onInactive_;
};
