#pragma once

#include "master/coordinator.h"

#include <xnet/channel_handler.h>
#include <xnet/channel_handler_context.h>

#include <any>
#include <memory>

namespace xmr::master {
    class ConnectionLifecycleHandler : public ChannelInboundHandler {
    public:
        /// @brief 连接断开的是要处理
        /// @param state 连接
        /// @param coordinator 负责处理
        /// @param coordinatorLoop coordinator让连接关联的线程处理
        ConnectionLifecycleHandler(std::shared_ptr<ConnState> state, Coordinator* coordinator,
                                   EventLoop* coordinatorLoop)
            : state_(std::move(state)), coordinator_(coordinator), coordinatorLoop_(coordinatorLoop) {}

        void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
            ctx.fireChannelRead(message);
        }

        // 控制端口有连接断开了
        void channelInactive(ChannelHandlerContext& ctx) override {
            auto state = state_;
            coordinatorLoop_->runInLoop([state, this] {
                // 处理连接控制
                coordinator_->onDisconnect(state->id);
            });
        }

    private:
        // master控制端口上的连接 连接断开的时候处理它
        std::shared_ptr<ConnState> state_;
        // coordinator让连接专属的线程处理连接断开
        Coordinator* coordinator_;
        // 连接映射的线程 连接断开的时候这个线程负责处理
        EventLoop* coordinatorLoop_;
    };

    /**
     * master节点上服务端TCP的读写处理 包括
     *   - 控制端口
     *   - 数据端口
     */
    class MasterConnHandler : public ChannelInboundHandler {
    public:
        MasterConnHandler(std::shared_ptr<ConnState> state, Coordinator* coordinator, EventLoop* coordinatorLoop)
            : state_(std::move(state)), coordinator_(coordinator), coordinatorLoop_(coordinatorLoop) {}

        // TCP对端有数据发来
        void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
            // Pipeline上解码器已经处理好了流转到这
            xmr::protocol::Frame* frame = std::any_cast<xmr::protocol::Frame>(&message);
            if (frame == nullptr) {
                return;
            }
            auto state = state_;
            coordinatorLoop_->runInLoop([state, frame = *frame, this]() mutable {
                // 有人给master发消息了 master对不同的消息处理
                coordinator_->onFrame(state->id, frame);
            });
        }

        // 连接断开
        void channelInactive(ChannelHandlerContext& ctx) override {
            auto state = state_;
            coordinatorLoop_->runInLoop([state, this] {
                coordinator_->onDisconnect(state->id);
            });
        }

    private:
        std::shared_ptr<ConnState> state_;
        Coordinator* coordinator_;
        EventLoop* coordinatorLoop_;
    };
}  // namespace xmr::master
