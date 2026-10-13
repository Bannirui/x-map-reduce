#include "handler/worker_control_handler.h"
#include "protocol/framing.h"

WorkerControlHandler::WorkerControlHandler(std::function<void(const xmr::protocol::Frame&)> onFrame,
                                           std::function<void()> onInactive)
    : onFrame_(std::move(onFrame)), onInactive_(std::move(onInactive)) {}

void WorkerControlHandler::channelRead(ChannelHandlerContext& ctx, std::any& message) {
    xmr::protocol::Frame* frame = std::any_cast<xmr::protocol::Frame>(&message);
    if (frame != nullptr) {
        onFrame_(*frame);
    }
}

void WorkerControlHandler::channelInactive(ChannelHandlerContext& ctx) {
    onInactive_();
}