#include "net/channel_handler.h"
#include "net/channel_handler_context.h"

#include <any>

void ChannelOutboundHandler::write(ChannelHandlerContext& ctx, std::any& message) {
    ctx.write(message);
}

void ChannelOutboundHandler::flush(ChannelHandlerContext& ctx) {
    ctx.flush();
}

void ChannelOutboundHandler::close(ChannelHandlerContext& ctx) {
    ctx.close();
}
