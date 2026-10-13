#include "dfs/block_server.h"

#include "dfs/dfs_codec.h"
#include "dfs/dfs_messages.h"

#include <xnet/channel.h>
#include <xnet/channel_handler.h>
#include <xnet/channel_handler_context.h>
#include <xnet/channel_pipeline.h>

#include <algorithm>
#include <any>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

class DfsBlockServerHandler : public ChannelInboundHandler {
public:
    explicit DfsBlockServerHandler(BlockStore* store) : store_(store) {}

    void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
        DfsFrame* frame = std::any_cast<DfsFrame>(&message);
        if (frame == nullptr) {
            return;
        }
        if (frame->header.type == DfsMessageType::WriteBlock) {
            handleWrite(ctx, *frame);
            return;
        }
        if (frame->header.type == DfsMessageType::ReadBlock) {
            handleRead(ctx, *frame);
        }
    }

private:
    void handleWrite(ChannelHandlerContext& ctx, const DfsFrame& frame) {
        const WriteBlock chunk = WriteBlock::decode(frame.body);
        if (chunk.offset != writeBuffer_.size()) {
            writeBuffer_.clear();
            sendWriteAck(ctx, frame.header.requestId, chunk.blockId, false, "bad offset");
            return;
        }
        writeBuffer_.insert(writeBuffer_.end(), chunk.payload.begin(), chunk.payload.end());
        if ((frame.header.flags & static_cast<std::uint16_t>(DfsFlag::More)) != 0) {
            return;
        }
        const bool ok = store_->put(chunk.blockId, writeBuffer_);
        writeBuffer_.clear();
        sendWriteAck(ctx, frame.header.requestId, chunk.blockId, ok, ok ? std::string() : "put failed");
    }

    void handleRead(ChannelHandlerContext& ctx, const DfsFrame& frame) {
        const ReadBlock request = ReadBlock::decode(frame.body);
        const auto data = store_->get(request.blockId);
        if (!data.has_value()) {
            BlockData out;
            out.blockId = request.blockId;
            out.found = false;
            sendBlockData(ctx, frame.header.requestId, out, false);
            return;
        }
        std::size_t offset = 0;
        do {
            const std::size_t n = std::min<std::size_t>(kDfsChunkBytes, data->size() - offset);
            BlockData out;
            out.blockId = request.blockId;
            out.offset = offset;
            out.found = true;
            out.payload.assign(data->begin() + static_cast<std::ptrdiff_t>(offset),
                               data->begin() + static_cast<std::ptrdiff_t>(offset + n));
            const bool more = offset + n < data->size();
            sendBlockData(ctx, frame.header.requestId, out, more);
            offset += n;
        } while (offset < data->size());
    }

    void sendWriteAck(ChannelHandlerContext& ctx, std::uint32_t rid, const std::string& blockId, bool ok,
                      const std::string& reason) {
        WriteAck ack;
        ack.blockId = blockId;
        ack.ok = ok;
        ack.reason = reason;
        sendFrame(ctx, DfsMessageType::WriteAck, rid, ack.encode(), 0);
    }

    void sendBlockData(ChannelHandlerContext& ctx, std::uint32_t rid, const BlockData& data, bool more) {
        sendFrame(ctx, DfsMessageType::BlockData, rid, data.encode(),
                  more ? static_cast<std::uint16_t>(DfsFlag::More) : 0);
    }

    void sendFrame(ChannelHandlerContext& ctx, DfsMessageType type, std::uint32_t rid,
                   const std::vector<std::uint8_t>& body, std::uint16_t flags) {
        DfsFrame out;
        out.header.type = type;
        out.header.requestId = rid;
        out.header.flags = flags;
        out.body = body;
        std::any outgoing = std::move(out);
        ctx.write(outgoing);
    }

    BlockStore* store_;
    std::vector<std::uint8_t> writeBuffer_;
};

BlockServer::BlockServer(BlockStore& store, std::size_t ioThreads)
    : store_(store), group_(ioThreads == 0 ? 1 : ioThreads) {}

BlockServer::~BlockServer() {
    stop();
}

std::uint16_t BlockServer::start(const std::string& host, std::uint16_t port) {
    group_.start();
    server_.group(group_).childHandler([this](Channel& channel) {
        channel.pipeline().addLast(std::make_shared<DfsFrameEncoder>());
        channel.pipeline().addLast(std::make_shared<DfsFrameDecoder>());
        channel.pipeline().addLast(std::make_shared<DfsBlockServerHandler>(&store_));
    });
    server_.bind(host, port);
    started_ = true;
    return server_.port();
}

void BlockServer::stop() {
    if (!started_) {
        return;
    }
    server_.close();
    group_.stop();
    started_ = false;
}

std::uint16_t BlockServer::port() const noexcept {
    return server_.port();
}
