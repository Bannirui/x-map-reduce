#include "handler/worker_data_handler.h"

#include "protocol/frame_codec.h"
#include "protocol/messages.h"
#include "protocol/protocol.h"

#include <xnet/channel_handler_context.h>

#include <algorithm>
#include <cstddef>

WorkerDataHandler::WorkerDataHandler(std::unordered_map<std::size_t, std::vector<std::string> >* store,
                                     std::mutex* storeMutex)
    : store_(store), storeMutex_(storeMutex) {}

void WorkerDataHandler::channelRead(ChannelHandlerContext& ctx, std::any& message) {
    // Pipeline最前面的解码器已经解码成了Frame
    xmr::protocol::Frame* frame = std::any_cast<xmr::protocol::Frame>(&message);
    if (frame == nullptr || frame->header.type != xmr::protocol::MessageType::Pull) {
        // 一定是别的worker来拉reduce分区的情况
        return;
    }
    const auto request = xmr::protocol::Pull::decode(frame->body);
    // 别的worker需要的map中间结果
    std::string blob;
    {
        std::lock_guard<std::mutex> lock(*storeMutex_);
        // 任务号找到worker节点上那个任务的所有分区中间结果
        const auto it = store_->find(request.mapTask);
        if (it != store_->end() && request.partition < it->second.size()) {
            // 指定分区的中间结果
            blob = it->second[request.partition];
        }
    }
    // 指针控制缓冲区 [0...offset)已经发完的 [offset...]等着继续要发的
    std::size_t offset = 0;
    // 不确定这个数据大小 可能太大 就拆
    do {
        // 软限制就是一个消息帧的上限 一个消息帧发不完就多个消息帧
        const std::size_t n = std::min<std::size_t>(xmr::protocol::kChunkBytes, blob.size() - offset);
        xmr::protocol::Chunk data;
        data.offset = offset;
        data.total = blob.size();
        data.payload.assign(blob.begin() + static_cast<std::ptrdiff_t>(offset),
                            blob.begin() + static_cast<std::ptrdiff_t>(offset + n));
        // 标识是拆分帧 TCP对端收到字节流的时候观察到more的时候会组转起来
        const bool more = (offset + n) < blob.size();
        // 这次发出去的一个消息帧
        xmr::protocol::Frame out;
        out.header.type = xmr::protocol::MessageType::Chunk;
        // 回复的是哪个请求
        out.header.requestId = frame->header.requestId;
        out.header.flags = more ? static_cast<std::uint16_t>(xmr::protocol::Flag::More) : 0;
        out.body = data.encode();
        std::any outgoing = std::move(out);
        ctx.write(outgoing);
        // 发完的就丢掉
        offset += n;
    } while (offset < blob.size());
    // 发完就断开TCP连接
    ctx.close();
}