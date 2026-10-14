#pragma once

#include <xnet/channel_handler.h>

#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace xmr::protocol {
    class Frame;
}

namespace xmr::worker {

    // worker上控制端口
    class WorkerControlHandler : public ChannelInboundHandler {
    public:
        /// @param onFrame 处理worker控制端口收到master的 解码器接出来的 回调函数处理
        /// @param onInactive 处理worker控制端口跟master连接断掉的情况
        WorkerControlHandler(std::function<void(const xmr::protocol::Frame&)> onFrame, std::function<void()> onInactive)
            : onFrame_(std::move(onFrame)), onInactive_(std::move(onInactive)) {}

        void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
            xmr::protocol::Frame* frame = std::any_cast<xmr::protocol::Frame>(&message);
            if (frame != nullptr) {
                onFrame_(*frame);
            }
        }

        void channelInactive(ChannelHandlerContext& ctx) override {
            onInactive_();
        }

    private:
        // 收到了消息调用回调函数处理
        std::function<void(const xmr::protocol::Frame&)> onFrame_;
        // TCP断开的话 回调哪个函数
        std::function<void()> onInactive_;
    };

    // worker上数据端口
    class WorkerDataHandler : public ChannelInboundHandler {
    public:
        /// @brief 收到了来自其他worker的pull请求后 把本地的map中间结果发送给其他的worker
        /// @param store 自己map阶段中间结果 key=reduce的分区 value=map中间结果
        /// @param storeMutex 锁
        WorkerDataHandler(std::unordered_map<std::size_t, std::vector<std::string> >* store, std::mutex* storeMutex)
            : store_(store), storeMutex_(storeMutex) {}

        /// @brief 收到了其他worker来拉reduce阶段的分区数据 把本地内存的中间结果给它 完事把TCP连接断开
        void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
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

    private:
        /**
         * 自己这个worker执行的map中间结果 按照R分区好了
         *   - key=哪个任务 属于[0...M]
         *   - value=分区好的map中间结果
         *     - index=分区号 属于[0...R]
         *     - 对应分区的map中间结果
         */
        std::unordered_map<std::size_t, std::vector<std::string> >* store_;
        std::mutex* storeMutex_;
    };
}  // namespace xmr::worker
