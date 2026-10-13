#pragma once

#include <xnet/channel_handler.h>

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

// worker上数据端口
class WorkerDataHandler : public ChannelInboundHandler {
public:
    /// @brief 收到了来自其他worker的pull请求后 把本地的map中间结果发送给其他的worker
    /// @param store 自己map阶段中间结果 key=reduce的分区 value=map中间结果
    /// @param storeMutex 锁
    WorkerDataHandler(std::unordered_map<std::size_t, std::vector<std::string> >* store, std::mutex* storeMutex);

    /// @brief 收到了其他worker来拉reduce阶段的分区数据 把本地内存的中间结果给它 完事把TCP连接断开
    void channelRead(ChannelHandlerContext& ctx, std::any& message) override;

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