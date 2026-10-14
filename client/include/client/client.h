#pragma once

#include"protocol/framing.h"
#include"protocol/protocol.h"

#include <xnet/channel.h>
#include <xnet/event_loop_group.h>

#include<condition_variable>
#include<cstdint>
#include<deque>
#include<memory>
#include<mutex>
#include<string>
#include<vector>

namespace xmr::client {
    // 一次job提交的请求
    struct SubmitRequest {
        std::string job;
        std::uint64_t reducers = 1;
        std::string output;
        std::vector<std::string> inputs;
        // 插件.so的本地路径 由client读出来上传 为空表示worker本地已预加载
        std::string pluginPath;
    };

    // job执行结果
    struct SubmitResult {
        protocol::StatusCode status = protocol::StatusCode::Unknown;
        std::string output;
        std::string reason;
    };

    // master的客户端 连接 提交 等结果 关停
    class Client {
    public:
        // endpoint是host:port
        explicit Client(const std::string& endpoint);

        ~Client();

        Client(const Client&) = delete;

        Client& operator=(const Client&) = delete;

        /**
         * 发送Submit提交任务 等SubmitAck
         * @return false表示被master拒了
         */
        bool submit(const SubmitRequest& request, std::string& reason);

        /**
         * 把本地文件切块直接写入DFS的worker 再提交DFS路径建立命名空间
         * @param localPath 本地文件
         * @param dfsPath DFS路径(后续提交job用)
         * @return false表示入库失败
         */
        bool put(const std::string& localPath, const std::string& dfsPath, std::string& reason);

        // 提交成功后等job结果
        SubmitResult wait();

        // 让master关停
        void shutdown();

    private:
        void send(protocol::MessageType type, std::uint32_t requestId,
                  const std::vector<std::uint8_t>& body, std::uint16_t flags = 0);

        template <class Message>
        void send(std::uint32_t requestId, const Message& message, std::uint16_t flags = 0) {
            send(Message::kType, requestId, message.encode(), flags);
        }

        protocol::Frame receive();

        bool uploadBlocks(const std::vector<std::vector<std::uint8_t> >& inputs,
                          std::vector<std::vector<std::string> >& blockIds, std::string& reason);

        void onFrame(protocol::Frame frame);

        void onDisconnected();

        EventLoopGroup group_;
        std::shared_ptr<Channel> control_;
        std::mutex mutex_;
        std::condition_variable received_;
        std::deque<protocol::Frame> inbox_;
        bool disconnected_ = false;
    };
} // namespace xmr::client
