#pragma once

#include"net/channel.h"
#include"net/event_loop_group.h"
#include"protocol/framing.h"
#include"protocol/protocol.h"

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

        // 提交成功后等job结果
        SubmitResult wait();

        // 让master关停
        void shutdown();

    private:
        void send(protocol::MessageType type, std::uint32_t requestId,
                  const std::vector<std::uint8_t>& body, std::uint16_t flags = 0);

        protocol::Frame receive();

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
