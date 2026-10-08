#pragma once

#include"net/net.h"
#include"protocol/framing.h"
#include"protocol/protocol.h"

#include<cstdint>
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

        net::Connection connection_;
        net::ByteBuffer in_;
    };
} // namespace xmr::client
