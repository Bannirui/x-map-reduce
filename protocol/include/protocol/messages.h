#pragma once

#include"protocol/protocol.h"

#include<cstdint>
#include<optional>
#include<string>
#include<vector>

namespace xmr::protocol {
    // worker节点上跑的是什么任务 map函数还是reduce函数
    enum class WorkKind : std::uint8_t {
        None = 0,
        // map任务
        Map = 1u << 0,
        // reduce任务
        Reduce = 1u << 1,
    };

    struct Hello {
        std::uint64_t protocolVersion = kVersion;
        std::string workerId;
        std::uint64_t capabilities = 0;
        std::optional<std::uint64_t> pid;

        std::vector<std::uint8_t> encode() const;

        static Hello decode(const std::vector<std::uint8_t>& body);
    };

    struct HelloAck {
        std::uint64_t protocolVersion = kVersion;
        std::string sessionId;
        std::uint64_t capabilities = 0;
        StatusCode statusCode = StatusCode::Ok;
        std::string reason;

        std::vector<std::uint8_t> encode() const;

        static HelloAck decode(const std::vector<std::uint8_t>& body);
    };

    struct RequestTask {
        std::vector<std::uint8_t> encode() const;

        static RequestTask decode(const std::vector<std::uint8_t>& body);
    };

    struct TaskMessage {
        WorkKind kind = WorkKind::None;
        std::uint64_t taskId = 0;
        std::string job;
        std::uint64_t reducers = 1;
        std::uint64_t maps = 0;
        std::optional<std::string> input;
        // map中间结果按照R分区了 所以如果派发的是reduce任务 还得告诉它去哪些worker上的什么地方接数据 也就是woker的数据端口 mapTask,host,port
        std::vector<std::string> locations;

        std::vector<std::uint8_t> encode() const;

        static TaskMessage decode(const std::vector<std::uint8_t>& body);
    };

    struct InputRequest {
        std::uint64_t taskId = 0;

        std::vector<std::uint8_t> encode() const;

        static InputRequest decode(const std::vector<std::uint8_t>& body);
    };

    struct DataMessage {
        std::uint64_t offset = 0;
        std::optional<std::uint64_t> total;
        std::vector<std::uint8_t> payload;

        std::vector<std::uint8_t> encode() const;

        static DataMessage decode(const std::vector<std::uint8_t>& body);
    };

    struct MapOutput {
        std::uint64_t mapTask = 0;
        std::uint64_t partition = 0;
        std::uint64_t offset = 0;
        std::vector<std::uint8_t> payload;

        std::vector<std::uint8_t> encode() const;

        static MapOutput decode(const std::vector<std::uint8_t>& body);
    };

    struct Fetch {
        std::uint64_t mapTask = 0;
        std::uint64_t partition = 0;
        std::uint64_t offset = 0;

        std::vector<std::uint8_t> encode() const;

        static Fetch decode(const std::vector<std::uint8_t>& body);
    };

    struct ResultMessage {
        std::uint64_t reduceTask = 0;
        std::uint64_t offset = 0;
        std::vector<std::uint8_t> payload;

        std::vector<std::uint8_t> encode() const;

        static ResultMessage decode(const std::vector<std::uint8_t>& body);
    };

    struct Done {
        WorkKind kind = WorkKind::None;
        std::uint64_t taskId = 0;

        std::vector<std::uint8_t> encode() const;

        static Done decode(const std::vector<std::uint8_t>& body);
    };

    struct Fail {
        WorkKind kind = WorkKind::None;
        std::uint64_t taskId = 0;
        StatusCode statusCode = StatusCode::Unknown;
        std::string reason;

        std::vector<std::uint8_t> encode() const;

        static Fail decode(const std::vector<std::uint8_t>& body);
    };

    struct Ping {
        std::uint64_t nonce = 0;

        std::vector<std::uint8_t> encode() const;

        static Ping decode(const std::vector<std::uint8_t>& body);
    };

    struct Pong {
        std::uint64_t nonce = 0;

        std::vector<std::uint8_t> encode() const;

        static Pong decode(const std::vector<std::uint8_t>& body);
    };

    struct Stop {
        std::string reason;

        std::vector<std::uint8_t> encode() const;

        static Stop decode(const std::vector<std::uint8_t>& body);
    };

    struct Shutdown {
        std::vector<std::uint8_t> encode() const;

        static Shutdown decode(const std::vector<std::uint8_t>& body);
    };

    // client向master提交的job
    struct Submit {
        // job名 唯一索引 要找job插件用的
        std::string job;
        // 确定好R map阶段分区要用
        std::uint64_t reducers = 1;
        // 最终job的输出路径
        std::string output;
        // map阶段的key 也就是文件名 决定了M
        std::vector<std::string> inputs;
        // 插件的内容哈希 为空表示worker本地已预加载
        std::string pluginHash;

        std::vector<std::uint8_t> encode() const;

        static Submit decode(const std::vector<std::uint8_t>& body);
    };

    // client向master提交job后 后面master需要让client把job的插件二进制和map阶段的input走数据端口传进来
    struct SubmitAck {
        StatusCode statusCode = StatusCode::Ok;
        std::string reason;
        // master的数据端口 供client上传插件和input数据
        std::string dataHost;
        std::uint64_t dataPort = 0;

        std::vector<std::uint8_t> encode() const;

        static SubmitAck decode(const std::vector<std::uint8_t>& body);
    };

    struct SubmitResult {
        StatusCode statusCode = StatusCode::Ok;
        std::string output;
        std::string reason;

        std::vector<std::uint8_t> encode() const;

        static SubmitResult decode(const std::vector<std::uint8_t>& body);
    };

    // worker之间拉中间结果 请求某个map任务的某个分区
    struct Pull {
        std::uint64_t mapTask = 0;
        std::uint64_t partition = 0;

        std::vector<std::uint8_t> encode() const;

        static Pull decode(const std::vector<std::uint8_t>& body);
    };

    // worker把自己的数据面监听端口报给master
    struct DataAddress {
        std::uint64_t port = 0;

        std::vector<std::uint8_t> encode() const;

        static DataAddress decode(const std::vector<std::uint8_t>& body);
    };

    // 插件二进制分块传输 client->master 或 master->worker
    struct Plugin {
        std::string hash;
        std::string job;
        std::uint64_t offset = 0;
        std::vector<std::uint8_t> payload;

        std::vector<std::uint8_t> encode() const;

        static Plugin decode(const std::vector<std::uint8_t>& body);
    };

    struct PluginAck {
        std::string hash;
        bool ok = false;
        std::string reason;

        std::vector<std::uint8_t> encode() const;

        static PluginAck decode(const std::vector<std::uint8_t>& body);
    };

    // worker从master数据面拉map输入
    struct PullInput {
        std::uint64_t taskId = 0;

        std::vector<std::uint8_t> encode() const;

        static PullInput decode(const std::vector<std::uint8_t>& body);
    };

    // worker从master数据面拉插件
    struct PullPlugin {
        std::string hash;

        std::vector<std::uint8_t> encode() const;

        static PullPlugin decode(const std::vector<std::uint8_t>& body);
    };

    // master把自己的数据面监听端口告诉worker
    struct MasterData {
        std::uint64_t port = 0;

        std::vector<std::uint8_t> encode() const;

        static MasterData decode(const std::vector<std::uint8_t>& body);
    };

    // master告诉worker去拉哪个插件
    struct NeedPlugin {
        std::string hash;
        std::string job;

        std::vector<std::uint8_t> encode() const;

        static NeedPlugin decode(const std::vector<std::uint8_t>& body);
    };

    // client在数据面把map输入分块传上来
    struct InputBlob {
        std::uint64_t index = 0;
        std::uint64_t offset = 0;
        std::vector<std::uint8_t> payload;

        std::vector<std::uint8_t> encode() const;

        static InputBlob decode(const std::vector<std::uint8_t>& body);
    };

    // worker上报任务进度 fraction是0..100
    struct Progress {
        WorkKind kind = WorkKind::None;
        std::uint64_t taskId = 0;
        std::uint64_t fraction = 0;

        std::vector<std::uint8_t> encode() const;

        static Progress decode(const std::vector<std::uint8_t>& body);
    };

    // master让worker取消某个重复attempt
    struct Cancel {
        WorkKind kind = WorkKind::None;
        std::uint64_t taskId = 0;

        std::vector<std::uint8_t> encode() const;

        static Cancel decode(const std::vector<std::uint8_t>& body);
    };
} // namespace xmr::protocol
