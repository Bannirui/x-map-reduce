#pragma once

#include "protocol/protocol.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace xmr::protocol {
    // worker节点上跑的是什么任务 map函数还是reduce函数
    enum class WorkKind : std::uint8_t {
        None = 0,
        // map任务
        Map = 1u,
        // reduce任务
        Reduce = 1u << 1,
    };

    struct Hello {
        static constexpr MessageType kType = MessageType::Hello;
        std::uint64_t protocolVersion = kVersion;
        std::string workerId;
        std::uint64_t capabilities = 0;
        std::optional<std::uint64_t> pid;

        std::vector<std::uint8_t> encode() const;

        static Hello decode(const std::vector<std::uint8_t>& body);
    };

    struct RequestTask {
        static constexpr MessageType kType = MessageType::RequestTask;
        std::vector<std::uint8_t> encode() const;

        static RequestTask decode(const std::vector<std::uint8_t>& body);
    };

    struct TaskAssignment {
        static constexpr MessageType kType = MessageType::TaskAssignment;
        WorkKind kind = WorkKind::None;
        std::uint64_t taskId = 0;
        std::string job;
        std::uint64_t reducers = 1;
        std::uint64_t maps = 0;
        std::optional<std::string> input;
        // map中间结果按照R分区了 所以如果派发的是reduce任务 还得告诉它去哪些worker上的什么地方接数据
        // 也就是woker的数据端口 mapTask,host,port
        std::vector<std::string> locations;
        std::vector<std::string> blocks;

        std::vector<std::uint8_t> encode() const;

        static TaskAssignment decode(const std::vector<std::uint8_t>& body);
    };

    struct Chunk {
        static constexpr MessageType kType = MessageType::Chunk;
        std::uint64_t offset = 0;
        std::optional<std::uint64_t> total;
        std::vector<std::uint8_t> payload;

        std::vector<std::uint8_t> encode() const;

        static Chunk decode(const std::vector<std::uint8_t>& body);
    };

    struct ReduceResult {
        static constexpr MessageType kType = MessageType::ReduceResult;
        std::uint64_t reduceTask = 0;
        std::uint64_t offset = 0;
        std::vector<std::uint8_t> payload;

        std::vector<std::uint8_t> encode() const;

        static ReduceResult decode(const std::vector<std::uint8_t>& body);
    };

    struct Done {
        static constexpr MessageType kType = MessageType::Done;
        WorkKind kind = WorkKind::None;
        std::uint64_t taskId = 0;

        std::vector<std::uint8_t> encode() const;

        static Done decode(const std::vector<std::uint8_t>& body);
    };

    struct Fail {
        static constexpr MessageType kType = MessageType::Fail;
        WorkKind kind = WorkKind::None;
        std::uint64_t taskId = 0;
        StatusCode statusCode = StatusCode::Unknown;
        std::string reason;

        std::vector<std::uint8_t> encode() const;

        static Fail decode(const std::vector<std::uint8_t>& body);
    };

    struct Ping {
        static constexpr MessageType kType = MessageType::Ping;
        std::uint64_t nonce = 0;

        std::vector<std::uint8_t> encode() const;

        static Ping decode(const std::vector<std::uint8_t>& body);
    };

    struct Pong {
        static constexpr MessageType kType = MessageType::Pong;
        std::uint64_t nonce = 0;

        std::vector<std::uint8_t> encode() const;

        static Pong decode(const std::vector<std::uint8_t>& body);
    };

    struct JobFinished {
        static constexpr MessageType kType = MessageType::JobFinished;
        std::string reason;

        std::vector<std::uint8_t> encode() const;

        static JobFinished decode(const std::vector<std::uint8_t>& body);
    };

    // 关机
    struct Shutdown {
        static constexpr MessageType kType = MessageType::Shutdown;
        std::vector<std::uint8_t> encode() const;

        static Shutdown decode(const std::vector<std::uint8_t>& body);
    };

    // client向master提交的job
    struct Submit {
        static constexpr MessageType kType = MessageType::Submit;
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
        static constexpr MessageType kType = MessageType::SubmitAck;
        StatusCode statusCode = StatusCode::Ok;
        std::string reason;
        // master的数据端口 供client上传插件和input数据
        std::string dataHost;
        std::uint64_t dataPort = 0;

        std::vector<std::uint8_t> encode() const;

        static SubmitAck decode(const std::vector<std::uint8_t>& body);
    };

    struct SubmitResult {
        static constexpr MessageType kType = MessageType::SubmitResult;
        StatusCode statusCode = StatusCode::Ok;
        std::string output;
        std::string reason;

        std::vector<std::uint8_t> encode() const;

        static SubmitResult decode(const std::vector<std::uint8_t>& body);
    };

    // worker之间拉中间结果 请求某个map任务的某个分区
    struct Pull {
        static constexpr MessageType kType = MessageType::Pull;
        // 哪个map任务 属于[0...M]
        std::uint64_t mapTask = 0;
        // 哪个R分区 属于[0...R]
        std::uint64_t partition = 0;

        std::vector<std::uint8_t> encode() const;

        static Pull decode(const std::vector<std::uint8_t>& body);
    };

    // worker把自己的数据面监听端口报给master
    struct DataAddress {
        static constexpr MessageType kType = MessageType::DataAddress;
        // 数据端口 用来worker-worker之间拉map中间结果
        std::uint64_t port = 0;
        // dfs block server的端口
        std::uint64_t blockPort = 0;

        std::vector<std::uint8_t> encode() const;

        static DataAddress decode(const std::vector<std::uint8_t>& body);
    };

    // 插件二进制分块传输 client->master 或 master->worker
    struct Plugin {
        static constexpr MessageType kType = MessageType::Plugin;
        std::string hash;
        std::string job;
        std::uint64_t offset = 0;
        std::vector<std::uint8_t> payload;

        std::vector<std::uint8_t> encode() const;

        static Plugin decode(const std::vector<std::uint8_t>& body);
    };

    struct PluginAck {
        static constexpr MessageType kType = MessageType::PluginAck;
        std::string hash;
        bool ok = false;
        std::string reason;

        std::vector<std::uint8_t> encode() const;

        static PluginAck decode(const std::vector<std::uint8_t>& body);
    };

    // worker从master数据面拉map输入
    struct PullInput {
        static constexpr MessageType kType = MessageType::PullInput;
        std::uint64_t taskId = 0;

        std::vector<std::uint8_t> encode() const;

        static PullInput decode(const std::vector<std::uint8_t>& body);
    };

    // worker从master数据面拉插件
    struct PullPlugin {
        static constexpr MessageType kType = MessageType::PullPlugin;
        std::string hash;

        std::vector<std::uint8_t> encode() const;

        static PullPlugin decode(const std::vector<std::uint8_t>& body);
    };

    // master把自己的数据面监听端口告诉worker
    struct MasterData {
        static constexpr MessageType kType = MessageType::MasterData;
        std::uint64_t port = 0;

        std::vector<std::uint8_t> encode() const;

        static MasterData decode(const std::vector<std::uint8_t>& body);
    };

    // master告诉worker去拉哪个插件
    struct FetchPlugin {
        static constexpr MessageType kType = MessageType::FetchPlugin;
        std::string hash;
        std::string job;

        std::vector<std::uint8_t> encode() const;

        static FetchPlugin decode(const std::vector<std::uint8_t>& body);
    };

    // worker上报任务进度 fraction是0..100
    struct Progress {
        static constexpr MessageType kType = MessageType::Progress;
        WorkKind kind = WorkKind::None;
        std::uint64_t taskId = 0;
        std::uint64_t fraction = 0;

        std::vector<std::uint8_t> encode() const;

        static Progress decode(const std::vector<std::uint8_t>& body);
    };

    // master让worker取消某个重复attempt
    struct Cancel {
        static constexpr MessageType kType = MessageType::Cancel;
        WorkKind kind = WorkKind::None;
        std::uint64_t taskId = 0;

        std::vector<std::uint8_t> encode() const;

        static Cancel decode(const std::vector<std::uint8_t>& body);
    };

    // client把输入切成的block清单报给master 每项是"inputIndex,blockIndex,blockId,size"
    struct BlockManifest {
        static constexpr MessageType kType = MessageType::BlockManifest;
        std::vector<std::string> blocks;

        std::vector<std::uint8_t> encode() const;

        static BlockManifest decode(const std::vector<std::uint8_t>& body);
    };

    // master回复每个block的副本位置 每项是"blockId,host,port"
    struct BlockPlacement {
        static constexpr MessageType kType = MessageType::BlockPlacement;
        StatusCode statusCode = StatusCode::Ok;
        std::string reason;
        std::vector<std::string> replicas;

        std::vector<std::uint8_t> encode() const;

        static BlockPlacement decode(const std::vector<std::uint8_t>& body);
    };

    // client把block都写完的通知
    struct UploadDone {
        static constexpr MessageType kType = MessageType::UploadDone;
        std::string job;

        std::vector<std::uint8_t> encode() const;

        static UploadDone decode(const std::vector<std::uint8_t>& body);
    };

    // master让worker从别的副本复制一个block过来
    struct ReplicateBlock {
        static constexpr MessageType kType = MessageType::ReplicateBlock;
        std::string blockId;
        std::string sourceHost;
        std::uint64_t sourcePort = 0;

        std::vector<std::uint8_t> encode() const;

        static ReplicateBlock decode(const std::vector<std::uint8_t>& body);
    };

    // worker上报自己本地持有的block清单
    struct BlockReport {
        static constexpr MessageType kType = MessageType::BlockReport;
        std::vector<std::string> blocks;

        std::vector<std::uint8_t> encode() const;

        static BlockReport decode(const std::vector<std::uint8_t>& body);
    };

    // master让worker删掉某个不再需要的block
    struct DeleteBlock {
        static constexpr MessageType kType = MessageType::DeleteBlock;
        std::string blockId;

        std::vector<std::uint8_t> encode() const;

        static DeleteBlock decode(const std::vector<std::uint8_t>& body);
    };

    // client把一个DFS路径对应的block清单提交给master 建立 path -> [blockId] 命名空间
    struct CommitFile {
        static constexpr MessageType kType = MessageType::CommitFile;
        // DFS路径
        std::string path;
        // 这个文件按顺序切成的blockId列表
        std::vector<std::string> blocks;

        std::vector<std::uint8_t> encode() const;

        static CommitFile decode(const std::vector<std::uint8_t>& body);
    };

    // master对CommitFile的应答
    struct CommitAck {
        static constexpr MessageType kType = MessageType::CommitAck;
        StatusCode statusCode = StatusCode::Ok;
        std::string reason;

        std::vector<std::uint8_t> encode() const;

        static CommitAck decode(const std::vector<std::uint8_t>& body);
    };

    template <class Message>
    inline constexpr MessageType messageTypeOf = Message::kType;
}  // namespace xmr::protocol
