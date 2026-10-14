#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace xmr::protocol {
    // 协议头顶格4字节 魔数校验
    inline constexpr std::array<std::uint8_t, 4> kMagic{'X', 'M', 'R', 'P'};
    inline constexpr std::uint8_t kVersion = 3;
    // 协议=协议头16字节+body
    inline constexpr std::size_t kHeaderSize = 16;
    // 约定一次消息传的大小上限64MB
    inline constexpr std::uint32_t kMaxPayloadBytes = 64u * 1024u * 1024u;
    /**
     * 一个消息帧的硬限制是64M 当数据真的挺大的时候但是<64M 还是要拆分多个消息帧出去
     * 防止EventLoop发送大数据占用太长时间导致其他的TCP读写没办法及时处理 所以设置个软限制
     * 判断数据比这个软限制大的就拆分
     */
    inline constexpr std::uint32_t kChunkBytes = 1u << 20;

    // 通信消息的种类
    enum class MessageCategory : std::uint8_t {
        // 生命周期类
        Lifecycle = 1,
        // 控制面任务调度
        JobControl = 2,
        // 数据面 Job插件/map输入/map中间结果
        Data = 3,
        // 作业提交与状态
        JobSubmit = 4,
        // Block上传与副本管理
        Dfs = 5,
    };

    /// @param category 消息的种类
    /// @param code 消息的code
    /// @return 消息枚举类型编码 8bit=[高4位种类, 低4位类型]
    constexpr std::uint8_t msgType(MessageCategory category, std::uint8_t code) {
        return static_cast<std::uint8_t>((static_cast<std::uint8_t>(category) << 4) | (code & 0x0F));
    }

    // 消息编码
    enum class MessageType : std::uint8_t {
        // worker跟master建立连接
        Hello = msgType(MessageCategory::Lifecycle, 1),
        // 心跳
        Ping = msgType(MessageCategory::Lifecycle, 2),
        Pong = msgType(MessageCategory::Lifecycle, 3),
        // client向master提交job的时候指定了--shutdown语义是任务执行完后关停master
        Shutdown = msgType(MessageCategory::Lifecycle, 4),
        // worker把自己的数据面监听端口报给master
        DataAddress = msgType(MessageCategory::Lifecycle, 5),
        // master的数据端口信息 master告诉worker自己端口是哪个
        MasterData = msgType(MessageCategory::Lifecycle, 6),

        // worker告诉master自己在等待任务
        RequestTask = msgType(MessageCategory::JobControl, 1),
        // master向worker派发任务
        TaskAssignment = msgType(MessageCategory::JobControl, 2),
        // 任务成功
        Done = msgType(MessageCategory::JobControl, 3),
        // 任务失败
        Fail = msgType(MessageCategory::JobControl, 4),
        // job的进度报告
        Progress = msgType(MessageCategory::JobControl, 5),
        // master让worker取消某个重复attempt
        Cancel = msgType(MessageCategory::JobControl, 6),
        // master告诉worker去拉插件
        FetchPlugin = msgType(MessageCategory::JobControl, 7),
        // master通知job结束了
        JobFinished = msgType(MessageCategory::JobControl, 8),

        // 配Flag::More 通用大blob流式分块
        Chunk = msgType(MessageCategory::Data, 1),
        // reduce结果
        ReduceResult = msgType(MessageCategory::Data, 2),
        // worker间拉某个map任务的分区 worker执行reduce阶段的时候只去别的worker节点拉需要分区的数据
        Pull = msgType(MessageCategory::Data, 3),
        // client传Job插件给master
        Plugin = msgType(MessageCategory::Data, 4),
        // worker加载插件的结果
        PluginAck = msgType(MessageCategory::Data, 5),
        // worker跟master要map阶段的input
        PullInput = msgType(MessageCategory::Data, 6),
        // worker跟master要插件
        PullPlugin = msgType(MessageCategory::Data, 7),

        // 提交job
        Submit = msgType(MessageCategory::JobSubmit, 1),
        // 受理job
        SubmitAck = msgType(MessageCategory::JobSubmit, 2),
        // job的最终结果
        SubmitResult = msgType(MessageCategory::JobSubmit, 3),

        // client把切好的block清单报给master 请master分配副本位置
        BlockManifest = msgType(MessageCategory::Dfs, 1),
        // master把每个block的副本位置回给client
        BlockPlacement = msgType(MessageCategory::Dfs, 2),
        // client把block都写完的通知
        UploadDone = msgType(MessageCategory::Dfs, 3),
        // master让worker从别的副本复制一个block过来
        ReplicateBlock = msgType(MessageCategory::Dfs, 4),
        // worker上报自己本地持有的block清单
        BlockReport = msgType(MessageCategory::Dfs, 5),
        // master让worker删掉某个不再需要的block
        DeleteBlock = msgType(MessageCategory::Dfs, 6),
    };

    /// @param type 消息编码
    /// @return 消息编码属于哪种种类
    constexpr MessageCategory categoryOf(MessageType type) {
        return static_cast<MessageCategory>(static_cast<std::uint8_t>(type) >> 4);
    }

    /// @return 消息编码对应的值 低4位
    constexpr std::uint8_t messageCode(MessageType type) {
        return static_cast<std::uint8_t>(type) & 0x0F;
    }

    enum class WireType : std::uint8_t {
        Varint = 0,
        Bytes = 1,
        Fixed64 = 2,
    };

    enum class StatusCode : std::uint32_t {
        Ok = 0,
        Unknown = 1,
        InvalidArgument = 2,
        NotFound = 3,
        Internal = 4,
        Canceled = 5,
        Unavailable = 6,
        UnsupportedVersion = 7,
    };

    // 协议头里面flag字段2字节 现在只用低3位 其他bit位用0占位当保留位
    enum class Flag : std::uint16_t {
        // 分块未结束 后续还有同逻辑消息 用于大blob流式
        More = 1u << 0,
        // body已压缩 当前未实现 预留
        Compressed = 1u << 1,
        // 本条为错误应答 body带状态码/原因
        Error = 1u << 2,
    };

    class ProtocolError : public std::runtime_error {
    public:
        explicit ProtocolError(const std::string& what) : std::runtime_error(what) {
            // todo
        }
    };

    /**
     * 消息头 协议=协议头(传递的时候把协议头编码成16字节)+body
     *
     * 协议头的布局
     * | offset | size | field | 说明 |
     * |-------:|-----:|-------|------|
     * | 0      |   4  |`magic`| 固定 `'X','M','R','P'` (`0x584D5250`) |
     * | 4      |   1  |`version`    | 协议版本 固定值1 |
     * | 5      |   1  |`type`       | 消息类型 |
     * | 6      |   2  |`flags`      | 位标志 |
     * | 8      |   4  |`requestId`  | 请求/应答关联；单向消息填0 |
     * | 12     |   4  |`payloadLen` | body字节数 上限是64 MiB |
     */
    struct Header {
        // 版本号 校验
        std::uint8_t version = kVersion;
        // 消息类型
        MessageType type = MessageType::Ping;
        /**
         * 2字节 flag
         *   - 低0位表示More
         *   - 低1位表示Compressed
         *   - 低2位表示Error
         * 其他bit位保留位用0占位
         */
        std::uint16_t flags = 0;
        // 请求/应答的关联id 0表示单向消息
        std::uint32_t requestId = 0;
        // body有多少个字节
        std::uint32_t payloadLen = 0;
    };

    /**
     * 根据消息头编码成协议头
     * 协议帧=协议头16字节+变长body
     * @param header 消息头
     */
    std::array<std::uint8_t, kHeaderSize> encodeHeader(const Header& header);

    /**
     * @param data 把网络上传来的数据解码成header 解码后原始数据不破坏
     * @param size data多大
     */
    Header decodeHeader(const std::uint8_t* data, std::size_t size);

    /// @brief 协议头+消息体 拼成帧字节流
    /// @param header 协议头
    /// @param payload 消息体
    std::vector<std::uint8_t> encodeFrame(const Header& header, const std::vector<std::uint8_t>& payload);

    /// @brief 逻辑消息转换成TCP帧字节流
    /// @param type 消息类型
    /// @param requestId 请求/响应的关联 0-表示单向消息
    /// @param body 消息内容
    /// @param flags 放在协议头里面
    std::vector<std::uint8_t> makeFrame(MessageType type, std::uint32_t requestId,
                                        const std::vector<std::uint8_t>& body, std::uint16_t flags = 0);

    void appendVarint(std::vector<std::uint8_t>& out, std::uint64_t value);

    std::string_view messageTypeName(MessageType type);

    std::optional<MessageType> parseMessageType(std::string_view name);

    class FieldWriter {
    public:
        void putU64(std::uint16_t id, std::uint64_t value);

        void putString(std::uint16_t id, std::string_view value);

        void putBytes(std::uint16_t id, const std::vector<std::uint8_t>& value);

        const std::vector<std::uint8_t>& bytes() const noexcept {
            return buffer_;
        }

        std::vector<std::uint8_t> take() {
            return std::move(buffer_);
        }

    private:
        void putField(std::uint16_t id, WireType type, const std::uint8_t* data, std::size_t size);

        std::vector<std::uint8_t> buffer_;
    };

    class FieldReader {
    public:
        explicit FieldReader(const std::vector<std::uint8_t>& data);

        FieldReader(const std::uint8_t* data, std::size_t size);

        bool next();

        bool done() const noexcept {
            return offset_ >= size_;
        }

        std::uint16_t id() const noexcept {
            return id_;
        }

        WireType wireType() const noexcept {
            return wireType_;
        }

        std::uint64_t asU64() const;

        std::string asString() const;

        std::vector<std::uint8_t> asBytes() const;

    private:
        const std::uint8_t* data_ = nullptr;
        std::size_t size_ = 0;
        std::size_t offset_ = 0;
        std::uint16_t id_ = 0;
        WireType wireType_ = WireType::Varint;
        const std::uint8_t* value_ = nullptr;
        std::size_t valueSize_ = 0;
    };

    namespace hello {
        inline constexpr std::uint16_t kProtocolVersion = 1;
        inline constexpr std::uint16_t kWorkerId = 2;
        inline constexpr std::uint16_t kCapabilities = 3;
        inline constexpr std::uint16_t kPid = 4;
    }  // namespace hello

    namespace taskMsg {
        inline constexpr std::uint16_t kKind = 1;
        inline constexpr std::uint16_t kTaskId = 2;
        inline constexpr std::uint16_t kJob = 3;
        inline constexpr std::uint16_t kReducers = 4;
        inline constexpr std::uint16_t kMaps = 5;
        inline constexpr std::uint16_t kInput = 6;
        // 重复字段 每个是"mapTask,host,port" reduce去这些worker拉中间结果
        inline constexpr std::uint16_t kLocation = 7;
        // 重复字段 每个是"blockId,host,port" map任务去这些位置读输入block
        inline constexpr std::uint16_t kBlock = 8;
    }  // namespace taskMsg

    namespace data {
        inline constexpr std::uint16_t kOffset = 1;
        inline constexpr std::uint16_t kTotal = 2;
        inline constexpr std::uint16_t kPayload = 3;
    }  // namespace data

    namespace resultMsg {
        inline constexpr std::uint16_t kReduceTask = 1;
        inline constexpr std::uint16_t kOffset = 2;
        inline constexpr std::uint16_t kPayload = 3;
    }  // namespace resultMsg

    namespace done {
        inline constexpr std::uint16_t kKind = 1;
        inline constexpr std::uint16_t kTaskId = 2;
    }  // namespace done

    namespace fail {
        inline constexpr std::uint16_t kKind = 1;
        inline constexpr std::uint16_t kTaskId = 2;
        inline constexpr std::uint16_t kStatusCode = 3;
        inline constexpr std::uint16_t kReason = 4;
    }  // namespace fail

    namespace ping {
        inline constexpr std::uint16_t kNonce = 1;
    }  // namespace ping

    namespace stop {
        inline constexpr std::uint16_t kReason = 1;
    }  // namespace stop

    namespace submit {
        inline constexpr std::uint16_t kJob = 1;
        inline constexpr std::uint16_t kReducers = 2;
        inline constexpr std::uint16_t kOutput = 4;
        inline constexpr std::uint16_t kInput = 5;
        inline constexpr std::uint16_t kPluginHash = 6;
    }  // namespace submit

    namespace submitAck {
        inline constexpr std::uint16_t kStatusCode = 1;
        inline constexpr std::uint16_t kReason = 2;
        inline constexpr std::uint16_t kDataHost = 3;
        inline constexpr std::uint16_t kDataPort = 4;
    }  // namespace submitAck

    namespace submitResult {
        inline constexpr std::uint16_t kStatusCode = 1;
        inline constexpr std::uint16_t kOutput = 2;
        inline constexpr std::uint16_t kReason = 3;
    }  // namespace submitResult

    namespace pull {
        inline constexpr std::uint16_t kMapTask = 1;
        inline constexpr std::uint16_t kPartition = 2;
    }  // namespace pull

    namespace dataAddress {
        inline constexpr std::uint16_t kPort = 1;
        inline constexpr std::uint16_t kBlockPort = 2;
    }  // namespace dataAddress

    namespace plugin {
        inline constexpr std::uint16_t kHash = 1;
        inline constexpr std::uint16_t kJob = 2;
        inline constexpr std::uint16_t kOffset = 3;
        inline constexpr std::uint16_t kPayload = 4;
    }  // namespace plugin

    namespace pluginAck {
        inline constexpr std::uint16_t kHash = 1;
        inline constexpr std::uint16_t kOk = 2;
        inline constexpr std::uint16_t kReason = 3;
    }  // namespace pluginAck

    namespace pullInput {
        inline constexpr std::uint16_t kTaskId = 1;
    }  // namespace pullInput

    namespace pullPlugin {
        inline constexpr std::uint16_t kHash = 1;
    }  // namespace pullPlugin

    namespace masterData {
        inline constexpr std::uint16_t kPort = 1;
    }  // namespace masterData

    namespace needPlugin {
        inline constexpr std::uint16_t kHash = 1;
        inline constexpr std::uint16_t kJob = 2;
    }  // namespace needPlugin

    namespace progress {
        inline constexpr std::uint16_t kKind = 1;
        inline constexpr std::uint16_t kTaskId = 2;
        inline constexpr std::uint16_t kFraction = 3;
    }  // namespace progress

    namespace cancel {
        inline constexpr std::uint16_t kKind = 1;
        inline constexpr std::uint16_t kTaskId = 2;
    }  // namespace cancel

    namespace blockManifest {
        inline constexpr std::uint16_t kBlock = 1;
    }  // namespace blockManifest

    namespace blockPlacement {
        inline constexpr std::uint16_t kStatusCode = 1;
        inline constexpr std::uint16_t kReason = 2;
        inline constexpr std::uint16_t kReplica = 3;
    }  // namespace blockPlacement

    namespace uploadDone {
        inline constexpr std::uint16_t kJob = 1;
    }  // namespace uploadDone

    namespace replicateBlock {
        inline constexpr std::uint16_t kBlockId = 1;
        inline constexpr std::uint16_t kSourceHost = 2;
        inline constexpr std::uint16_t kSourcePort = 3;
    }  // namespace replicateBlock

    namespace blockReport {
        inline constexpr std::uint16_t kBlock = 1;
    }  // namespace blockReport

    namespace deleteBlock {
        inline constexpr std::uint16_t kBlockId = 1;
    }  // namespace deleteBlock

    // 内容哈希(FNV-1a 64) 十六进制 用作插件的缓存key
    std::string contentHash(const std::vector<std::uint8_t>& data);
}  // namespace xmr::protocol
