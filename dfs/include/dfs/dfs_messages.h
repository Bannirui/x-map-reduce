#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

class ByteBuffer;

inline constexpr std::array<std::uint8_t, 4> kDfsMagic{'X', 'D', 'F', 'S'};
inline constexpr std::uint8_t kDfsVersion = 1;
inline constexpr std::size_t kDfsHeaderSize = 16;
// block文件内容的硬限制
inline constexpr std::uint32_t kDfsMaxPayloadBytes = 64u * 1024u * 1024u;
// 大文件分块发送
inline constexpr std::uint32_t kDfsChunkBytes = 1u << 20;

// dfs block存储层帧类型
enum class DfsMessageType : std::uint8_t {
    // client->block server 写一个block分片
    WriteBlock = 1,
    // block server->client 写结果回执
    WriteAck = 2,
    // client->block server 读一个block分片
    ReadBlock = 3,
    // block server->client 返回数据
    BlockData = 4,
};

enum class DfsFlag : std::uint16_t {
    More = 1u << 0,
    Error = 1u << 2,
};

enum class DfsWireType : std::uint8_t {
    Varint = 0,
    Bytes = 1,
    Fixed64 = 2,
};

/**
 * dfs协议的16字节定长帧头
 *
 * offset 字节  字段          值
 * 0...3   4   magic        XDFS
 * 4       1   version
 * 5       1   type
 * 6...7   2   flags
 * 8...11  4   reqId
 * 12...15 4   body len
 */
struct DfsHeader {
    // 协议版本
    std::uint8_t version = kDfsVersion;
    DfsMessageType type = DfsMessageType::WriteAck;
    std::uint16_t flags = 0;
    // 关联req/resp
    std::uint32_t requestId = 0;
    // body长度
    std::uint32_t payloadLen = 0;
};

// dfs消息=帧头+载荷
struct DfsFrame {
    // 16字节定长帧头
    DfsHeader header;
    // TVL编码
    std::vector<std::uint8_t> body;
};

class DfsProtocolError : public std::runtime_error {
public:
    explicit DfsProtocolError(const std::string& what) : std::runtime_error(what) {}
};

std::array<std::uint8_t, kDfsHeaderSize> encodeDfsHeader(const DfsHeader& header);

DfsHeader decodeDfsHeader(const std::uint8_t* data, std::size_t size);

std::vector<std::uint8_t> encodeDfsFrame(const DfsHeader& header, const std::vector<std::uint8_t>& payload);

std::vector<std::uint8_t> makeDfsFrame(DfsMessageType type, std::uint32_t requestId,
                                       const std::vector<std::uint8_t>& body, std::uint16_t flags = 0);

class DfsFieldWriter {
public:
    void putU64(std::uint16_t id, std::uint64_t value);

    void putString(std::uint16_t id, std::string_view value);

    void putBytes(std::uint16_t id, const std::vector<std::uint8_t>& value);

    std::vector<std::uint8_t> take() {
        return std::move(buffer_);
    }

private:
    void putField(std::uint16_t id, DfsWireType type, const std::uint8_t* data, std::size_t size);

    std::vector<std::uint8_t> buffer_;
};

class DfsFieldReader {
public:
    explicit DfsFieldReader(const std::vector<std::uint8_t>& data);

    DfsFieldReader(const std::uint8_t* data, std::size_t size);

    bool next();

    bool done() const noexcept {
        return offset_ >= size_;
    }

    std::uint16_t id() const noexcept {
        return id_;
    }

    DfsWireType wireType() const noexcept {
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
    DfsWireType wireType_ = DfsWireType::Varint;
    const std::uint8_t* value_ = nullptr;
    std::size_t valueSize_ = 0;
};

class DfsFrameParser {
public:
    explicit DfsFrameParser(ByteBuffer& buffer) : buffer_(buffer) {}

    std::optional<DfsFrame> next();

    std::size_t buffered() const noexcept;

private:
    ByteBuffer& buffer_;
};

struct WriteBlock {
    static constexpr DfsMessageType kType = DfsMessageType::WriteBlock;
    std::string blockId;
    std::uint64_t offset = 0;
    std::vector<std::uint8_t> payload;

    std::vector<std::uint8_t> encode() const;

    static WriteBlock decode(const std::vector<std::uint8_t>& body);
};

struct WriteAck {
    static constexpr DfsMessageType kType = DfsMessageType::WriteAck;
    std::string blockId;
    bool ok = false;
    std::string reason;

    std::vector<std::uint8_t> encode() const;

    static WriteAck decode(const std::vector<std::uint8_t>& body);
};

struct ReadBlock {
    static constexpr DfsMessageType kType = DfsMessageType::ReadBlock;
    std::string blockId;
    std::uint64_t offset = 0;

    std::vector<std::uint8_t> encode() const;

    static ReadBlock decode(const std::vector<std::uint8_t>& body);
};

struct BlockData {
    static constexpr DfsMessageType kType = DfsMessageType::BlockData;
    // block文件名
    std::string blockId;
    // 这一帧内容在整个block的位置 block内容太大的时候会分多个块发送出去
    std::uint64_t offset = 0;
    // 标识在节点上有没有bloc数据 比如客户端想要某个block文件名的内容 但是在block节点上可能没有对应的内容
    // false表示没有数据
    bool found = false;
    std::vector<std::uint8_t> payload;

    std::vector<std::uint8_t> encode() const;

    static BlockData decode(const std::vector<std::uint8_t>& body);
};
