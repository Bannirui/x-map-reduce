#include "dfs/dfs_messages.h"

#include <xnet/buffer.h>

#include <algorithm>
#include <cstring>

namespace {

    /// @brief 小端序写成大端序
    /// @param out 大端序结果缓冲区
    /// @param value 16位小端序
    void writeBE16(std::uint8_t* out, std::uint16_t value) {
        out[0] = static_cast<std::uint8_t>(value >> 8);
        out[1] = static_cast<std::uint8_t>(value);
    }

    /// @brief 小端序写成大端序
    /// @param out 大端序结果缓冲区
    /// @param value 32位小端序
    void writeBE32(std::uint8_t* out, std::uint32_t value) {
        out[0] = static_cast<std::uint8_t>(value >> 24);
        out[1] = static_cast<std::uint8_t>(value >> 16);
        out[2] = static_cast<std::uint8_t>(value >> 8);
        out[3] = static_cast<std::uint8_t>(value);
    }

    /// @brief 16位整数 网络数据读到本机
    /// @param data 网络上是大端序
    /// @return 本机是小端序
    std::uint16_t readBE16(const std::uint8_t* data) {
        return static_cast<std::uint16_t>((static_cast<std::uint16_t>(data[0]) << 8) | data[1]);
    }

    /// @brief 32位整数 网络数据读到本机
    /// @param data 网络上是大端序
    /// @return 本机是小端序
    std::uint32_t readBE32(const std::uint8_t* data) {
        return (static_cast<std::uint32_t>(data[0]) << 24) | (static_cast<std::uint32_t>(data[1]) << 16) |
               (static_cast<std::uint32_t>(data[2]) << 8) | static_cast<std::uint32_t>(data[3]);
    }

    /// @brief 64位整数 网络数据读到本机
    /// @param data 网络上是大端序
    /// @return 本机是小端序
    std::uint64_t readBE64(const std::uint8_t* data) {
        std::uint64_t value = 0;
        for (int i = 0; i < 8; ++i) {
            value = (value << 8) | data[i];
        }
        return value;
    }

    /// @brief 16位 小端数据转到大端
    /// @param out 大端结果
    /// @param value 小端数据
    void appendBE16(std::vector<std::uint8_t>& out, std::uint16_t value) {
        out.push_back(static_cast<std::uint8_t>(value >> 8));
        out.push_back(static_cast<std::uint8_t>(value));
    }

    /// @brief 32位 小端数据转到大端
    /// @param out 大端结果
    /// @param value 小端数据
    void appendBE32(std::vector<std::uint8_t>& out, std::uint32_t value) {
        out.push_back(static_cast<std::uint8_t>(value >> 24));
        out.push_back(static_cast<std::uint8_t>(value >> 16));
        out.push_back(static_cast<std::uint8_t>(value >> 8));
        out.push_back(static_cast<std::uint8_t>(value));
    }

    /// @brief 64位 小端数据转到大端
    /// @param out 大端结果
    /// @param value 小端数据
    void appendVarint(std::vector<std::uint8_t>& out, std::uint64_t value) {
        while (value >= 0x80u) {
            out.push_back(static_cast<std::uint8_t>(value) | 0x80u);
            value >>= 7;
        }
        out.push_back(static_cast<std::uint8_t>(value));
    }

    std::uint64_t readVarint(const std::uint8_t* data, std::size_t size, std::size_t& consumed) {
        std::uint64_t result = 0;
        unsigned shift = 0;
        std::size_t index = 0;
        while (index < size) {
            const std::uint8_t byte = data[index++];
            if (index == 10 && byte > 1) {
                throw DfsProtocolError("varint overflow");
            }
            result |= static_cast<std::uint64_t>(byte & 0x7fu) << shift;
            if ((byte & 0x80u) == 0) {
                consumed = index;
                return result;
            }
            shift += 7;
            if (index >= 10) {
                throw DfsProtocolError("varint overflow");
            }
        }
        throw DfsProtocolError("truncated varint");
    }

    static constexpr std::uint16_t kBlockIdField = 1;
    static constexpr std::uint16_t kOffsetField = 2;
    static constexpr std::uint16_t kPayloadField = 3;
    static constexpr std::uint16_t kOkField = 2;
    static constexpr std::uint16_t kReasonField = 3;
    static constexpr std::uint16_t kFoundField = 3;
    static constexpr std::uint16_t kDataPayloadField = 4;
}  // namespace

std::array<std::uint8_t, kDfsHeaderSize> encodeDfsHeader(const DfsHeader& header) {
    if (header.payloadLen > kDfsMaxPayloadBytes) {
        throw DfsProtocolError("payload exceeds kDfsMaxPayloadBytes");
    }
    std::array<std::uint8_t, kDfsHeaderSize> out{};
    std::copy(kDfsMagic.begin(), kDfsMagic.end(), out.begin());
    out[4] = header.version;
    out[5] = static_cast<std::uint8_t>(header.type);
    writeBE16(out.data() + 6, header.flags);
    writeBE32(out.data() + 8, header.requestId);
    writeBE32(out.data() + 12, header.payloadLen);
    return out;
}

DfsHeader decodeDfsHeader(const std::uint8_t* data, std::size_t size) {
    if (data == nullptr || size < kDfsHeaderSize) {
        throw DfsProtocolError("short frame header");
    }
    if (!std::equal(kDfsMagic.begin(), kDfsMagic.end(), data)) {
        throw DfsProtocolError("bad dfs magic");
    }
    DfsHeader header;
    header.version = data[4];
    header.type = static_cast<DfsMessageType>(data[5]);
    header.flags = readBE16(data + 6);
    header.requestId = readBE32(data + 8);
    header.payloadLen = readBE32(data + 12);
    if (header.payloadLen > kDfsMaxPayloadBytes) {
        throw DfsProtocolError("payload exceeds kDfsMaxPayloadBytes");
    }
    return header;
}

std::vector<std::uint8_t> encodeDfsFrame(const DfsHeader& header, const std::vector<std::uint8_t>& payload) {
    if (payload.size() > kDfsMaxPayloadBytes) {
        throw DfsProtocolError("payload exceeds kDfsMaxPayloadBytes");
    }
    DfsHeader framed = header;
    framed.payloadLen = static_cast<std::uint32_t>(payload.size());
    const auto encoded = encodeDfsHeader(framed);
    std::vector<std::uint8_t> frame(encoded.begin(), encoded.end());
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

std::vector<std::uint8_t> makeDfsFrame(DfsMessageType type, std::uint32_t requestId,
                                       const std::vector<std::uint8_t>& body, std::uint16_t flags) {
    DfsHeader header;
    header.type = type;
    header.flags = flags;
    header.requestId = requestId;
    return encodeDfsFrame(header, body);
}

void DfsFieldWriter::putField(std::uint16_t id, DfsWireType type, const std::uint8_t* data, std::size_t size) {
    if (size > kDfsMaxPayloadBytes) {
        throw DfsProtocolError("field exceeds kDfsMaxPayloadBytes");
    }
    appendBE16(buffer_, id);
    buffer_.push_back(static_cast<std::uint8_t>(type));
    buffer_.push_back(0);
    appendBE32(buffer_, static_cast<std::uint32_t>(size));
    if (size > 0) {
        buffer_.insert(buffer_.end(), data, data + size);
    }
}

void DfsFieldWriter::putU64(std::uint16_t id, std::uint64_t value) {
    std::vector<std::uint8_t> encoded;
    appendVarint(encoded, value);
    putField(id, DfsWireType::Varint, encoded.data(), encoded.size());
}

void DfsFieldWriter::putString(std::uint16_t id, std::string_view value) {
    putField(id, DfsWireType::Bytes, reinterpret_cast<const std::uint8_t*>(value.data()), value.size());
}

void DfsFieldWriter::putBytes(std::uint16_t id, const std::vector<std::uint8_t>& value) {
    putField(id, DfsWireType::Bytes, value.data(), value.size());
}

DfsFieldReader::DfsFieldReader(const std::vector<std::uint8_t>& data) : data_(data.data()), size_(data.size()) {}

DfsFieldReader::DfsFieldReader(const std::uint8_t* data, std::size_t size) : data_(data), size_(size) {}

bool DfsFieldReader::next() {
    if (offset_ >= size_) {
        return false;
    }
    const std::size_t remaining = size_ - offset_;
    if (remaining < 8) {
        throw DfsProtocolError("truncated field header");
    }
    const std::uint8_t* field = data_ + offset_;
    id_ = readBE16(field);
    wireType_ = static_cast<DfsWireType>(field[2]);
    const std::uint32_t length = readBE32(field + 4);
    if (length > remaining - 8) {
        throw DfsProtocolError("truncated field value");
    }
    value_ = field + 8;
    valueSize_ = length;
    offset_ += 8 + length;
    return true;
}

std::uint64_t DfsFieldReader::asU64() const {
    if (wireType_ == DfsWireType::Varint) {
        std::size_t consumed = 0;
        const std::uint64_t value = readVarint(value_, valueSize_, consumed);
        if (consumed != valueSize_) {
            throw DfsProtocolError("varint field has trailing bytes");
        }
        return value;
    }
    if (wireType_ == DfsWireType::Fixed64) {
        if (valueSize_ != 8) {
            throw DfsProtocolError("fixed64 field must be 8 bytes");
        }
        return readBE64(value_);
    }
    throw DfsProtocolError("field is not an integer");
}

std::string DfsFieldReader::asString() const {
    if (wireType_ != DfsWireType::Bytes) {
        throw DfsProtocolError("field is not bytes");
    }
    return std::string(reinterpret_cast<const char*>(value_), valueSize_);
}

std::vector<std::uint8_t> DfsFieldReader::asBytes() const {
    if (wireType_ != DfsWireType::Bytes) {
        throw DfsProtocolError("field is not bytes");
    }
    return std::vector<std::uint8_t>(value_, value_ + valueSize_);
}

std::optional<DfsFrame> DfsFrameParser::next() {
    if (buffer_.size() < kDfsHeaderSize) {
        return std::nullopt;
    }
    const DfsHeader header = decodeDfsHeader(buffer_.data(), buffer_.size());
    const std::size_t total = kDfsHeaderSize + static_cast<std::size_t>(header.payloadLen);
    if (buffer_.size() < total) {
        return std::nullopt;
    }
    buffer_.consume(kDfsHeaderSize);
    DfsFrame frame;
    frame.header = header;
    frame.body = buffer_.take(header.payloadLen);
    return frame;
}

std::size_t DfsFrameParser::buffered() const noexcept {
    return buffer_.size();
}

std::vector<std::uint8_t> WriteBlock::encode() const {
    DfsFieldWriter writer;
    writer.putString(kBlockIdField, blockId);
    writer.putU64(kOffsetField, offset);
    writer.putBytes(kPayloadField, payload);
    return writer.take();
}

WriteBlock WriteBlock::decode(const std::vector<std::uint8_t>& body) {
    WriteBlock message;
    DfsFieldReader reader(body);
    while (reader.next()) {
        switch (reader.id()) {
            case kBlockIdField:
                message.blockId = reader.asString();
                break;
            case kOffsetField:
                message.offset = reader.asU64();
                break;
            case kPayloadField:
                message.payload = reader.asBytes();
                break;
            default:
                break;
        }
    }
    return message;
}

std::vector<std::uint8_t> WriteAck::encode() const {
    DfsFieldWriter writer;
    writer.putString(kBlockIdField, blockId);
    writer.putU64(kOkField, ok ? 1 : 0);
    writer.putString(kReasonField, reason);
    return writer.take();
}

WriteAck WriteAck::decode(const std::vector<std::uint8_t>& body) {
    WriteAck message;
    DfsFieldReader reader(body);
    while (reader.next()) {
        switch (reader.id()) {
            case kBlockIdField:
                message.blockId = reader.asString();
                break;
            case kOkField:
                message.ok = reader.asU64() != 0;
                break;
            case kReasonField:
                message.reason = reader.asString();
                break;
            default:
                break;
        }
    }
    return message;
}

std::vector<std::uint8_t> ReadBlock::encode() const {
    DfsFieldWriter writer;
    writer.putString(kBlockIdField, blockId);
    writer.putU64(kOffsetField, offset);
    return writer.take();
}

ReadBlock ReadBlock::decode(const std::vector<std::uint8_t>& body) {
    ReadBlock message;
    DfsFieldReader reader(body);
    while (reader.next()) {
        switch (reader.id()) {
            case kBlockIdField:
                message.blockId = reader.asString();
                break;
            case kOffsetField:
                message.offset = reader.asU64();
                break;
            default:
                break;
        }
    }
    return message;
}

std::vector<std::uint8_t> BlockData::encode() const {
    DfsFieldWriter writer;
    writer.putString(kBlockIdField, blockId);
    writer.putU64(kOffsetField, offset);
    writer.putU64(kFoundField, found ? 1 : 0);
    writer.putBytes(kDataPayloadField, payload);
    return writer.take();
}

BlockData BlockData::decode(const std::vector<std::uint8_t>& body) {
    BlockData message;
    DfsFieldReader reader(body);
    while (reader.next()) {
        switch (reader.id()) {
            case kBlockIdField:
                message.blockId = reader.asString();
                break;
            case kOffsetField:
                message.offset = reader.asU64();
                break;
            case kFoundField:
                message.found = reader.asU64() != 0;
                break;
            case kDataPayloadField:
                message.payload = reader.asBytes();
                break;
            default:
                break;
        }
    }
    return message;
}
