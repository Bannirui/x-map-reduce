#include"protocol/protocol.h"

#include<algorithm>

namespace xmr::protocol {
    namespace {
        /**
         * 主机上16位小端整数写成网络大端序
         * @param value 大端数据写到哪儿
         */
        void writeBE16(std::uint8_t* out, std::uint16_t value) {
            out[0] = static_cast<std::uint8_t>(value >> 8);
            out[1] = static_cast<std::uint8_t>(value);
        }

        /**
         * 主机上32位小端整数写成网络大端序
         * @param value 大端数据写到哪儿
         */
        void writeBE32(std::uint8_t* out, std::uint32_t value) {
            out[0] = static_cast<std::uint8_t>(value >> 24);
            out[1] = static_cast<std::uint8_t>(value >> 16);
            out[2] = static_cast<std::uint8_t>(value >> 8);
            out[3] = static_cast<std::uint8_t>(value);
        }

        /**
         * 从大端序数据里面读出来16位整数
         * @param 网络上传过来的数据 大端序
         */
        std::uint16_t readBE16(const std::uint8_t* data) {
            return static_cast<std::uint16_t>((static_cast<std::uint16_t>(data[0]) << 8) | data[1]);
        }

        /**
         * 从大端序数据里面读出来32位整数
         * @param 网络上传过来的数据 大端序
         */
        std::uint32_t readBE32(const std::uint8_t* data) {
            return (static_cast<std::uint32_t>(data[0]) << 24)
                   | (static_cast<std::uint32_t>(data[1]) << 16)
                   | (static_cast<std::uint32_t>(data[2]) << 8)
                   | static_cast<std::uint32_t>(data[3]);
        }

        /**
         * 从大端序数据里面读出来64位整数
         * @param 网络上传过来的数据 大端序
         */
        std::uint64_t readBE64(const std::uint8_t* data) {
            std::uint64_t value = 0;
            for (int i = 0; i < 8; ++i) {
                value = (value << 8) | data[i];
            }
            return value;
        }

        void appendBE16(std::vector<std::uint8_t>& out, std::uint16_t value) {
            out.push_back(static_cast<std::uint8_t>(value >> 8));
            out.push_back(static_cast<std::uint8_t>(value));
        }

        void appendBE32(std::vector<std::uint8_t>& out, std::uint32_t value) {
            out.push_back(static_cast<std::uint8_t>(value >> 24));
            out.push_back(static_cast<std::uint8_t>(value >> 16));
            out.push_back(static_cast<std::uint8_t>(value >> 8));
            out.push_back(static_cast<std::uint8_t>(value));
        }

        std::uint64_t readVarint(const std::uint8_t* data, std::size_t size, std::size_t& consumed) {
            std::uint64_t result = 0;
            unsigned shift = 0;
            std::size_t index = 0;
            while (index < size) {
                const std::uint8_t byte = data[index++];
                if (index == 10 && byte > 1) {
                    throw ProtocolError("varint overflow");
                }
                result |= static_cast<std::uint64_t>(byte & 0x7fu) << shift;
                if ((byte & 0x80u) == 0) {
                    consumed = index;
                    return result;
                }
                shift += 7;
                if (index >= 10) {
                    throw ProtocolError("varint overflow");
                }
            }
            throw ProtocolError("truncated varint");
        }
    } // namespace

    std::array<std::uint8_t, kHeaderSize> encodeHeader(const Header& header) {
        // 消息大小别超
        if (header.payloadLen > kMaxPayloadBytes) {
            throw ProtocolError("payload exceeds kMaxPayloadBytes");
        }
        // 协议头
        std::array<std::uint8_t, kHeaderSize> out{};
        // 4字节 魔数
        std::copy(kMagic.begin(), kMagic.end(), out.begin());
        // 1字节 版本号
        out[4] = header.version;
        // 1字节 类型
        out[5] = static_cast<std::uint8_t>(header.type);
        // 2字节 flag
        writeBE16(out.data() + 6, header.flags);
        // 4字节 消息id
        writeBE32(out.data() + 8, header.requestId);
        // 4字节 body多少个字节
        writeBE32(out.data() + 12, header.payloadLen);
        return out;
    }

    Header decodeHeader(const std::uint8_t* data, std::size_t size) {
        if (data == nullptr || size < kHeaderSize) {
            throw ProtocolError("short frame header");
        }
        // 协议头 前4字节魔数校验
        if (!std::equal(kMagic.begin(), kMagic.end(), data)) {
            throw ProtocolError("bad magic");
        }
        // 解码协议头
        Header header;
        // 1字节 版本号
        header.version = data[4];
        // 1字节 消息类型
        header.type = static_cast<MessageType>(data[5]);
        // 从网络大端序转到主机小端序
        header.flags = readBE16(data + 6);
        header.requestId = readBE32(data + 8);
        header.payloadLen = readBE32(data + 12);
        // 消息大小校验
        if (header.payloadLen > kMaxPayloadBytes) {
            throw ProtocolError("payload exceeds kMaxPayloadBytes");
        }
        return header;
    }

    std::vector<std::uint8_t> encodeFrame(const Header& header, const std::vector<std::uint8_t>& payload) {
        if (payload.size() > kMaxPayloadBytes) {
            throw ProtocolError("payload exceeds kMaxPayloadBytes");
        }
        Header framed = header;
        framed.payloadLen = static_cast<std::uint32_t>(payload.size());
        // 编码协议头
        const auto encoded = encodeHeader(framed);
        // 协议帧 header
        std::vector<std::uint8_t> frame(encoded.begin(), encoded.end());
        // body
        frame.insert(frame.end(), payload.begin(), payload.end());
        return frame;
    }

    void appendVarint(std::vector<std::uint8_t>& out, std::uint64_t value) {
        while (value >= 0x80u) {
            out.push_back(static_cast<std::uint8_t>(value) | 0x80u);
            value >>= 7;
        }
        out.push_back(static_cast<std::uint8_t>(value));
    }

    std::string_view messageTypeName(MessageType type) {
        switch (type) {
            case MessageType::Hello: return "HELLO";
            case MessageType::HelloAck: return "HELLO_ACK";
            case MessageType::RequestTask: return "REQUEST_TASK";
            case MessageType::Task: return "TASK";
            case MessageType::InputRequest: return "INPUT_REQUEST";
            case MessageType::Data: return "DATA";
            case MessageType::MapOutput: return "MAP_OUTPUT";
            case MessageType::Fetch: return "FETCH";
            case MessageType::Result: return "RESULT";
            case MessageType::Done: return "DONE";
            case MessageType::Fail: return "FAIL";
            case MessageType::Ping: return "PING";
            case MessageType::Pong: return "PONG";
            case MessageType::Stop: return "STOP";
        }
        return "UNKNOWN";
    }

    std::optional<MessageType> parseMessageType(std::string_view name) {
        for (std::uint8_t raw = 1; raw <= static_cast<std::uint8_t>(MessageType::Stop); ++raw) {
            const auto type = static_cast<MessageType>(raw);
            if (messageTypeName(type) == name) {
                return type;
            }
        }
        return std::nullopt;
    }

    void FieldWriter::putField(std::uint16_t id, WireType type, const std::uint8_t* data, std::size_t size) {
        if (size > kMaxPayloadBytes) {
            throw ProtocolError("field exceeds kMaxPayloadBytes");
        }
        appendBE16(buffer_, id);
        buffer_.push_back(static_cast<std::uint8_t>(type));
        buffer_.push_back(0);
        appendBE32(buffer_, static_cast<std::uint32_t>(size));
        if (size > 0) {
            buffer_.insert(buffer_.end(), data, data + size);
        }
    }

    void FieldWriter::putU64(std::uint16_t id, std::uint64_t value) {
        std::vector<std::uint8_t> encoded;
        appendVarint(encoded, value);
        putField(id, WireType::Varint, encoded.data(), encoded.size());
    }

    void FieldWriter::putString(std::uint16_t id, std::string_view value) {
        putField(id, WireType::Bytes, reinterpret_cast<const std::uint8_t*>(value.data()), value.size());
    }

    void FieldWriter::putBytes(std::uint16_t id, const std::vector<std::uint8_t>& value) {
        putField(id, WireType::Bytes, value.data(), value.size());
    }

    FieldReader::FieldReader(const std::vector<std::uint8_t>& data)
        : data_(data.data()), size_(data.size()) {
    }

    FieldReader::FieldReader(const std::uint8_t* data, std::size_t size)
        : data_(data), size_(size) {
    }

    bool FieldReader::next() {
        if (offset_ >= size_) {
            return false;
        }
        const std::size_t remaining = size_ - offset_;
        if (remaining < 8) {
            throw ProtocolError("truncated field header");
        }
        const std::uint8_t* field = data_ + offset_;
        id_ = readBE16(field);
        wireType_ = static_cast<WireType>(field[2]);
        const std::uint32_t length = readBE32(field + 4);
        if (length > remaining - 8) {
            throw ProtocolError("truncated field value");
        }
        value_ = field + 8;
        valueSize_ = length;
        offset_ += 8 + length;
        return true;
    }

    std::uint64_t FieldReader::asU64() const {
        if (wireType_ == WireType::Varint) {
            std::size_t consumed = 0;
            const std::uint64_t value = readVarint(value_, valueSize_, consumed);
            if (consumed != valueSize_) {
                throw ProtocolError("varint field has trailing bytes");
            }
            return value;
        }
        if (wireType_ == WireType::Fixed64) {
            if (valueSize_ != 8) {
                throw ProtocolError("fixed64 field must be 8 bytes");
            }
            return readBE64(value_);
        }
        throw ProtocolError("field is not an integer");
    }

    std::string FieldReader::asString() const {
        if (wireType_ != WireType::Bytes) {
            throw ProtocolError("field is not bytes");
        }
        return std::string(reinterpret_cast<const char*>(value_), valueSize_);
    }

    std::vector<std::uint8_t> FieldReader::asBytes() const {
        if (wireType_ != WireType::Bytes) {
            throw ProtocolError("field is not bytes");
        }
        return std::vector<std::uint8_t>(value_, value_ + valueSize_);
    }
} // namespace xmr::protocol
