#include "protocol/protocol.h"

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

namespace {

int failures = 0;

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

template <typename Fn>
bool throwsProtocol(Fn&& fn) {
    try {
        fn();
    } catch (const xmr::protocol::ProtocolError&) {
        return true;
    } catch (...) {
        return false;
    }
    return false;
}

void writeBE32(std::uint8_t* out, std::uint32_t value) {
    out[0] = static_cast<std::uint8_t>(value >> 24);
    out[1] = static_cast<std::uint8_t>(value >> 16);
    out[2] = static_cast<std::uint8_t>(value >> 8);
    out[3] = static_cast<std::uint8_t>(value);
}

std::vector<std::uint8_t> rawHeader(std::uint32_t payloadLen) {
    std::vector<std::uint8_t> header(xmr::protocol::kHeaderSize, 0);
    const std::uint8_t magic[4] = {'X', 'M', 'R', 'P'};
    for (int i = 0; i < 4; ++i) {
        header[i] = magic[i];
    }
    header[4] = xmr::protocol::kVersion;
    header[5] = static_cast<std::uint8_t>(xmr::protocol::MessageType::Task);
    writeBE32(header.data() + 12, payloadLen);
    return header;
}

}  // namespace

int main() {
    using namespace xmr::protocol;

    try {
        {
            Header header;
            header.version = kVersion;
            header.type = MessageType::Task;
            header.flags = static_cast<std::uint16_t>(Flag::More);
            header.requestId = 0x01020304u;
            header.payloadLen = 5;
            const auto bytes = encodeHeader(header);

            check(bytes[0] == 'X' && bytes[1] == 'M' && bytes[2] == 'R' && bytes[3] == 'P',
                  "magic should be XMRP");
            check(bytes[4] == kVersion, "version byte");
            check(bytes[5] == static_cast<std::uint8_t>(MessageType::Task), "type byte");
            check(bytes[6] == 0 && bytes[7] == 1, "flags should be big-endian");
            check(bytes[8] == 0x01 && bytes[9] == 0x02 && bytes[10] == 0x03 && bytes[11] == 0x04,
                  "requestId should be big-endian");
            check(bytes[12] == 0 && bytes[13] == 0 && bytes[14] == 0 && bytes[15] == 5,
                  "payloadLen should be big-endian");

            const Header decoded = decodeHeader(bytes.data(), bytes.size());
            check(decoded.version == kVersion, "decoded version");
            check(decoded.type == MessageType::Task, "decoded type");
            check(decoded.flags == static_cast<std::uint16_t>(Flag::More), "decoded flags");
            check(decoded.requestId == 0x01020304u, "decoded requestId");
            check(decoded.payloadLen == 5, "decoded payloadLen");
        }

        {
            auto bytes = rawHeader(3);
            bytes[0] = 'B';
            check(throwsProtocol([&] { decodeHeader(bytes.data(), bytes.size()); }), "bad magic should throw");
        }

        {
            auto bytes = rawHeader(0);
            check(throwsProtocol([&] { decodeHeader(bytes.data(), kHeaderSize - 1); }), "short header should throw");
            check(throwsProtocol([&] { decodeHeader(nullptr, 0); }), "null header should throw");
        }

        {
            auto bytes = rawHeader(kMaxPayloadBytes + 1);
            check(throwsProtocol([&] { decodeHeader(bytes.data(), bytes.size()); }),
                  "payload over limit should throw");
        }

        {
            Header header;
            header.type = MessageType::Data;
            header.requestId = 7;
            const std::vector<std::uint8_t> payload{1, 2, 3, 4};
            const auto frame = encodeFrame(header, payload);
            check(frame.size() == kHeaderSize + payload.size(), "frame should be header plus payload");

            const Header decoded = decodeHeader(frame.data(), frame.size());
            check(decoded.payloadLen == payload.size(), "encodeFrame should set payloadLen");
            check(decoded.requestId == 7, "encodeFrame should preserve requestId");
            check(std::vector<std::uint8_t>(frame.begin() + kHeaderSize, frame.end()) == payload,
                  "frame payload should round-trip");
        }

        {
            const std::vector<std::uint64_t> values{0, 1, 127, 128, 300, 16384, 0xFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull};
            FieldWriter writer;
            for (std::size_t i = 0; i < values.size(); ++i) {
                writer.putU64(static_cast<std::uint16_t>(i + 1), values[i]);
            }
            FieldReader reader(writer.bytes());
            for (std::size_t i = 0; i < values.size(); ++i) {
                check(reader.next(), "varint field should exist");
                check(reader.id() == i + 1, "varint field id");
                check(reader.wireType() == WireType::Varint, "varint wire type");
                check(reader.asU64() == values[i], "varint value round-trip");
            }
            check(!reader.next(), "no more varint fields");
            check(reader.done(), "reader should be done");
        }

        {
            const std::string text = std::string("a\tb\nc\0d", 7);
            FieldWriter writer;
            writer.putString(1, text);
            FieldReader reader(writer.bytes());
            check(reader.next(), "string field should exist");
            check(reader.asString() == text, "binary-safe string should round-trip");

            const std::vector<std::uint8_t> blob{0, 255, 10, 13};
            FieldWriter writer2;
            writer2.putBytes(1, blob);
            FieldReader reader2(writer2.bytes());
            check(reader2.next(), "blob field should exist");
            check(reader2.asBytes() == blob, "blob should round-trip");
        }

        {
            FieldWriter writer;
            writer.putU64(1, 42);
            writer.putString(99, "field from the future");
            writer.putString(2, "known");
            FieldReader reader(writer.bytes());
            std::uint64_t knownId1 = 0;
            std::string knownId2;
            while (reader.next()) {
                if (reader.id() == 1) {
                    knownId1 = reader.asU64();
                } else if (reader.id() == 2) {
                    knownId2 = reader.asString();
                }
            }
            check(knownId1 == 42, "known field 1 survives an unknown field");
            check(knownId2 == "known", "known field 2 after an unknown field");
        }

        {
            FieldWriter writer;
            writer.putU64(1, 5);
            FieldReader reader(writer.bytes());
            check(reader.next(), "field should exist");
            check(throwsProtocol([&] { reader.asString(); }), "reading varint as string should throw");
        }

        {
            FieldWriter writer;
            writer.putString(1, "abcdef");
            auto bytes = writer.bytes();
            bytes.pop_back();
            FieldReader reader(bytes);
            check(throwsProtocol([&] { reader.next(); }), "truncated field value should throw");
        }

        {
            FieldWriter writer;
            writer.putU64(5, 1);
            writer.putU64(5, 9);
            FieldReader reader(writer.bytes());
            check(reader.next() && reader.asU64() == 1, "first duplicate field");
            check(reader.next() && reader.asU64() == 9, "second duplicate field");
        }

        {
            for (std::uint8_t raw = 1; raw <= static_cast<std::uint8_t>(MessageType::Stop); ++raw) {
                const auto type = static_cast<MessageType>(raw);
                const auto parsed = parseMessageType(messageTypeName(type));
                check(parsed.has_value() && *parsed == type, "message type name should round-trip");
            }
            check(!parseMessageType("NOPE").has_value(), "unknown message type should not parse");
        }

        {
            const std::vector<std::uint8_t> empty;
            Header header;
            header.type = MessageType::Stop;
            const auto frame = encodeFrame(header, empty);
            check(frame.size() == kHeaderSize, "empty payload frame should be just the header");
            check(decodeHeader(frame.data(), frame.size()).payloadLen == 0, "empty payload length");
        }
    } catch (const std::exception& error) {
        std::cerr << "unexpected exception: " << error.what() << '\n';
        ++failures;
    }

    if (failures != 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
