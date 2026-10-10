#include "net/buffer.h"
#include "net/net.h"
#include "protocol/framing.h"
#include "protocol/messages.h"

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

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

std::vector<std::uint8_t> toBytes(const std::string& text) {
    return std::vector<std::uint8_t>(text.begin(), text.end());
}

std::string toString(const std::vector<std::uint8_t>& bytes) {
    return std::string(bytes.begin(), bytes.end());
}

std::vector<std::uint8_t> rawHeader(const std::uint8_t magic[4], std::uint32_t payloadLen) {
    std::vector<std::uint8_t> header(xmr::protocol::kHeaderSize, 0);
    for (int i = 0; i < 4; ++i) {
        header[i] = magic[i];
    }
    header[4] = xmr::protocol::kVersion;
    header[5] = static_cast<std::uint8_t>(xmr::protocol::MessageType::Task);
    header[12] = static_cast<std::uint8_t>(payloadLen >> 24);
    header[13] = static_cast<std::uint8_t>(payloadLen >> 16);
    header[14] = static_cast<std::uint8_t>(payloadLen >> 8);
    header[15] = static_cast<std::uint8_t>(payloadLen);
    return header;
}

}  // namespace

int main() {
    using namespace xmr::protocol;

    try {
        {
            ByteBuffer buffer;
            check(buffer.empty() && buffer.size() == 0, "new buffer should be empty");

            const std::string text = "hello";
            buffer.append(text.data(), text.size());
            check(buffer.size() == 5, "append size");
            check(std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()) == text,
                  "append content");

            buffer.consume(2);
            check(buffer.size() == 3, "consume size");
            check(std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()) == "llo",
                  "consume advances read cursor");

            const auto tail = buffer.take(3);
            check(toString(tail) == "llo", "take content");
            check(buffer.empty(), "take empties buffer");

            buffer.append(toBytes("abcdefgh"));
            buffer.consume(4);
            buffer.append(toBytes("ij"));
            check(toString(buffer.take(buffer.size())) == "efghij", "append after consume compacts");

            check([] {
                ByteBuffer small;
                small.append(toBytes("x"));
                try {
                    small.consume(2);
                    return false;
                } catch (const std::out_of_range&) {
                    return true;
                }
            }(), "consume past end should throw");
        }

        {
            ByteBuffer buffer;
            FrameParser decoder(buffer);

            Hello hello;
            hello.workerId = "worker-1";
            const auto frame = makeFrame(MessageType::Hello, 5, hello.encode(),
                                         static_cast<std::uint16_t>(Flag::More));

            buffer.append(frame);
            auto decoded = decoder.next();
            check(decoded.has_value(), "full frame should decode");
            check(decoded->header.type == MessageType::Hello, "decoded frame type");
            check(decoded->header.requestId == 5, "decoded frame requestId");
            check(decoded->header.flags == static_cast<std::uint16_t>(Flag::More), "decoded frame flags");
            check(Hello::decode(decoded->body).workerId == "worker-1", "decoded frame body");
            check(!decoder.next().has_value(), "no second frame");
            check(buffer.empty(), "buffer drained");
        }

        {
            ByteBuffer buffer;
            FrameParser decoder(buffer);

            Hello hello;
            hello.workerId = "split";
            const auto frame = makeFrame(MessageType::Hello, 0, hello.encode());

            buffer.append(frame.data(), kHeaderSize - 1);
            check(!decoder.next().has_value(), "partial header should wait");
            check(decoder.buffered() == kHeaderSize - 1, "partial header stays buffered");

            buffer.append(frame.data() + kHeaderSize - 1, 1);
            check(!decoder.next().has_value(), "header only should wait for body");
            check(decoder.buffered() == kHeaderSize, "header stays buffered");

            buffer.append(frame.data() + kHeaderSize, frame.size() - kHeaderSize);
            auto decoded = decoder.next();
            check(decoded.has_value() && Hello::decode(decoded->body).workerId == "split",
                  "frame should decode once complete");
        }

        {
            ByteBuffer buffer;
            FrameParser decoder(buffer);

            Ping ping;
            ping.nonce = 1;
            Pong pong;
            pong.nonce = 2;
            const auto first = makeFrame(MessageType::Ping, 1, ping.encode());
            const auto second = makeFrame(MessageType::Pong, 2, pong.encode());
            buffer.append(first);
            buffer.append(second);

            auto a = decoder.next();
            auto b = decoder.next();
            check(a.has_value() && a->header.type == MessageType::Ping, "first of two frames");
            check(b.has_value() && b->header.type == MessageType::Pong, "second of two frames");
            check(Ping::decode(a->body).nonce == 1 && Pong::decode(b->body).nonce == 2, "two frame bodies");
            check(!decoder.next().has_value(), "no third frame");
        }

        {
            const std::uint8_t wrongMagic[4] = {'N', 'O', 'P', 'E'};
            ByteBuffer buffer;
            buffer.append(rawHeader(wrongMagic, 0));
            FrameParser decoder(buffer);
            check(throwsProtocol([&] { decoder.next(); }), "bad magic should throw");
        }

        {
            const std::uint8_t magic[4] = {'X', 'M', 'R', 'P'};
            ByteBuffer buffer;
            buffer.append(rawHeader(magic, kMaxPayloadBytes + 1));
            FrameParser decoder(buffer);
            check(throwsProtocol([&] { decoder.next(); }), "oversized frame should throw");
        }

        {
            int fds[2] = {-1, -1};
            check(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0, "socketpair should succeed");

            setNonBlocking(fds[0]);

            ByteBuffer buffer;
            check(recvInto(fds[0], buffer) == IoStatus::WouldBlock, "empty non-blocking recv should block");

            const std::string payload = "abc";
            sendAll(fds[1], payload.data(), payload.size());
            check(recvInto(fds[0], buffer) == IoStatus::Ok, "recv should read data");
            check(toString(buffer.take(buffer.size())) == payload, "recv content");
            check(recvInto(fds[0], buffer) == IoStatus::WouldBlock, "drained recv should block");

            ::close(fds[1]);
            check(recvInto(fds[0], buffer) == IoStatus::Closed, "peer close should be detected");
            ::close(fds[0]);
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
