#pragma once

#include"protocol/protocol.h"

#include<cstdint>
#include<optional>
#include<string>
#include<vector>

namespace xmr::protocol {
    enum class WorkKind : std::uint8_t {
        Map = 0,
        Reduce = 1,
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
        WorkKind kind = WorkKind::Map;
        std::uint64_t taskId = 0;
        std::string job;
        std::uint64_t reducers = 1;
        std::uint64_t maps = 0;
        std::optional<std::string> input;

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
        WorkKind kind = WorkKind::Map;
        std::uint64_t taskId = 0;

        std::vector<std::uint8_t> encode() const;

        static Done decode(const std::vector<std::uint8_t>& body);
    };

    struct Fail {
        WorkKind kind = WorkKind::Map;
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

    std::vector<std::uint8_t> makeFrame(MessageType type, std::uint32_t requestId,
                                        const std::vector<std::uint8_t>& body, std::uint16_t flags = 0);
} // namespace xmr::protocol
