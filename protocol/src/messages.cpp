#include"protocol/messages.h"

namespace xmr::protocol {
    namespace {
        void require(bool condition, const char* what) {
            if (!condition) {
                throw ProtocolError(what);
            }
        }

        WorkKind decodeKind(std::uint64_t raw) {
            require(raw == static_cast<std::uint64_t>(WorkKind::Map)
                    || raw == static_cast<std::uint64_t>(WorkKind::Reduce), "bad work kind");
            return static_cast<WorkKind>(raw);
        }

        StatusCode decodeStatus(std::uint64_t raw) {
            require(raw <= static_cast<std::uint64_t>(StatusCode::UnsupportedVersion), "bad status code");
            return static_cast<StatusCode>(raw);
        }
    } // namespace

    std::vector<std::uint8_t> Hello::encode() const {
        FieldWriter writer;
        writer.putU64(hello::kProtocolVersion, protocolVersion);
        writer.putString(hello::kWorkerId, workerId);
        writer.putU64(hello::kCapabilities, capabilities);
        if (pid) {
            writer.putU64(hello::kPid, *pid);
        }
        return writer.take();
    }

    Hello Hello::decode(const std::vector<std::uint8_t>& body) {
        Hello message;
        std::uint32_t seen = 0;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case hello::kProtocolVersion:
                    message.protocolVersion = reader.asU64();
                    seen |= 1u << 0;
                    break;
                case hello::kWorkerId:
                    message.workerId = reader.asString();
                    seen |= 1u << 1;
                    break;
                case hello::kCapabilities:
                    message.capabilities = reader.asU64();
                    seen |= 1u << 2;
                    break;
                case hello::kPid:
                    message.pid = reader.asU64();
                    break;
                default:
                    break;
            }
        }
        require((seen & 0b11u) == 0b11u, "hello missing required field");
        return message;
    }

    std::vector<std::uint8_t> HelloAck::encode() const {
        FieldWriter writer;
        writer.putU64(helloAck::kProtocolVersion, protocolVersion);
        writer.putString(helloAck::kSessionId, sessionId);
        writer.putU64(helloAck::kCapabilities, capabilities);
        writer.putU64(helloAck::kStatusCode, static_cast<std::uint64_t>(statusCode));
        writer.putString(helloAck::kReason, reason);
        return writer.take();
    }

    HelloAck HelloAck::decode(const std::vector<std::uint8_t>& body) {
        HelloAck message;
        std::uint32_t seen = 0;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case helloAck::kProtocolVersion:
                    message.protocolVersion = reader.asU64();
                    seen |= 1u << 0;
                    break;
                case helloAck::kSessionId:
                    message.sessionId = reader.asString();
                    break;
                case helloAck::kCapabilities:
                    message.capabilities = reader.asU64();
                    break;
                case helloAck::kStatusCode:
                    message.statusCode = decodeStatus(reader.asU64());
                    seen |= 1u << 1;
                    break;
                case helloAck::kReason:
                    message.reason = reader.asString();
                    break;
                default:
                    break;
            }
        }
        require((seen & 0b11u) == 0b11u, "hello_ack missing required field");
        return message;
    }

    std::vector<std::uint8_t> RequestTask::encode() const {
        return {};
    }

    RequestTask RequestTask::decode(const std::vector<std::uint8_t>& body) {
        FieldReader reader(body);
        while (reader.next()) {
        }
        return {};
    }

    std::vector<std::uint8_t> TaskMessage::encode() const {
        FieldWriter writer;
        writer.putU64(taskMsg::kKind, static_cast<std::uint64_t>(kind));
        writer.putU64(taskMsg::kTaskId, taskId);
        writer.putString(taskMsg::kJob, job);
        writer.putU64(taskMsg::kReducers, reducers);
        writer.putU64(taskMsg::kMaps, maps);
        if (input) {
            writer.putString(taskMsg::kInput, *input);
        }
        for (const auto& location : locations) {
            writer.putString(taskMsg::kLocation, location);
        }
        return writer.take();
    }

    TaskMessage TaskMessage::decode(const std::vector<std::uint8_t>& body) {
        TaskMessage message;
        std::uint32_t seen = 0;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case taskMsg::kKind:
                    message.kind = decodeKind(reader.asU64());
                    seen |= 1u << 0;
                    break;
                case taskMsg::kTaskId:
                    message.taskId = reader.asU64();
                    seen |= 1u << 1;
                    break;
                case taskMsg::kJob:
                    message.job = reader.asString();
                    seen |= 1u << 2;
                    break;
                case taskMsg::kReducers:
                    message.reducers = reader.asU64();
                    seen |= 1u << 3;
                    break;
                case taskMsg::kMaps:
                    message.maps = reader.asU64();
                    seen |= 1u << 4;
                    break;
                case taskMsg::kInput:
                    message.input = reader.asString();
                    break;
                case taskMsg::kLocation:
                    message.locations.push_back(reader.asString());
                    break;
                default:
                    break;
            }
        }
        require((seen & 0b11111u) == 0b11111u, "task missing required field");
        if (message.kind == WorkKind::Map) {
            require(message.input.has_value(), "map task missing input");
        }
        return message;
    }

    std::vector<std::uint8_t> InputRequest::encode() const {
        FieldWriter writer;
        writer.putU64(inputRequest::kTaskId, taskId);
        return writer.take();
    }

    InputRequest InputRequest::decode(const std::vector<std::uint8_t>& body) {
        InputRequest message;
        bool seen = false;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == inputRequest::kTaskId) {
                message.taskId = reader.asU64();
                seen = true;
            }
        }
        require(seen, "input_request missing taskId");
        return message;
    }

    std::vector<std::uint8_t> DataMessage::encode() const {
        FieldWriter writer;
        writer.putU64(data::kOffset, offset);
        if (total) {
            writer.putU64(data::kTotal, *total);
        }
        writer.putBytes(data::kPayload, payload);
        return writer.take();
    }

    DataMessage DataMessage::decode(const std::vector<std::uint8_t>& body) {
        DataMessage message;
        std::uint32_t seen = 0;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case data::kOffset:
                    message.offset = reader.asU64();
                    seen |= 1u << 0;
                    break;
                case data::kTotal:
                    message.total = reader.asU64();
                    break;
                case data::kPayload:
                    message.payload = reader.asBytes();
                    seen |= 1u << 1;
                    break;
                default:
                    break;
            }
        }
        require((seen & 0b11u) == 0b11u, "data missing required field");
        return message;
    }

    std::vector<std::uint8_t> MapOutput::encode() const {
        FieldWriter writer;
        writer.putU64(mapOutput::kMapTask, mapTask);
        writer.putU64(mapOutput::kPartition, partition);
        writer.putU64(mapOutput::kOffset, offset);
        writer.putBytes(mapOutput::kPayload, payload);
        return writer.take();
    }

    MapOutput MapOutput::decode(const std::vector<std::uint8_t>& body) {
        MapOutput message;
        std::uint32_t seen = 0;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case mapOutput::kMapTask:
                    message.mapTask = reader.asU64();
                    seen |= 1u << 0;
                    break;
                case mapOutput::kPartition:
                    message.partition = reader.asU64();
                    seen |= 1u << 1;
                    break;
                case mapOutput::kOffset:
                    message.offset = reader.asU64();
                    seen |= 1u << 2;
                    break;
                case mapOutput::kPayload:
                    message.payload = reader.asBytes();
                    seen |= 1u << 3;
                    break;
                default:
                    break;
            }
        }
        require((seen & 0b1111u) == 0b1111u, "map_output missing required field");
        return message;
    }

    std::vector<std::uint8_t> Fetch::encode() const {
        FieldWriter writer;
        writer.putU64(fetch::kMapTask, mapTask);
        writer.putU64(fetch::kPartition, partition);
        writer.putU64(fetch::kOffset, offset);
        return writer.take();
    }

    Fetch Fetch::decode(const std::vector<std::uint8_t>& body) {
        Fetch message;
        std::uint32_t seen = 0;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case fetch::kMapTask:
                    message.mapTask = reader.asU64();
                    seen |= 1u << 0;
                    break;
                case fetch::kPartition:
                    message.partition = reader.asU64();
                    seen |= 1u << 1;
                    break;
                case fetch::kOffset:
                    message.offset = reader.asU64();
                    break;
                default:
                    break;
            }
        }
        require((seen & 0b11u) == 0b11u, "fetch missing required field");
        return message;
    }

    std::vector<std::uint8_t> ResultMessage::encode() const {
        FieldWriter writer;
        writer.putU64(resultMsg::kReduceTask, reduceTask);
        writer.putU64(resultMsg::kOffset, offset);
        writer.putBytes(resultMsg::kPayload, payload);
        return writer.take();
    }

    ResultMessage ResultMessage::decode(const std::vector<std::uint8_t>& body) {
        ResultMessage message;
        std::uint32_t seen = 0;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case resultMsg::kReduceTask:
                    message.reduceTask = reader.asU64();
                    seen |= 1u << 0;
                    break;
                case resultMsg::kOffset:
                    message.offset = reader.asU64();
                    break;
                case resultMsg::kPayload:
                    message.payload = reader.asBytes();
                    seen |= 1u << 1;
                    break;
                default:
                    break;
            }
        }
        require((seen & 0b11u) == 0b11u, "result missing required field");
        return message;
    }

    std::vector<std::uint8_t> Done::encode() const {
        FieldWriter writer;
        writer.putU64(done::kKind, static_cast<std::uint64_t>(kind));
        writer.putU64(done::kTaskId, taskId);
        return writer.take();
    }

    Done Done::decode(const std::vector<std::uint8_t>& body) {
        Done message;
        std::uint32_t seen = 0;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case done::kKind:
                    message.kind = decodeKind(reader.asU64());
                    seen |= 1u << 0;
                    break;
                case done::kTaskId:
                    message.taskId = reader.asU64();
                    seen |= 1u << 1;
                    break;
                default:
                    break;
            }
        }
        require((seen & 0b11u) == 0b11u, "done missing required field");
        return message;
    }

    std::vector<std::uint8_t> Fail::encode() const {
        FieldWriter writer;
        writer.putU64(fail::kKind, static_cast<std::uint64_t>(kind));
        writer.putU64(fail::kTaskId, taskId);
        writer.putU64(fail::kStatusCode, static_cast<std::uint64_t>(statusCode));
        writer.putString(fail::kReason, reason);
        return writer.take();
    }

    Fail Fail::decode(const std::vector<std::uint8_t>& body) {
        Fail message;
        std::uint32_t seen = 0;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case fail::kKind:
                    message.kind = decodeKind(reader.asU64());
                    seen |= 1u << 0;
                    break;
                case fail::kTaskId:
                    message.taskId = reader.asU64();
                    seen |= 1u << 1;
                    break;
                case fail::kStatusCode:
                    message.statusCode = decodeStatus(reader.asU64());
                    seen |= 1u << 2;
                    break;
                case fail::kReason:
                    message.reason = reader.asString();
                    break;
                default:
                    break;
            }
        }
        require((seen & 0b111u) == 0b111u, "fail missing required field");
        return message;
    }

    std::vector<std::uint8_t> Ping::encode() const {
        FieldWriter writer;
        writer.putU64(ping::kNonce, nonce);
        return writer.take();
    }

    Ping Ping::decode(const std::vector<std::uint8_t>& body) {
        Ping message;
        bool seen = false;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == ping::kNonce) {
                message.nonce = reader.asU64();
                seen = true;
            }
        }
        require(seen, "ping missing nonce");
        return message;
    }

    std::vector<std::uint8_t> Pong::encode() const {
        FieldWriter writer;
        writer.putU64(ping::kNonce, nonce);
        return writer.take();
    }

    Pong Pong::decode(const std::vector<std::uint8_t>& body) {
        Pong message;
        bool seen = false;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == ping::kNonce) {
                message.nonce = reader.asU64();
                seen = true;
            }
        }
        require(seen, "pong missing nonce");
        return message;
    }

    std::vector<std::uint8_t> Stop::encode() const {
        FieldWriter writer;
        writer.putString(stop::kReason, reason);
        return writer.take();
    }

    Stop Stop::decode(const std::vector<std::uint8_t>& body) {
        Stop message;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == stop::kReason) {
                message.reason = reader.asString();
            }
        }
        return message;
    }

    std::vector<std::uint8_t> Shutdown::encode() const {
        return {};
    }

    Shutdown Shutdown::decode(const std::vector<std::uint8_t>& body) {
        FieldReader reader(body);
        while (reader.next()) {
        }
        return {};
    }

    std::vector<std::uint8_t> Submit::encode() const {
        FieldWriter writer;
        writer.putString(submit::kJob, job);
        writer.putU64(submit::kReducers, reducers);
        writer.putU64(submit::kWorkers, workers);
        writer.putString(submit::kOutput, output);
        for (const auto& input : inputs) {
            writer.putString(submit::kInput, input);
        }
        return writer.take();
    }

    Submit Submit::decode(const std::vector<std::uint8_t>& body) {
        Submit message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case submit::kJob:
                    message.job = reader.asString();
                    break;
                case submit::kReducers:
                    message.reducers = reader.asU64();
                    break;
                case submit::kWorkers:
                    message.workers = reader.asU64();
                    break;
                case submit::kOutput:
                    message.output = reader.asString();
                    break;
                case submit::kInput:
                    message.inputs.push_back(reader.asString());
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> SubmitAck::encode() const {
        FieldWriter writer;
        writer.putU64(submitAck::kStatusCode, static_cast<std::uint64_t>(statusCode));
        writer.putString(submitAck::kReason, reason);
        return writer.take();
    }

    SubmitAck SubmitAck::decode(const std::vector<std::uint8_t>& body) {
        SubmitAck message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case submitAck::kStatusCode:
                    message.statusCode = decodeStatus(reader.asU64());
                    break;
                case submitAck::kReason:
                    message.reason = reader.asString();
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> SubmitResult::encode() const {
        FieldWriter writer;
        writer.putU64(submitResult::kStatusCode, static_cast<std::uint64_t>(statusCode));
        writer.putString(submitResult::kOutput, output);
        writer.putString(submitResult::kReason, reason);
        return writer.take();
    }

    SubmitResult SubmitResult::decode(const std::vector<std::uint8_t>& body) {
        SubmitResult message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case submitResult::kStatusCode:
                    message.statusCode = decodeStatus(reader.asU64());
                    break;
                case submitResult::kOutput:
                    message.output = reader.asString();
                    break;
                case submitResult::kReason:
                    message.reason = reader.asString();
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> Pull::encode() const {
        FieldWriter writer;
        writer.putU64(pull::kMapTask, mapTask);
        writer.putU64(pull::kPartition, partition);
        return writer.take();
    }

    Pull Pull::decode(const std::vector<std::uint8_t>& body) {
        Pull message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case pull::kMapTask:
                    message.mapTask = reader.asU64();
                    break;
                case pull::kPartition:
                    message.partition = reader.asU64();
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> DataAddress::encode() const {
        FieldWriter writer;
        writer.putU64(dataAddress::kPort, port);
        return writer.take();
    }

    DataAddress DataAddress::decode(const std::vector<std::uint8_t>& body) {
        DataAddress message;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == dataAddress::kPort) {
                message.port = reader.asU64();
            }
        }
        return message;
    }
} // namespace xmr::protocol
