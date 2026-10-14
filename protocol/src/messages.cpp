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

    std::vector<std::uint8_t> RequestTask::encode() const {
        return {};
    }

    RequestTask RequestTask::decode(const std::vector<std::uint8_t>& body) {
        FieldReader reader(body);
        while (reader.next()) {
        }
        return {};
    }

    std::vector<std::uint8_t> TaskAssignment::encode() const {
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
        for (const auto& block : blocks) {
            writer.putString(taskMsg::kBlock, block);
        }
        return writer.take();
    }

    TaskAssignment TaskAssignment::decode(const std::vector<std::uint8_t>& body) {
        TaskAssignment message;
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
                case taskMsg::kBlock:
                    message.blocks.push_back(reader.asString());
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

    std::vector<std::uint8_t> Chunk::encode() const {
        FieldWriter writer;
        writer.putU64(data::kOffset, offset);
        if (total) {
            writer.putU64(data::kTotal, *total);
        }
        writer.putBytes(data::kPayload, payload);
        return writer.take();
    }

    Chunk Chunk::decode(const std::vector<std::uint8_t>& body) {
        Chunk message;
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

    std::vector<std::uint8_t> ReduceResult::encode() const {
        FieldWriter writer;
        writer.putU64(resultMsg::kReduceTask, reduceTask);
        writer.putU64(resultMsg::kOffset, offset);
        writer.putBytes(resultMsg::kPayload, payload);
        return writer.take();
    }

    ReduceResult ReduceResult::decode(const std::vector<std::uint8_t>& body) {
        ReduceResult message;
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

    std::vector<std::uint8_t> JobFinished::encode() const {
        FieldWriter writer;
        writer.putString(stop::kReason, reason);
        return writer.take();
    }

    JobFinished JobFinished::decode(const std::vector<std::uint8_t>& body) {
        JobFinished message;
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
        writer.putString(submit::kOutput, output);
        writer.putString(submit::kPluginHash, pluginHash);
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
                case submit::kOutput:
                    message.output = reader.asString();
                    break;
                case submit::kPluginHash:
                    message.pluginHash = reader.asString();
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
        writer.putString(submitAck::kDataHost, dataHost);
        writer.putU64(submitAck::kDataPort, dataPort);
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
                case submitAck::kDataHost:
                    message.dataHost = reader.asString();
                    break;
                case submitAck::kDataPort:
                    message.dataPort = reader.asU64();
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
        writer.putU64(dataAddress::kBlockPort, blockPort);
        return writer.take();
    }

    DataAddress DataAddress::decode(const std::vector<std::uint8_t>& body) {
        DataAddress message;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == dataAddress::kPort) {
                message.port = reader.asU64();
            } else if (reader.id() == dataAddress::kBlockPort) {
                message.blockPort = reader.asU64();
            }
        }
        return message;
    }

    std::vector<std::uint8_t> Plugin::encode() const {
        FieldWriter writer;
        writer.putString(plugin::kHash, hash);
        writer.putString(plugin::kJob, job);
        writer.putU64(plugin::kOffset, offset);
        writer.putBytes(plugin::kPayload, payload);
        return writer.take();
    }

    Plugin Plugin::decode(const std::vector<std::uint8_t>& body) {
        Plugin message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case plugin::kHash:
                    message.hash = reader.asString();
                    break;
                case plugin::kJob:
                    message.job = reader.asString();
                    break;
                case plugin::kOffset:
                    message.offset = reader.asU64();
                    break;
                case plugin::kPayload:
                    message.payload = reader.asBytes();
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> PluginAck::encode() const {
        FieldWriter writer;
        writer.putString(pluginAck::kHash, hash);
        writer.putU64(pluginAck::kOk, ok ? 1u : 0u);
        writer.putString(pluginAck::kReason, reason);
        return writer.take();
    }

    PluginAck PluginAck::decode(const std::vector<std::uint8_t>& body) {
        PluginAck message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case pluginAck::kHash:
                    message.hash = reader.asString();
                    break;
                case pluginAck::kOk:
                    message.ok = reader.asU64() != 0;
                    break;
                case pluginAck::kReason:
                    message.reason = reader.asString();
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> PullInput::encode() const {
        FieldWriter writer;
        writer.putU64(pullInput::kTaskId, taskId);
        return writer.take();
    }

    PullInput PullInput::decode(const std::vector<std::uint8_t>& body) {
        PullInput message;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == pullInput::kTaskId) {
                message.taskId = reader.asU64();
            }
        }
        return message;
    }

    std::vector<std::uint8_t> PullPlugin::encode() const {
        FieldWriter writer;
        writer.putString(pullPlugin::kHash, hash);
        return writer.take();
    }

    PullPlugin PullPlugin::decode(const std::vector<std::uint8_t>& body) {
        PullPlugin message;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == pullPlugin::kHash) {
                message.hash = reader.asString();
            }
        }
        return message;
    }

    std::vector<std::uint8_t> MasterData::encode() const {
        FieldWriter writer;
        writer.putU64(masterData::kPort, port);
        return writer.take();
    }

    MasterData MasterData::decode(const std::vector<std::uint8_t>& body) {
        MasterData message;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == masterData::kPort) {
                message.port = reader.asU64();
            }
        }
        return message;
    }

    std::vector<std::uint8_t> FetchPlugin::encode() const {
        FieldWriter writer;
        writer.putString(needPlugin::kHash, hash);
        writer.putString(needPlugin::kJob, job);
        return writer.take();
    }

    FetchPlugin FetchPlugin::decode(const std::vector<std::uint8_t>& body) {
        FetchPlugin message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case needPlugin::kHash:
                    message.hash = reader.asString();
                    break;
                case needPlugin::kJob:
                    message.job = reader.asString();
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> Progress::encode() const {
        FieldWriter writer;
        writer.putU64(progress::kKind, static_cast<std::uint64_t>(kind));
        writer.putU64(progress::kTaskId, taskId);
        writer.putU64(progress::kFraction, fraction);
        return writer.take();
    }

    Progress Progress::decode(const std::vector<std::uint8_t>& body) {
        Progress message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case progress::kKind:
                    message.kind = decodeKind(reader.asU64());
                    break;
                case progress::kTaskId:
                    message.taskId = reader.asU64();
                    break;
                case progress::kFraction:
                    message.fraction = reader.asU64();
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> Cancel::encode() const {
        FieldWriter writer;
        writer.putU64(cancel::kKind, static_cast<std::uint64_t>(kind));
        writer.putU64(cancel::kTaskId, taskId);
        return writer.take();
    }

    Cancel Cancel::decode(const std::vector<std::uint8_t>& body) {
        Cancel message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case cancel::kKind:
                    message.kind = decodeKind(reader.asU64());
                    break;
                case cancel::kTaskId:
                    message.taskId = reader.asU64();
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> BlockManifest::encode() const {
        FieldWriter writer;
        for (const auto& block : blocks) {
            writer.putString(blockManifest::kBlock, block);
        }
        return writer.take();
    }

    BlockManifest BlockManifest::decode(const std::vector<std::uint8_t>& body) {
        BlockManifest message;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == blockManifest::kBlock) {
                message.blocks.push_back(reader.asString());
            }
        }
        return message;
    }

    std::vector<std::uint8_t> BlockPlacement::encode() const {
        FieldWriter writer;
        writer.putU64(blockPlacement::kStatusCode, static_cast<std::uint64_t>(statusCode));
        writer.putString(blockPlacement::kReason, reason);
        for (const auto& replica : replicas) {
            writer.putString(blockPlacement::kReplica, replica);
        }
        return writer.take();
    }

    BlockPlacement BlockPlacement::decode(const std::vector<std::uint8_t>& body) {
        BlockPlacement message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case blockPlacement::kStatusCode:
                    message.statusCode = decodeStatus(reader.asU64());
                    break;
                case blockPlacement::kReason:
                    message.reason = reader.asString();
                    break;
                case blockPlacement::kReplica:
                    message.replicas.push_back(reader.asString());
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> UploadDone::encode() const {
        FieldWriter writer;
        writer.putString(uploadDone::kJob, job);
        return writer.take();
    }

    UploadDone UploadDone::decode(const std::vector<std::uint8_t>& body) {
        UploadDone message;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == uploadDone::kJob) {
                message.job = reader.asString();
            }
        }
        return message;
    }

    std::vector<std::uint8_t> ReplicateBlock::encode() const {
        FieldWriter writer;
        writer.putString(replicateBlock::kBlockId, blockId);
        writer.putString(replicateBlock::kSourceHost, sourceHost);
        writer.putU64(replicateBlock::kSourcePort, sourcePort);
        return writer.take();
    }

    ReplicateBlock ReplicateBlock::decode(const std::vector<std::uint8_t>& body) {
        ReplicateBlock message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case replicateBlock::kBlockId:
                    message.blockId = reader.asString();
                    break;
                case replicateBlock::kSourceHost:
                    message.sourceHost = reader.asString();
                    break;
                case replicateBlock::kSourcePort:
                    message.sourcePort = reader.asU64();
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> BlockReport::encode() const {
        FieldWriter writer;
        for (const auto& block : blocks) {
            writer.putString(blockReport::kBlock, block);
        }
        return writer.take();
    }

    BlockReport BlockReport::decode(const std::vector<std::uint8_t>& body) {
        BlockReport message;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == blockReport::kBlock) {
                message.blocks.push_back(reader.asString());
            }
        }
        return message;
    }

    std::vector<std::uint8_t> DeleteBlock::encode() const {
        FieldWriter writer;
        writer.putString(deleteBlock::kBlockId, blockId);
        return writer.take();
    }

    DeleteBlock DeleteBlock::decode(const std::vector<std::uint8_t>& body) {
        DeleteBlock message;
        FieldReader reader(body);
        while (reader.next()) {
            if (reader.id() == deleteBlock::kBlockId) {
                message.blockId = reader.asString();
            }
        }
        return message;
    }

    std::vector<std::uint8_t> CommitFile::encode() const {
        FieldWriter writer;
        writer.putString(commitFile::kPath, path);
        for (const auto& block : blocks) {
            writer.putString(commitFile::kBlock, block);
        }
        return writer.take();
    }

    CommitFile CommitFile::decode(const std::vector<std::uint8_t>& body) {
        CommitFile message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case commitFile::kPath:
                    message.path = reader.asString();
                    break;
                case commitFile::kBlock:
                    message.blocks.push_back(reader.asString());
                    break;
                default:
                    break;
            }
        }
        return message;
    }

    std::vector<std::uint8_t> CommitAck::encode() const {
        FieldWriter writer;
        writer.putU64(commitAck::kStatusCode, static_cast<std::uint64_t>(statusCode));
        writer.putString(commitAck::kReason, reason);
        return writer.take();
    }

    CommitAck CommitAck::decode(const std::vector<std::uint8_t>& body) {
        CommitAck message;
        FieldReader reader(body);
        while (reader.next()) {
            switch (reader.id()) {
                case commitAck::kStatusCode:
                    message.statusCode = decodeStatus(reader.asU64());
                    break;
                case commitAck::kReason:
                    message.reason = reader.asString();
                    break;
                default:
                    break;
            }
        }
        return message;
    }
} // namespace xmr::protocol
