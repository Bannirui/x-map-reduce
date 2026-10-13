#include "protocol/messages.h"

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

std::vector<std::uint8_t> bodyOf(const std::vector<std::uint8_t>& frame) {
    using namespace xmr::protocol;
    return std::vector<std::uint8_t>(frame.begin() + kHeaderSize, frame.end());
}

}  // namespace

int main() {
    using namespace xmr::protocol;

    try {
        {
            Hello hello;
            hello.protocolVersion = kVersion;
            hello.workerId = "worker-7";
            hello.capabilities = 0x5;
            hello.pid = 4242;
            const auto decoded = Hello::decode(hello.encode());
            check(decoded.protocolVersion == kVersion, "hello version");
            check(decoded.workerId == "worker-7", "hello workerId");
            check(decoded.capabilities == 0x5, "hello capabilities");
            check(decoded.pid.has_value() && *decoded.pid == 4242, "hello pid");

            Hello noPid;
            noPid.workerId = "w";
            const auto decodedNoPid = Hello::decode(noPid.encode());
            check(!decodedNoPid.pid.has_value(), "hello without pid");
        }

        {
            RequestTask request;
            check(RequestTask::decode(request.encode()).encode().empty(), "request_task is empty");
        }

        {
            TaskAssignment task;
            task.kind = WorkKind::Map;
            task.taskId = 3;
            task.job = "word_count";
            task.reducers = 4;
            task.maps = 9;
            task.input = "asset/wordCount1.txt";
            const auto decoded = TaskAssignment::decode(task.encode());
            check(decoded.kind == WorkKind::Map, "task kind");
            check(decoded.taskId == 3, "task id");
            check(decoded.job == "word_count", "task job");
            check(decoded.reducers == 4, "task reducers");
            check(decoded.maps == 9, "task maps");
            check(decoded.input.has_value() && *decoded.input == "asset/wordCount1.txt", "task input");

            TaskAssignment reduce;
            reduce.kind = WorkKind::Reduce;
            reduce.taskId = 2;
            reduce.job = "word_count";
            reduce.reducers = 4;
            reduce.maps = 9;
            const auto decodedReduce = TaskAssignment::decode(reduce.encode());
            check(decodedReduce.kind == WorkKind::Reduce, "reduce kind");
            check(!decodedReduce.input.has_value(), "reduce has no input");
        }

        {
            Chunk data;
            data.offset = 1024;
            data.total = 8192;
            data.payload = std::vector<std::uint8_t>{0, 1, 2, 253, 254, 255};
            const auto decoded = Chunk::decode(data.encode());
            check(decoded.offset == 1024, "data offset");
            check(decoded.total.has_value() && *decoded.total == 8192, "data total");
            check(decoded.payload == data.payload, "data payload");

            Chunk noTotal;
            noTotal.payload = {9, 8, 7};
            const auto decodedNoTotal = Chunk::decode(noTotal.encode());
            check(!decodedNoTotal.total.has_value(), "data without total");
            check(decodedNoTotal.payload == noTotal.payload, "data payload without total");
        }

        {
            ReduceResult result;
            result.reduceTask = 0;
            result.offset = 64;
            result.payload = {1, 2, 3};
            const auto decoded = ReduceResult::decode(result.encode());
            check(decoded.reduceTask == 0 && decoded.offset == 64, "result ids");
            check(decoded.payload == result.payload, "result payload");
        }

        {
            Done done;
            done.kind = WorkKind::Reduce;
            done.taskId = 8;
            const auto decoded = Done::decode(done.encode());
            check(decoded.kind == WorkKind::Reduce && decoded.taskId == 8, "done fields");

            Fail fail;
            fail.kind = WorkKind::Map;
            fail.taskId = 1;
            fail.statusCode = StatusCode::Internal;
            fail.reason = "boom";
            const auto decodedFail = Fail::decode(fail.encode());
            check(decodedFail.kind == WorkKind::Map, "fail kind");
            check(decodedFail.taskId == 1, "fail taskId");
            check(decodedFail.statusCode == StatusCode::Internal, "fail status");
            check(decodedFail.reason == "boom", "fail reason");
        }

        {
            Ping ping;
            ping.nonce = 0xDEADBEEFu;
            check(Ping::decode(ping.encode()).nonce == ping.nonce, "ping nonce");

            Pong pong;
            pong.nonce = 99;
            check(Pong::decode(pong.encode()).nonce == 99, "pong nonce");

            JobFinished stop;
            stop.reason = "job finished";
            check(JobFinished::decode(stop.encode()).reason == "job finished", "stop reason");
        }

        {
            Hello hello;
            hello.workerId = "w";
            const auto frame = makeFrame(MessageType::Hello, 42, hello.encode(), static_cast<std::uint16_t>(Flag::More));
            const Header header = decodeHeader(frame.data(), frame.size());
            check(header.type == MessageType::Hello, "frame type");
            check(header.requestId == 42, "frame requestId");
            check(header.flags == static_cast<std::uint16_t>(Flag::More), "frame flags");
            check(Hello::decode(bodyOf(frame)).workerId == "w", "frame body round-trip");
        }

        {
            FieldWriter writer;
            writer.putString(999, "field from the future");
            writer.putU64(ping::kNonce, 7);
            check(Ping::decode(writer.take()).nonce == 7, "message should skip unknown field");
        }

        {
            FieldWriter writer;
            writer.putU64(hello::kProtocolVersion, kVersion);
            check(throwsProtocol([&] { Hello::decode(writer.take()); }),
                  "hello without workerId should throw");
        }

        {
            FieldWriter writer;
            writer.putU64(taskMsg::kKind, 3);
            writer.putU64(taskMsg::kTaskId, 0);
            writer.putString(taskMsg::kJob, "word_count");
            writer.putU64(taskMsg::kReducers, 1);
            writer.putU64(taskMsg::kMaps, 1);
            check(throwsProtocol([&] { TaskAssignment::decode(writer.take()); }),
                  "out-of-range work kind should throw");
        }

        {
            FieldWriter writer;
            writer.putU64(taskMsg::kKind, static_cast<std::uint64_t>(WorkKind::None));
            writer.putU64(taskMsg::kTaskId, 0);
            writer.putString(taskMsg::kJob, "word_count");
            writer.putU64(taskMsg::kReducers, 1);
            writer.putU64(taskMsg::kMaps, 1);
            check(throwsProtocol([&] { TaskAssignment::decode(writer.take()); }),
                  "unset work kind should throw");
        }

        {
            FieldWriter writer;
            writer.putU64(taskMsg::kKind, static_cast<std::uint64_t>(WorkKind::Map));
            writer.putU64(taskMsg::kTaskId, 0);
            writer.putString(taskMsg::kJob, "word_count");
            writer.putU64(taskMsg::kReducers, 1);
            writer.putU64(taskMsg::kMaps, 1);
            check(throwsProtocol([&] { TaskAssignment::decode(writer.take()); }),
                  "map task without input should throw");
        }

        {
            DataAddress address;
            address.port = 9331;
            address.blockPort = 9332;
            const auto decoded = DataAddress::decode(address.encode());
            check(decoded.port == 9331, "data_address port");
            check(decoded.blockPort == 9332, "data_address block port");

            DataAddress legacy;
            legacy.port = 9331;
            const auto decodedLegacy = DataAddress::decode(legacy.encode());
            check(decodedLegacy.blockPort == 0, "data_address block port defaults to 0");
        }

        {
            BlockManifest manifest;
            manifest.blocks = {"0,0,abc,5", "1,0,def,7"};
            const auto decoded = BlockManifest::decode(manifest.encode());
            check(decoded.blocks.size() == 2, "block_manifest count");
            check(decoded.blocks[0] == "0,0,abc,5", "block_manifest first entry");

            BlockPlacement placement;
            placement.statusCode = StatusCode::Ok;
            placement.replicas = {"abc,127.0.0.1,9332", "def,127.0.0.1,9333"};
            const auto decodedPlacement = BlockPlacement::decode(placement.encode());
            check(decodedPlacement.statusCode == StatusCode::Ok, "block_placement status");
            check(decodedPlacement.replicas.size() == 2
                  && decodedPlacement.replicas[1] == "def,127.0.0.1,9333", "block_placement replicas");

            UploadDone done;
            done.job = "word_count";
            check(UploadDone::decode(done.encode()).job == "word_count", "upload_done job");

            ReplicateBlock replicate;
            replicate.blockId = "abc";
            replicate.sourceHost = "127.0.0.1";
            replicate.sourcePort = 9332;
            const auto decodedReplicate = ReplicateBlock::decode(replicate.encode());
            check(decodedReplicate.blockId == "abc" && decodedReplicate.sourceHost == "127.0.0.1"
                  && decodedReplicate.sourcePort == 9332, "replicate_block round-trip");

            BlockReport report;
            report.blocks = {"abc", "def"};
            const auto decodedReport = BlockReport::decode(report.encode());
            check(decodedReport.blocks.size() == 2 && decodedReport.blocks[1] == "def",
                  "block_report round-trip");

            DeleteBlock del;
            del.blockId = "abc";
            check(DeleteBlock::decode(del.encode()).blockId == "abc", "delete_block round-trip");
        }

        {
            const std::vector<std::uint8_t> truncated{0x01, 0x00, 0x00};
            check(throwsProtocol([&] { Ping::decode(truncated); }), "truncated message should throw");
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
