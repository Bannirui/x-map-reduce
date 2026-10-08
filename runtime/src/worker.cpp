#include"net/net.h"
#include"protocol/messages.h"
#include"runtime/exit_code.h"
#include"runtime/plugin.h"
#include"runtime/task.h"
#include"runtime/wire.h"

#include<cstdint>
#include<iostream>
#include<stdexcept>
#include<string>
#include<utility>
#include<vector>

#include<unistd.h>

namespace {
    std::vector<std::uint8_t> toBytes(const std::string& text) {
        return std::vector<std::uint8_t>(text.begin(), text.end());
    }

    std::string toString(const std::vector<std::uint8_t>& bytes) {
        return std::string(bytes.begin(), bytes.end());
    }

    std::pair<std::string, std::string> parseEndpoint(const std::string& endpoint) {
        const auto colon = endpoint.rfind(':');
        if (colon == std::string::npos) {
            throw std::runtime_error("expected host:port in '" + endpoint + "'");
        }
        return {endpoint.substr(0, colon), endpoint.substr(colon + 1)};
    }

    xmr::TaskKind taskKind(xmr::protocol::WorkKind kind) {
        return kind == xmr::protocol::WorkKind::Map ? xmr::TaskKind::Map : xmr::TaskKind::Reduce;
    }

    xmr::protocol::WorkKind wireKind(xmr::TaskKind kind) {
        return kind == xmr::TaskKind::Map ? xmr::protocol::WorkKind::Map : xmr::protocol::WorkKind::Reduce;
    }

    xmr::Task toTask(const xmr::protocol::TaskMessage& message) {
        xmr::Task task;
        task.kind = taskKind(message.kind);
        task.id = message.taskId;
        task.job = message.job;
        task.reducers = message.reducers;
        task.maps = message.maps;
        if (message.kind == xmr::protocol::WorkKind::Map) {
            task.input = *message.input;
        }
        return task;
    }

    void usage(const char* program) {
        std::cerr << "Usage: " << program << " --coordinator <host:port> [--plugin <path>]...\n";
    }
} // namespace

int main(int argc, char** argv) {
    // master的host:port
    std::string coordinator;
    // job的插件
    std::vector<std::string> plugins;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--coordinator" && i + 1 < argc) {
            coordinator = argv[++i];
        } else if (arg == "--plugin" && i + 1 < argc) {
            plugins.emplace_back(argv[++i]);
        }
    }

    try {
        if (coordinator.empty()) {
            throw UsageError("missing --coordinator");
        }
        // 加载job插件
        for (const auto& plugin : plugins) {
            loadJobPlugin(plugin);
        }
        const auto [host,portText] = parseEndpoint(coordinator);
        const std::uint16_t port = static_cast<std::uint16_t>(std::stoul(portText));
        // TCP连接
        xmr::net::Connection connection = xmr::net::connectTo(host, port);
        std::uint32_t requestId = 0;

        // 发个探测协议
        xmr::protocol::Hello hello;
        hello.workerId = "worker-" + std::to_string(::getpid());
        hello.pid = static_cast<std::uint64_t>(::getpid());
        xmr::sendFrame(connection.fd(), xmr::protocol::MessageType::Hello, 0, hello.encode());

        while (true) {
            // worker告诉master我空闲了 给我个任务
            xmr::sendFrame(connection.fd(), xmr::protocol::MessageType::RequestTask, ++requestId, {});
            const xmr::protocol::Frame frame = xmr::receiveFrame(connection.fd());
            // master告诉worker任务结束了 可以关停了
            if (frame.header.type == xmr::protocol::MessageType::Stop) {
                break;
            }
            if (frame.header.type != xmr::protocol::MessageType::Task) {
                throw std::runtime_error("unexpected coordinator message");
            }
            // worker收到master派发的任务
            const xmr::Task task = toTask(xmr::protocol::TaskMessage::decode(frame.body));
            try {
                if (task.kind == xmr::TaskKind::Map) {
                    // worker收到master派发的map任务 跟master要这个map任务的数据
                    xmr::protocol::InputRequest request;
                    request.taskId = task.id;
                    xmr::sendFrame(connection.fd(), xmr::protocol::MessageType::InputRequest,
                                   ++requestId, request.encode());
                    // worker收到master给的map任务数据
                    const xmr::protocol::Frame input = xmr::receiveFrame(connection.fd());
                    if (input.header.type != xmr::protocol::MessageType::Data) {
                        throw std::runtime_error("expected DATA reply from coordinator");
                    }
                    // map产出的中间结果 已经按照R分区好了 现在还放在worker的内存上 等着shuffle
                    const auto parts = xmr::runMapTask(task, toString(xmr::protocol::DataMessage::decode(input.body).payload));
                    for (std::size_t r = 0; r < parts.size(); ++r) {
                        // todo 论文里面master只负责管理元数据 业务数据是不管的 我们的架构里面先让master负责shuffle 把所有的中间结果网络发给master
                        xmr::protocol::MapOutput output;
                        output.mapTask = task.id;
                        output.partition = r;
                        output.offset = 0;
                        output.payload = toBytes(xmr::serializeKeyValues(parts[r]));
                        xmr::sendFrame(connection.fd(), xmr::protocol::MessageType::MapOutput, 0, output.encode());
                    }
                } else {
                    // worker收到master派发的reduce任务
                    auto fetch = [&](std::size_t mapTask, std::size_t partition) {
                        xmr::protocol::Fetch request;
                        request.mapTask = mapTask;
                        request.partition = partition;
                        request.offset = 0;
                        xmr::sendFrame(connection.fd(), xmr::protocol::MessageType::Fetch,
                                       ++requestId, request.encode());
                        const xmr::protocol::Frame data = xmr::receiveFrame(connection.fd());
                        if (data.header.type != xmr::protocol::MessageType::Data) {
                            throw std::runtime_error("expected DATA reply from coordinator");
                        }
                        return xmr::deserializeKeyValues(toString(xmr::protocol::DataMessage::decode(data.body).payload));
                    };
                    const auto result = xmr::runReduceTask(task, fetch);
                    xmr::protocol::ResultMessage message;
                    message.reduceTask = task.id;
                    message.offset = 0;
                    message.payload = toBytes(xmr::serializeKeyValues(result));
                    xmr::sendFrame(connection.fd(), xmr::protocol::MessageType::Result, 0, message.encode());
                }
                xmr::protocol::Done done;
                done.kind = wireKind(task.kind);
                done.taskId = task.id;
                xmr::sendFrame(connection.fd(), xmr::protocol::MessageType::Done, 0, done.encode());
            } catch (const std::exception& error) {
                xmr::protocol::Fail fail;
                fail.kind = wireKind(task.kind);
                fail.taskId = task.id;
                fail.statusCode = xmr::protocol::StatusCode::Internal;
                fail.reason = error.what();
                xmr::sendFrame(connection.fd(), xmr::protocol::MessageType::Fail, 0, fail.encode());
            }
        }
    } catch (const UsageError& error) {
        std::cerr << "worker: " << error.what() << '\n';
        usage(argv[0]);
        return static_cast<int>(ExitCode::Usage);
    } catch (const std::exception& error) {
        std::cerr << "worker: " << error.what() << '\n';
        return static_cast<int>(ExitCode::Failure);
    }
    return static_cast<int>(ExitCode::Success);
}
