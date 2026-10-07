#include"net/net.h"
#include"runtime/exit_code.h"
#include"runtime/plugin.h"
#include"runtime/task.h"

#include<cstdint>
#include<iostream>
#include<stdexcept>
#include<string>
#include<utility>
#include<vector>

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

    std::string kindToken(xmr::TaskKind kind) {
        return kind == xmr::TaskKind::Map ? "MAP" : "REDUCE";
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
        // 发个探测协议
        connection.send(toBytes("HELLO"));

        while (true) {
            // worker告诉master我空闲了 给我个任务
            connection.send(toBytes("REQUEST"));
            const std::string message = toString(connection.receive());
            // master告诉worker任务结束了 可以关停了
            if (message == "STOP") {
                break;
            }
            if (message.rfind("TASK\t", 0) != 0) {
                throw std::runtime_error("unexpected coordinator message");
            }
            // worker收到master派发的任务
            const xmr::Task task = xmr::Task::deserialize(message.substr(5));
            try {
                if (task.kind == xmr::TaskKind::Map) {
                    // worker收到master派发的map任务 跟master要这个map任务的数据
                    connection.send(toBytes("INPUT\t" + std::to_string(task.id)));
                    // worker收到master给的map任务数据
                    const std::string input = toString(connection.receive());
                    if (input.rfind("DATA\t", 0) != 0) {
                        throw std::runtime_error("expected DATA reply from coordinator");
                    }
                    // map产出的中间结果 已经按照R分区好了 现在还放在worker的内存上 等着shuffle
                    const auto parts = xmr::runMapTask(task, input.substr(5));
                    for (std::size_t r = 0; r < parts.size(); ++r) {
                        // todo 论文里面master只负责管理元数据 业务数据是不管的 我们的架构里面先让master负责shuffle 把所有的中间结果网络发给master
                        connection.send(toBytes(
                            "MAPOUT\t" + std::to_string(task.id) + "\t" +
                            std::to_string(r) + "\t" + xmr::serializeKeyValues(parts[r])));
                    }
                } else {
                    // worker收到master派发的reduce任务
                    auto fetch = [&](std::size_t mapTask, std::size_t partition) {
                        connection.send(toBytes("FETCH\t" + std::to_string(mapTask)
                                                + "\t" + std::to_string(partition)));
                        const std::string data = toString(connection.receive());
                        if (data.rfind("DATA\t", 0) != 0) {
                            throw std::runtime_error("expected DATA reply from coordinator");
                        }
                        return xmr::deserializeKeyValues(data.substr(5));
                    };
                    const auto result = xmr::runReduceTask(task, fetch);
                    connection.send(toBytes("RESULT\t" + std::to_string(task.id)
                                            + "\t" + xmr::serializeKeyValues(result)));
                }
                connection.send(toBytes("DONE\t" + kindToken(task.kind) + "\t" + std::to_string(task.id)));
            } catch (const std::exception& error) {
                connection.send(toBytes("FAIL\t" + kindToken(task.kind) + "\t"
                                        + std::to_string(task.id) + "\t" + error.what()));
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