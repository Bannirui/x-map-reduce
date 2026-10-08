#include"net/net.h"
#include"protocol/messages.h"
#include"runtime/exit_code.h"
#include"runtime/jobs.h"
#include"runtime/plugin.h"
#include"runtime/scheduler.h"
#include"runtime/task.h"
#include"runtime/wire.h"

#include<algorithm>
#include<cerrno>
#include<cstdint>
#include<fstream>
#include<iostream>
#include<iterator>
#include<poll.h>
#include<stdexcept>
#include<string>
#include<utility>
#include<vector>

#include<sys/socket.h>
#include<unistd.h>

namespace {
    void usage(const char* program) {
        std::cerr << "Usage: " << program
            << " --job <name> [--plugin <path>]... [--reducers <R>] [--workers <N>]"
            << " [--listen <host:port>] --output <file> <input...>\n";
    }

    /**
     * @param endpoint host:port格式
     */
    std::pair<std::string, std::uint16_t> parseEndpoint(const std::string& endpoint) {
        const auto colon = endpoint.rfind(':');
        if (colon == std::string::npos) {
            throw std::runtime_error("expected host:port in '" + endpoint + "'");
        }
        return {endpoint.substr(0, colon),
                static_cast<std::uint16_t>(std::stoul(endpoint.substr(colon + 1)))};
    }

    xmr::protocol::WorkKind wireKind(xmr::TaskKind kind) {
        return kind == xmr::TaskKind::Map ? xmr::protocol::WorkKind::Map : xmr::protocol::WorkKind::Reduce;
    }

    xmr::TaskKind taskKind(xmr::protocol::WorkKind kind) {
        return kind == xmr::protocol::WorkKind::Map ? xmr::TaskKind::Map : xmr::TaskKind::Reduce;
    }

    xmr::protocol::TaskMessage toTaskMessage(const xmr::Task& task) {
        xmr::protocol::TaskMessage message;
        message.kind = wireKind(task.kind);
        message.taskId = task.id;
        message.job = task.job;
        message.reducers = task.reducers;
        message.maps = task.maps;
        if (task.kind == xmr::TaskKind::Map) {
            message.input = task.input;
        }
        return message;
    }

    std::vector<std::uint8_t> toBytes(const std::string& text) {
        return std::vector<std::uint8_t>(text.begin(), text.end());
    }

    // 转字符串
    std::string toString(const std::vector<std::uint8_t>& bytes) {
        return std::string(bytes.begin(), bytes.end());
    }

    struct Worker {
        xmr::net::Connection connection;
        // worker节点状态
        bool idle = false;
        std::string id;
        std::uint32_t lastRequest = 0;
    };
} // namespace

int main(int argc, char** argv) {
    // MapReduce要处理的文件 现在没有文件系统 模拟大数据
    std::vector<std::string> inputs;
    std::string outputFile;
    // 用户提交给MapReduce的任务
    std::string jobName;
    // 用户提交的任务ABI实现
    std::vector<std::string> plugins;
    // todo 后面master管理worker后这个参数就没用了
    std::size_t expectedWorkers = 1;
    // reduce任务数 map要用它做分区
    std::size_t reducers = 3;
    // master端口
    std::string listen = "127.0.0.1:9527";

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--job" && i + 1 < argc) {
            jobName = argv[++i];
        } else if (arg == "--plugin" && i + 1 < argc) {
            plugins.emplace_back(argv[++i]);
        } else if (arg == "--reducers" && i + 1 < argc) {
            reducers = std::stoul(argv[++i]);
        } else if (arg == "--workers" && i + 1 < argc) {
            expectedWorkers = std::stoul(argv[++i]);
        } else if (arg == "--listen" && i + 1 < argc) {
            listen = argv[++i];
        } else if (arg == "--output" && i + 1 < argc) {
            outputFile = argv[++i];
        } else {
            inputs.emplace_back(arg);
        }
    }

    try {
        if (jobName.empty() || outputFile.empty() || inputs.empty() || reducers == 0 || expectedWorkers == 0) {
            throw UsageError("invalid or missing arguments");
        }

        for (const auto& plugin : plugins) {
            // job用插件形式注册到框架
            loadJobPlugin(plugin);
        }
        // 上面job注册过了 校验在系统中注册成功了
        if (findJob(jobName) == nullptr) {
            throw UsageError("unknown job '" + jobName + "'");
        }
        // todo 没有文件系统 用户提交的任务是给到的master master把要处理的任务走网络派发到worker上给map用
        std::vector<std::string> inputData;
        inputData.reserve(inputs.size());
        for (const auto& path : inputs) {
            std::ifstream in(path, std::ios::binary);
            if (!in) {
                throw std::runtime_error("failed to open input file: " + path);
            }
            inputData.emplace_back(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }

        const auto [host,port] = parseEndpoint(listen);
        xmr::net::Listener listener(host, port);
        std::cout << "LISTENING " << host << ":" << listener.port() << std::endl;

        xmr::Scheduler scheduler(jobName, inputs, reducers);
        // worker启动时候会连进来 master管理的worker节点
        std::vector<Worker> workers;

        // M*R 每个map任务要对自己的输出进行R的分区
        std::vector<std::vector<std::string> > mapOutput(inputs.size(), std::vector<std::string>(reducers));
        std::vector<KeyValue> merged;

        // master收worker发过来的消息
        auto handleMessage = [&](Worker& worker) {
            // 收到来自worker的消息
            const xmr::protocol::Frame frame = xmr::receiveFrame(worker.connection.fd());
            // 源文本里面第一个字段是命令名
            switch (frame.header.type) {
                case xmr::protocol::MessageType::Hello: {
                    // worker启动的时候给master发一下
                    worker.id = xmr::protocol::Hello::decode(frame.body).workerId;
                    break;
                }
                case xmr::protocol::MessageType::RequestTask: {
                    // worker告诉master它空闲了 希望master给它派任务
                    worker.idle = true;
                    worker.lastRequest = frame.header.requestId;
                    break;
                }
                case xmr::protocol::MessageType::Done: {
                    const auto done = xmr::protocol::Done::decode(frame.body);
                    scheduler.markDone(taskKind(done.kind));
                    worker.idle = false;
                    break;
                }
                case xmr::protocol::MessageType::Fail: {
                    const auto fail = xmr::protocol::Fail::decode(frame.body);
                    scheduler.markFailed(taskKind(fail.kind), fail.taskId, fail.reason);
                    worker.idle = false;
                    break;
                }
                case xmr::protocol::MessageType::MapOutput: {
                    // worker告诉master它完成了map任务 并把中间结果发送过来了
                    const auto output = xmr::protocol::MapOutput::decode(frame.body);
                    if (output.mapTask >= mapOutput.size() || output.partition >= reducers) {
                        throw std::runtime_error("MAPOUT out of range");
                    }
                    mapOutput[output.mapTask][output.partition] = toString(output.payload);
                    break;
                }
                case xmr::protocol::MessageType::Fetch: {
                    // worker准备执行reduce 跟master要reduce需要的kv
                    const auto fetch = xmr::protocol::Fetch::decode(frame.body);
                    if (fetch.mapTask >= mapOutput.size() || fetch.partition >= reducers) {
                        throw std::runtime_error("FETCH out of range");
                    }
                    const std::string& blob = mapOutput[fetch.mapTask][fetch.partition];
                    xmr::protocol::DataMessage data;
                    data.offset = 0;
                    data.total = blob.size();
                    data.payload = toBytes(blob);
                    xmr::sendFrame(worker.connection.fd(), xmr::protocol::MessageType::Data,
                                   frame.header.requestId, data.encode());
                    break;
                }
                case xmr::protocol::MessageType::InputRequest: {
                    // worker准备执行map函数了 跟master要map需要的文件数据
                    const auto request = xmr::protocol::InputRequest::decode(frame.body);
                    if (request.taskId >= inputData.size()) {
                        throw std::runtime_error("INPUT out of range");
                    }
                    const std::string& blob = inputData[request.taskId];
                    xmr::protocol::DataMessage data;
                    data.offset = 0;
                    data.total = blob.size();
                    data.payload = toBytes(blob);
                    xmr::sendFrame(worker.connection.fd(), xmr::protocol::MessageType::Data,
                                   frame.header.requestId, data.encode());
                    break;
                }
                case xmr::protocol::MessageType::Result: {
                    // worker窒息给你执行完了reduce 把最终结果给到了master
                    const auto result = xmr::protocol::ResultMessage::decode(frame.body);
                    auto pairs = xmr::deserializeKeyValues(toString(result.payload));
                    merged.insert(merged.end(),
                                  std::make_move_iterator(pairs.begin()), std::make_move_iterator(pairs.end()));
                    break;
                }
                default:
                    throw std::runtime_error("unexpected control message");
            }
        };

        auto dispatch = [&] {
            if (workers.size() < expectedWorkers) {
                return;
            }
            for (auto& worker : workers) {
                if (!worker.idle) {
                    continue;
                }
                auto task = scheduler.takeTask();
                if (!task) {
                    break;
                }
                // master向worker派发任务
                const auto message = toTaskMessage(*task);
                xmr::sendFrame(worker.connection.fd(), xmr::protocol::MessageType::Task,
                               worker.lastRequest, message.encode());
                worker.idle = false;
            }
        };

        while (!scheduler.finished() && !scheduler.failed()) {
            std::vector<pollfd> fds;
            fds.push_back(pollfd{listener.fd(),POLLIN, 0});
            for (const auto& worker : workers) {
                fds.push_back(pollfd{worker.connection.fd(),POLLIN, 0});
            }

            const int ready = ::poll(fds.data(), static_cast<nfds_t>(fds.size()), -1);
            if (ready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw std::runtime_error("poll failed");
            }

            for (std::size_t index = 1; index < fds.size(); ++index) {
                if (fds[index].revents & (POLLIN | POLLHUP | POLLERR)) {
                    handleMessage(workers[index - 1]);
                }
            }

            if (fds[0].revents & POLLIN) {
                workers.push_back(Worker{listener.accept(), false});
            }

            dispatch();
        }

        for (auto& worker : workers) {
            try {
                xmr::protocol::Stop stop;
                xmr::sendFrame(worker.connection.fd(), xmr::protocol::MessageType::Stop, 0, stop.encode());
                // Half-close, then drain any in-flight REQUEST so closing the
                // socket does not reset the connection underneath the worker.
                ::shutdown(worker.connection.fd(),SHUT_WR);
                char buffer[256];
                while (::read(worker.connection.fd(), buffer, sizeof(buffer)) > 0) {
                }
            } catch (const std::exception&) {
                // Worker already disconnected; nothing to do at shutdown.
            }
        }

        if (scheduler.failed()) {
            throw std::runtime_error(scheduler.error());
        }

        std::stable_sort(merged.begin(), merged.end(),
                         [](const KeyValue& a, const KeyValue& b) {
                             return a.first < b.first;
                         });
        WriteKeyValues(outputFile, merged);
    } catch (const UsageError& error) {
        std::cerr << "coordinator: " << error.what() << '\n';
        usage(argv[0]);
        return static_cast<int>(ExitCode::Usage);
    } catch (const std::exception& error) {
        std::cerr << "coordinator: " << error.what() << '\n';
        return static_cast<int>(ExitCode::Failure);
    }

    return static_cast<int>(ExitCode::Success);
}
