#include"net/event_loop.h"
#include"net/net.h"
#include"protocol/framing.h"
#include"protocol/messages.h"
#include"runtime/exit_code.h"
#include"runtime/jobs.h"
#include"runtime/plugin.h"
#include"runtime/scheduler.h"
#include"runtime/task.h"
#include"runtime/task_codec.h"
#include"runtime/worker_registry.h"

#include<algorithm>
#include<chrono>
#include<cstdint>
#include<fstream>
#include<iostream>
#include<iterator>
#include<memory>
#include<stdexcept>
#include<string>
#include<unordered_map>
#include<utility>
#include<vector>

#include<sys/socket.h>
#include<unistd.h>

namespace {
    // master判定worker心跳超时的阈值
    constexpr std::chrono::seconds kHeartbeatTimeout{6};
    // 单个任务执行超时的阈值 超了就重发
    constexpr std::chrono::seconds kTaskTimeout{30};

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

    std::vector<std::uint8_t> toBytes(const std::string& text) {
        return std::vector<std::uint8_t>(text.begin(), text.end());
    }

    // 转字符串
    std::string toString(const std::vector<std::uint8_t>& bytes) {
        return std::string(bytes.begin(), bytes.end());
    }

    struct Worker {
        xmr::net::Connection connection;
        xmr::net::ByteBuffer in;
        xmr::net::ByteBuffer out;
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
        xmr::net::setNonBlocking(listener.fd());
        std::cout << "LISTENING " << host << ":" << listener.port() << std::endl;

        xmr::Scheduler scheduler(jobName, inputs, reducers);
        // worker启动时候会连进来 master管理的worker节点
        std::vector<std::unique_ptr<Worker> > workers;
        std::unordered_map<int, Worker*> byFd;

        xmr::net::Poller poller;
        poller.add(listener.fd(), xmr::net::kReadable);
        xmr::WorkerRegistry registry(kHeartbeatTimeout, kTaskTimeout);
        std::unordered_map<std::string, Worker*> byId;

        // M*R 每个map任务要对自己的输出进行R的分区
        std::vector<std::vector<std::string> > mapOutput(inputs.size(), std::vector<std::string>(reducers));
        std::vector<KeyValue> merged;

        auto updateInterest = [&](Worker& worker) {
            std::uint32_t events = xmr::net::kReadable;
            if (!worker.out.empty()) {
                events |= xmr::net::kWritable;
            }
            poller.modify(worker.connection.fd(), events);
        };

        auto flush = [&](Worker& worker) {
            while (!worker.out.empty()) {
                std::size_t sent = 0;
                const xmr::net::IoStatus status = xmr::net::sendFrom(
                    worker.connection.fd(), worker.out.data(), worker.out.size(), sent);
                if (status == xmr::net::IoStatus::Ok) {
                    worker.out.consume(sent);
                } else if (status == xmr::net::IoStatus::WouldBlock) {
                    break;
                } else {
                    throw std::runtime_error("worker disconnected while sending");
                }
            }
            updateInterest(worker);
        };

        auto sendTo = [&](Worker& worker, xmr::protocol::MessageType type, std::uint32_t requestId,
                          const std::vector<std::uint8_t>& body, std::uint16_t flags = 0) {
            const auto frame = xmr::protocol::makeFrame(type, requestId, body, flags);
            worker.out.append(frame);
            flush(worker);
        };

        // master收worker发过来的消息
        auto handleFrame = [&](Worker& worker, const xmr::protocol::Frame& frame) {
            // 源文本里面第一个字段是命令名
            switch (frame.header.type) {
                case xmr::protocol::MessageType::Hello: {
                    // worker启动的时候给master发一下
                    worker.id = xmr::protocol::Hello::decode(frame.body).workerId;
                    registry.add(worker.id, std::chrono::steady_clock::now());
                    byId[worker.id] = &worker;
                    break;
                }
                case xmr::protocol::MessageType::RequestTask: {
                    // worker告诉master它空闲了 希望master给它派任务
                    registry.markIdle(worker.id);
                    worker.lastRequest = frame.header.requestId;
                    break;
                }
                case xmr::protocol::MessageType::Done: {
                    const auto done = xmr::protocol::Done::decode(frame.body);
                    const auto held = registry.taskOf(worker.id);
                    if (held && held->kind == xmr::taskKind(done.kind) && held->id == done.taskId) {
                        scheduler.markDone(held->kind, held->id, held->attempt);
                    }
                    registry.complete(worker.id);
                    break;
                }
                case xmr::protocol::MessageType::Fail: {
                    const auto fail = xmr::protocol::Fail::decode(frame.body);
                    const auto held = registry.taskOf(worker.id);
                    if (held && held->kind == xmr::taskKind(fail.kind) && held->id == fail.taskId) {
                        scheduler.markFailed(held->kind, held->id, fail.reason);
                    }
                    registry.complete(worker.id);
                    break;
                }
                case xmr::protocol::MessageType::MapOutput: {
                    // worker告诉master它完成了map任务 并把中间结果发送过来了
                    const auto output = xmr::protocol::MapOutput::decode(frame.body);
                    if (output.mapTask >= mapOutput.size() || output.partition >= reducers) {
                        throw std::runtime_error("MAPOUT out of range");
                    }
                    const auto held = registry.taskOf(worker.id);
                    if (!held || held->kind != xmr::TaskKind::Map || held->id != output.mapTask) {
                        break;
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
                    sendTo(worker, xmr::protocol::MessageType::Data, frame.header.requestId, data.encode());
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
                    sendTo(worker, xmr::protocol::MessageType::Data, frame.header.requestId, data.encode());
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
                case xmr::protocol::MessageType::Ping: {
                    // worker周期心跳 master回Pong
                    const auto ping = xmr::protocol::Ping::decode(frame.body);
                    xmr::protocol::Pong pong;
                    pong.nonce = ping.nonce;
                    sendTo(worker, xmr::protocol::MessageType::Pong, frame.header.requestId, pong.encode());
                    break;
                }
                default:
                    throw std::runtime_error("unexpected control message");
            }
        };

        auto processInput = [&](Worker& worker) {
            xmr::protocol::FrameDecoder decoder(worker.in);
            while (auto frame = decoder.next()) {
                handleFrame(worker, *frame);
            }
        };

        // 重发任务 次数用尽才判整个job失败
        auto retryOrFail = [&](const xmr::Task& task) {
            if (!scheduler.retry(task)) {
                scheduler.markFailed(task.kind, task.id, "attempts exhausted");
            }
        };

        // 回收worker持有的任务 重发
        auto requeueTask = [&](const std::string& id) {
            const auto task = registry.reclaim(id);
            if (task) {
                retryOrFail(*task);
            }
        };

        // 断开/失联的worker从master清理掉 任务回收重发
        auto dropWorker = [&](Worker& worker) {
            if (!worker.id.empty()) {
                requeueTask(worker.id);
                registry.remove(worker.id);
                byId.erase(worker.id);
            }
            poller.remove(worker.connection.fd());
            byFd.erase(worker.connection.fd());
            workers.erase(std::remove_if(workers.begin(), workers.end(),
                                         [&](const std::unique_ptr<Worker>& w) {
                                             return w.get() == &worker;
                                         }),
                          workers.end());
        };

        auto readWorker = [&](Worker& worker) {
            bool peerClosed = false;
            while (true) {
                const xmr::net::IoStatus status = xmr::net::recvInto(worker.connection.fd(), worker.in);
                if (status == xmr::net::IoStatus::Ok) {
                    continue;
                }
                if (status == xmr::net::IoStatus::WouldBlock) {
                    break;
                }
                peerClosed = true;
                break;
            }
            if (peerClosed) {
                dropWorker(worker);
                return;
            }
            processInput(worker);
            if (registry.contains(worker.id)) {
                registry.touch(worker.id, std::chrono::steady_clock::now());
            }
        };

        auto acceptWorkers = [&] {
            while (true) {
                xmr::net::Connection connection;
                const xmr::net::IoStatus status = listener.acceptNonBlocking(connection);
                if (status == xmr::net::IoStatus::WouldBlock) {
                    break;
                }
                if (status != xmr::net::IoStatus::Ok) {
                    throw std::runtime_error("accept failed");
                }
                xmr::net::setNonBlocking(connection.fd());
                auto worker = std::make_unique<Worker>();
                worker->connection = std::move(connection);
                byFd[worker->connection.fd()] = worker.get();
                workers.push_back(std::move(worker));
                poller.add(workers.back()->connection.fd(), xmr::net::kReadable);
            }
        };

        auto dispatch = [&] {
            if (workers.size() < expectedWorkers) {
                return;
            }
            while (registry.hasIdle()) {
                auto task = scheduler.takeTask();
                if (!task) {
                    break;
                }
                const auto id = registry.assignNext(*task, std::chrono::steady_clock::now());
                if (!id) {
                    break;
                }
                Worker* worker = byId[*id];
                // master向worker派发任务
                const auto message = xmr::toTaskMessage(*task);
                sendTo(*worker, xmr::protocol::MessageType::Task, worker->lastRequest, message.encode());
            }
        };

        while (!scheduler.finished() && !scheduler.failed()) {
            const auto now = std::chrono::steady_clock::now();
            for (const auto& event : poller.wait(registry.nextTimeoutMs(now))) {
                if (event.fd == listener.fd()) {
                    acceptWorkers();
                    continue;
                }
                const auto it = byFd.find(event.fd);
                if (it == byFd.end()) {
                    continue;
                }
                if (event.events & xmr::net::kWritable) {
                    flush(*it->second);
                }
                if (event.events & (xmr::net::kReadable | xmr::net::kBroken)) {
                    readWorker(*it->second);
                }
            }
            const auto expired = registry.poll(std::chrono::steady_clock::now());
            for (const auto& id : expired.workers) {
                requeueTask(id);
            }
            for (const auto& task : expired.tasks) {
                retryOrFail(task);
            }
            dispatch();
        }

        for (auto& worker : workers) {
            try {
                const auto frame = xmr::protocol::makeFrame(xmr::protocol::MessageType::Stop, 0,
                                                            xmr::protocol::Stop{}.encode());
                worker->out.append(frame);
                xmr::net::setBlocking(worker->connection.fd());
                while (!worker->out.empty()) {
                    const std::size_t pending = worker->out.size();
                    xmr::net::sendAll(worker->connection.fd(), worker->out.data(), pending);
                    worker->out.consume(pending);
                }
                // Half-close, then drain any in-flight REQUEST so closing the
                // socket does not reset the connection underneath the worker.
                ::shutdown(worker->connection.fd(),SHUT_WR);
                char buffer[256];
                while (::read(worker->connection.fd(), buffer, sizeof(buffer)) > 0) {
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
        std::cerr << "master: " << error.what() << '\n';
        usage(argv[0]);
        return static_cast<int>(ExitCode::Usage);
    } catch (const std::exception& error) {
        std::cerr << "master: " << error.what() << '\n';
        return static_cast<int>(ExitCode::Failure);
    }

    return static_cast<int>(ExitCode::Success);
}
