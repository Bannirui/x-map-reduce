#include"net/event_loop.h"
#include"net/net.h"
#include"net/notifier.h"
#include"net/thread_pool.h"
#include"net/timer.h"
#include"protocol/framing.h"
#include"protocol/messages.h"
#include"runtime/exit_code.h"
#include"runtime/plugin.h"
#include"runtime/task.h"
#include"runtime/task_codec.h"

#include<algorithm>
#include<chrono>
#include<cstdint>
#include<functional>
#include<iostream>
#include<mutex>
#include<optional>
#include<stdexcept>
#include<string>
#include<unordered_map>
#include<utility>
#include<vector>

#include<unistd.h>

namespace {
    // worker上报心跳的周期
    constexpr std::chrono::seconds kHeartbeatInterval{2};
    // 业务计算的线程池大小
    constexpr std::size_t kComputeThreads{4};

    enum class Stage {
        RequestTask,
        WaitingTask,
        WaitingInput,
        Computing,
    };

    // reduce要去哪个worker拉哪个map任务的分区
    struct Location {
        std::size_t mapTask = 0;
        std::string host;
        std::uint16_t port = 0;
    };

    std::vector<std::uint8_t> toBytes(const std::string& text) {
        return std::vector<std::uint8_t>(text.begin(), text.end());
    }

    std::pair<std::string, std::string> parseEndpoint(const std::string& endpoint) {
        const auto colon = endpoint.rfind(':');
        if (colon == std::string::npos) {
            throw std::runtime_error("expected host:port in '" + endpoint + "'");
        }
        return {endpoint.substr(0, colon), endpoint.substr(colon + 1)};
    }

    void usage(const char* program) {
        std::cerr << "Usage: " << program << " --master <host:port> [--plugin <path>]...\n";
    }

    // 从另一个worker拉某个map任务的分区结果 阻塞调用 只在线程池里跑
    std::vector<KeyValue> pullPartition(const std::string& host, std::uint16_t port,
                                        std::size_t mapTask, std::size_t partition) {
        xmr::net::Connection connection = xmr::net::connectTo(host, port);
        xmr::protocol::Pull pull;
        pull.mapTask = mapTask;
        pull.partition = partition;
        const auto request = xmr::protocol::makeFrame(xmr::protocol::MessageType::Pull, 1, pull.encode());
        xmr::net::sendAll(connection.fd(), request.data(), request.size());

        xmr::net::ByteBuffer in;
        std::string blob;
        while (true) {
            if (xmr::net::recvInto(connection.fd(), in) == xmr::net::IoStatus::Closed) {
                throw std::runtime_error("pull peer closed");
            }
            xmr::protocol::FrameDecoder decoder(in);
            while (auto frame = decoder.next()) {
                if (frame->header.type != xmr::protocol::MessageType::Data) {
                    continue;
                }
                const auto data = xmr::protocol::DataMessage::decode(frame->body);
                blob.append(data.payload.begin(), data.payload.end());
                if ((frame->header.flags & static_cast<std::uint16_t>(xmr::protocol::Flag::More)) == 0) {
                    return xmr::deserializeKeyValues(blob);
                }
            }
        }
    }

    // 响应另一个worker的拉取请求 阻塞调用 只在线程池里跑
    void servePull(xmr::net::Connection connection,
                   std::unordered_map<std::size_t, std::vector<std::string> >& store,
                   std::mutex& storeMutex) {
        xmr::net::ByteBuffer in;
        while (true) {
            if (xmr::net::recvInto(connection.fd(), in) == xmr::net::IoStatus::Closed) {
                return;
            }
            xmr::protocol::FrameDecoder decoder(in);
            while (auto frame = decoder.next()) {
                if (frame->header.type != xmr::protocol::MessageType::Pull) {
                    continue;
                }
                const auto request = xmr::protocol::Pull::decode(frame->body);
                std::string blob;
                {
                    std::lock_guard<std::mutex> lock(storeMutex);
                    const auto it = store.find(request.mapTask);
                    if (it != store.end() && request.partition < it->second.size()) {
                        blob = it->second[request.partition];
                    }
                }
                std::size_t offset = 0;
                do {
                    const std::size_t n = std::min<std::size_t>(xmr::protocol::kChunkBytes, blob.size() - offset);
                    xmr::protocol::DataMessage data;
                    data.offset = offset;
                    data.total = blob.size();
                    data.payload.assign(blob.begin() + static_cast<std::ptrdiff_t>(offset),
                                        blob.begin() + static_cast<std::ptrdiff_t>(offset + n));
                    const bool more = offset + n < blob.size();
                    const auto out = xmr::protocol::makeFrame(
                        xmr::protocol::MessageType::Data, frame->header.requestId, data.encode(),
                        more ? static_cast<std::uint16_t>(xmr::protocol::Flag::More) : 0);
                    xmr::net::sendAll(connection.fd(), out.data(), out.size());
                    offset += n;
                } while (offset < blob.size());
                return;
            }
        }
    }

    // "mapTask,host,port" -> Location
    Location parseLocation(const std::string& text) {
        Location location;
        const auto first = text.find(',');
        const auto second = text.find(',', first == std::string::npos ? 0 : first + 1);
        if (first == std::string::npos || second == std::string::npos) {
            throw std::runtime_error("bad location '" + text + "'");
        }
        location.mapTask = static_cast<std::size_t>(std::stoull(text.substr(0, first)));
        location.host = text.substr(first + 1, second - first - 1);
        location.port = static_cast<std::uint16_t>(std::stoul(text.substr(second + 1)));
        return location;
    }
} // namespace

int main(int argc, char** argv) {
    // master的host:port
    std::string master;
    // job的插件
    std::vector<std::string> plugins;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--master" && i + 1 < argc) {
            master = argv[++i];
        } else if (arg == "--plugin" && i + 1 < argc) {
            plugins.emplace_back(argv[++i]);
        }
    }

    try {
        if (master.empty()) {
            throw UsageError("missing --master");
        }
        // 加载job插件
        for (const auto& plugin : plugins) {
            loadJobPlugin(plugin);
        }
        const auto [host,portText] = parseEndpoint(master);
        const std::uint16_t port = static_cast<std::uint16_t>(std::stoul(portText));
        // TCP连接
        xmr::net::Connection connection = xmr::net::connectTo(host, port);
        xmr::net::setNonBlocking(connection.fd());

        // 数据面监听 给别的worker来拉中间结果
        xmr::net::Listener dataListener("0.0.0.0", 0);
        xmr::net::setNonBlocking(dataListener.fd());

        xmr::net::Poller poller;
        poller.add(connection.fd(), xmr::net::kReadable);
        poller.add(dataListener.fd(), xmr::net::kReadable);
        xmr::net::ByteBuffer in;
        xmr::net::ByteBuffer out;

        // 业务计算跑在线程池里 算完通过notifier把回调投递回reactor线程执行
        xmr::net::Notifier notifier;
        poller.add(notifier.fd(), xmr::net::kReadable);
        std::mutex completionMutex;
        std::vector<std::function<void()> > completions;
        // 本worker产出的中间结果 mapTask -> 每个分区的序列化blob 供别的worker拉
        std::unordered_map<std::size_t, std::vector<std::string> > mapStore;
        std::mutex mapStoreMutex;
        xmr::net::ThreadPool pool(kComputeThreads);

        std::uint32_t requestId = 0;
        Stage stage = Stage::RequestTask;
        std::optional<xmr::Task> current;
        std::unordered_map<std::uint32_t, std::string> incoming;
        bool running = true;
        // job代号 换job后自增 用来丢弃上一个job迟到的计算结果
        std::uint64_t epoch = 0;

        // 复位本次job的状态 准备接下一个job
        auto resetJob = [&] {
            ++epoch;
            current.reset();
            incoming.clear();
            {
                std::lock_guard<std::mutex> lock(mapStoreMutex);
                mapStore.clear();
            }
            stage = Stage::RequestTask;
        };

        // 任意线程都可以投递 只有reactor线程会执行
        auto post = [&](std::function<void()> completion) {
            {
                std::lock_guard<std::mutex> lock(completionMutex);
                completions.push_back(std::move(completion));
            }
            notifier.notify();
        };

        auto runCompletions = [&] {
            std::vector<std::function<void()> > ready;
            {
                std::lock_guard<std::mutex> lock(completionMutex);
                ready.swap(completions);
            }
            for (auto& completion : ready) {
                completion();
            }
        };

        auto updateInterest = [&] {
            std::uint32_t events = xmr::net::kReadable;
            if (!out.empty()) {
                events |= xmr::net::kWritable;
            }
            poller.modify(connection.fd(), events);
        };

        auto flush = [&] {
            while (!out.empty()) {
                std::size_t sent = 0;
                const xmr::net::IoStatus status = xmr::net::sendFrom(
                    connection.fd(), out.data(), out.size(), sent);
                if (status == xmr::net::IoStatus::Ok) {
                    out.consume(sent);
                } else if (status == xmr::net::IoStatus::WouldBlock) {
                    break;
                } else {
                    throw std::runtime_error("master disconnected while sending");
                }
            }
            updateInterest();
        };

        auto sendFrame = [&](xmr::protocol::MessageType type, std::uint32_t rid,
                             const std::vector<std::uint8_t>& body, std::uint16_t flags = 0) {
            out.append(xmr::protocol::makeFrame(type, rid, body, flags));
            flush();
        };

        // 周期给master上报心跳
        xmr::net::TimerQueue timers;
        std::uint64_t heartbeatNonce = 0;
        timers.addInterval(kHeartbeatInterval, [&] {
            xmr::protocol::Ping ping;
            ping.nonce = ++heartbeatNonce;
            sendFrame(xmr::protocol::MessageType::Ping, 0, ping.encode());
        });

        auto sendResult = [&](const xmr::Task& task, const std::vector<KeyValue>& result) {
            xmr::protocol::ResultMessage message;
            message.reduceTask = task.id;
            message.offset = 0;
            message.payload = toBytes(xmr::serializeKeyValues(result));
            sendFrame(xmr::protocol::MessageType::Result, 0, message.encode());
        };

        auto failTask = [&](const xmr::Task& task, const std::string& reason) {
            xmr::protocol::Fail fail;
            fail.kind = xmr::wireKind(task.kind);
            fail.taskId = task.id;
            fail.statusCode = xmr::protocol::StatusCode::Internal;
            fail.reason = reason;
            sendFrame(xmr::protocol::MessageType::Fail, 0, fail.encode());
        };

        auto sendDone = [&](const xmr::Task& task) {
            xmr::protocol::Done done;
            done.kind = xmr::wireKind(task.kind);
            done.taskId = task.id;
            sendFrame(xmr::protocol::MessageType::Done, 0, done.encode());
        };

        // 收到map任务输入后 在线程池里跑map 结果留在本地并上报Done
        auto startMap = [&](const xmr::Task& task, std::string content) {
            const std::uint64_t job = epoch;
            pool.submit([&, task, content = std::move(content), job] {
                std::vector<std::vector<KeyValue> > parts;
                try {
                    parts = xmr::runMapTask(task, content);
                } catch (const std::exception& error) {
                    const std::string reason = error.what();
                    post([&, task, reason, job] {
                        if (job != epoch) {
                            return;
                        }
                        failTask(task, reason);
                        stage = Stage::RequestTask;
                    });
                    return;
                }
                post([&, task, parts = std::move(parts), job]() mutable {
                    if (job != epoch) {
                        return;
                    }
                    // 中间结果留在本地 等reduce的worker来拉
                    {
                        std::lock_guard<std::mutex> lock(mapStoreMutex);
                        std::vector<std::string>& stored = mapStore[task.id];
                        stored.clear();
                        stored.reserve(parts.size());
                        for (auto& part : parts) {
                            stored.push_back(xmr::serializeKeyValues(part));
                        }
                    }
                    sendDone(task);
                    stage = Stage::RequestTask;
                });
            });
        };

        // 收到reduce任务 在线程池里直接从各个map worker拉数据再reduce
        auto startReduce = [&](const xmr::Task& task, std::vector<Location> locations) {
            const std::uint64_t job = epoch;
            pool.submit([&, task, locations = std::move(locations), job] {
                std::vector<KeyValue> result;
                try {
                    std::vector<std::vector<KeyValue> > fetched(task.maps);
                    for (const auto& location : locations) {
                        fetched[location.mapTask] = pullPartition(location.host, location.port,
                                                                  location.mapTask, task.id);
                    }
                    auto fetch = [&fetched](std::size_t mapTask, std::size_t) {
                        return std::move(fetched[mapTask]);
                    };
                    result = xmr::runReduceTask(task, fetch);
                } catch (const std::exception& error) {
                    const std::string reason = error.what();
                    post([&, task, reason, job] {
                        if (job != epoch) {
                            return;
                        }
                        failTask(task, reason);
                        stage = Stage::RequestTask;
                    });
                    return;
                }
                post([&, task, result = std::move(result), job]() mutable {
                    if (job != epoch) {
                        return;
                    }
                    sendResult(task, result);
                    sendDone(task);
                    stage = Stage::RequestTask;
                });
            });
        };

        auto handleFrame = [&](const xmr::protocol::Frame& frame) {
            if (frame.header.type == xmr::protocol::MessageType::Stop) {
                // master告诉worker本job结束了 复位状态准备接下一个job
                resetJob();
                return;
            }
            if (frame.header.type == xmr::protocol::MessageType::Shutdown) {
                // master要关停了 worker退出循环
                running = false;
                return;
            }
            if (frame.header.type == xmr::protocol::MessageType::Pong) {
                // master对心跳的应答 暂时不需要处理
                return;
            }
            if (frame.header.type == xmr::protocol::MessageType::Task) {
                // worker收到master派发的任务
                const auto message = xmr::protocol::TaskMessage::decode(frame.body);
                current = xmr::toTask(message);
                if (current->kind == xmr::TaskKind::Map) {
                    // worker收到master派发的map任务 跟master要这个map任务的数据
                    xmr::protocol::InputRequest request;
                    request.taskId = current->id;
                    sendFrame(xmr::protocol::MessageType::InputRequest, ++requestId, request.encode());
                    stage = Stage::WaitingInput;
                } else {
                    // worker收到master派发的reduce任务 附带"去哪些worker拉"的计划
                    std::vector<Location> locations;
                    locations.reserve(message.locations.size());
                    for (const auto& text : message.locations) {
                        locations.push_back(parseLocation(text));
                    }
                    const xmr::Task task = *current;
                    current.reset();
                    stage = Stage::Computing;
                    startReduce(task, std::move(locations));
                }
                return;
            }
            if (frame.header.type == xmr::protocol::MessageType::Data) {
                const auto data = xmr::protocol::DataMessage::decode(frame.body);
                std::string& buffer = incoming[frame.header.requestId];
                if (data.offset != buffer.size()) {
                    throw std::runtime_error("out of order DATA chunk");
                }
                buffer.append(data.payload.begin(), data.payload.end());
                if ((frame.header.flags & static_cast<std::uint16_t>(xmr::protocol::Flag::More)) != 0) {
                    return;
                }
                std::string payload = std::move(buffer);
                incoming.erase(frame.header.requestId);
                if (stage != Stage::WaitingInput) {
                    throw std::runtime_error("unexpected DATA reply");
                }
                const xmr::Task task = *current;
                current.reset();
                stage = Stage::Computing;
                startMap(task, std::move(payload));
                return;
            }
            throw std::runtime_error("unexpected master message");
        };

        // 发个探测协议
        xmr::protocol::Hello hello;
        hello.workerId = "worker-" + std::to_string(::getpid());
        hello.pid = static_cast<std::uint64_t>(::getpid());
        out.append(xmr::protocol::makeFrame(xmr::protocol::MessageType::Hello, 0, hello.encode()));
        flush();

        // 上报数据面监听端口
        xmr::protocol::DataAddress dataAddress;
        dataAddress.port = dataListener.port();
        sendFrame(xmr::protocol::MessageType::DataAddress, 0, dataAddress.encode());

        while (running) {
            if (stage == Stage::RequestTask) {
                // worker告诉master我空闲了 给我个任务
                sendFrame(xmr::protocol::MessageType::RequestTask, ++requestId, {});
                stage = Stage::WaitingTask;
            }

            for (const auto& event : poller.wait(timers.timeoutMs())) {
                if (event.fd == notifier.fd()) {
                    notifier.drain();
                    runCompletions();
                    continue;
                }
                if (event.fd == dataListener.fd()) {
                    while (true) {
                        xmr::net::Connection incomingConn;
                        const xmr::net::IoStatus status = dataListener.acceptNonBlocking(incomingConn);
                        if (status == xmr::net::IoStatus::WouldBlock) {
                            break;
                        }
                        if (status != xmr::net::IoStatus::Ok) {
                            throw std::runtime_error("data accept failed");
                        }
                        auto conn = std::make_shared<xmr::net::Connection>(std::move(incomingConn));
                        pool.submit([&, conn] {
                            servePull(std::move(*conn), mapStore, mapStoreMutex);
                        });
                    }
                    continue;
                }
                if (event.events & xmr::net::kWritable) {
                    flush();
                }
                if (event.events & (xmr::net::kReadable | xmr::net::kBroken)) {
                    bool peerClosed = false;
                    while (true) {
                        const xmr::net::IoStatus status = xmr::net::recvInto(connection.fd(), in);
                        if (status == xmr::net::IoStatus::Ok) {
                            continue;
                        }
                        if (status == xmr::net::IoStatus::WouldBlock) {
                            break;
                        }
                        peerClosed = true;
                        break;
                    }
                    xmr::protocol::FrameDecoder decoder(in);
                    while (auto frame = decoder.next()) {
                        handleFrame(*frame);
                    }
                    if (peerClosed && running) {
                        throw std::runtime_error("master disconnected");
                    }
                }
            }
            timers.fire();
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
