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
        WaitingFetch,
        Computing,
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

        xmr::net::Poller poller;
        poller.add(connection.fd(), xmr::net::kReadable);
        xmr::net::ByteBuffer in;
        xmr::net::ByteBuffer out;

        // 业务计算跑在线程池里 算完通过notifier把回调投递回reactor线程执行
        xmr::net::Notifier notifier;
        poller.add(notifier.fd(), xmr::net::kReadable);
        std::mutex completionMutex;
        std::vector<std::function<void()> > completions;
        xmr::net::ThreadPool pool(kComputeThreads);

        std::uint32_t requestId = 0;
        Stage stage = Stage::RequestTask;
        std::optional<xmr::Task> current;
        std::vector<std::vector<KeyValue> > fetched;
        std::unordered_map<std::uint32_t, std::size_t> fetchIndex;
        // 分块DATA的接收缓冲 按requestId聚合
        std::unordered_map<std::uint32_t, std::string> incoming;
        std::size_t outstanding = 0;
        bool stopped = false;

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

        auto sendMapOutput = [&](const xmr::Task& task, const std::vector<std::vector<KeyValue> >& parts) {
            // map产出的中间结果 已经按照R分区好了 现在还放在worker的内存上 等着shuffle
            for (std::size_t r = 0; r < parts.size(); ++r) {
                // todo 论文里面master只负责管理元数据 业务数据是不管的 我们的架构里面先让master负责shuffle 把所有的中间结果网络发给master
                const std::string blob = xmr::serializeKeyValues(parts[r]);
                std::size_t offset = 0;
                do {
                    const std::size_t n = std::min<std::size_t>(xmr::protocol::kChunkBytes, blob.size() - offset);
                    xmr::protocol::MapOutput output;
                    output.mapTask = task.id;
                    output.partition = r;
                    output.offset = offset;
                    output.payload.assign(blob.begin() + static_cast<std::ptrdiff_t>(offset),
                                          blob.begin() + static_cast<std::ptrdiff_t>(offset + n));
                    const bool more = offset + n < blob.size();
                    sendFrame(xmr::protocol::MessageType::MapOutput, 0, output.encode(),
                              more ? static_cast<std::uint16_t>(xmr::protocol::Flag::More) : 0);
                    offset += n;
                } while (offset < blob.size());
            }
        };

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

        auto handleFrame = [&](const xmr::protocol::Frame& frame) {
            if (frame.header.type == xmr::protocol::MessageType::Stop) {
                // master告诉worker任务结束了 可以关停了
                stopped = true;
                return;
            }
            if (frame.header.type == xmr::protocol::MessageType::Pong) {
                // master对心跳的应答 暂时不需要处理
                return;
            }
            if (frame.header.type == xmr::protocol::MessageType::Task) {
                // worker收到master派发的任务
                current = xmr::toTask(xmr::protocol::TaskMessage::decode(frame.body));
                if (current->kind == xmr::TaskKind::Map) {
                    // worker收到master派发的map任务 跟master要这个map任务的数据
                    xmr::protocol::InputRequest request;
                    request.taskId = current->id;
                    sendFrame(xmr::protocol::MessageType::InputRequest, ++requestId, request.encode());
                    stage = Stage::WaitingInput;
                } else {
                    // worker收到master派发的reduce任务
                    fetched.assign(current->maps, std::vector<KeyValue>{});
                    fetchIndex.clear();
                    outstanding = current->maps;
                    for (std::size_t mapTask = 0; mapTask < current->maps; ++mapTask) {
                        xmr::protocol::Fetch request;
                        request.mapTask = mapTask;
                        request.partition = current->id;
                        request.offset = 0;
                        const std::uint32_t rid = ++requestId;
                        fetchIndex[rid] = mapTask;
                        sendFrame(xmr::protocol::MessageType::Fetch, rid, request.encode());
                    }
                    stage = Stage::WaitingFetch;
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
                const std::string payload = std::move(buffer);
                incoming.erase(frame.header.requestId);
                if (stage == Stage::WaitingInput) {
                    // worker收到master给的map任务数据 计算丢到线程池 算完再回reactor发结果
                    const xmr::Task task = *current;
                    current.reset();
                    stage = Stage::Computing;
                    pool.submit([&, task, content = payload] {
                        std::vector<std::vector<KeyValue> > parts;
                        try {
                            parts = xmr::runMapTask(task, content);
                        } catch (const std::exception& error) {
                            const std::string reason = error.what();
                            post([&, task, reason] {
                                failTask(task, reason);
                                stage = Stage::RequestTask;
                            });
                            return;
                        }
                        post([&, task, parts = std::move(parts)]() mutable {
                            sendMapOutput(task, parts);
                            sendDone(task);
                            stage = Stage::RequestTask;
                        });
                    });
                    return;
                }
                if (stage == Stage::WaitingFetch) {
                    const auto it = fetchIndex.find(frame.header.requestId);
                    if (it == fetchIndex.end()) {
                        throw std::runtime_error("unexpected DATA reply");
                    }
                    fetched[it->second] = xmr::deserializeKeyValues(payload);
                    fetchIndex.erase(it);
                    if (--outstanding == 0) {
                        const xmr::Task task = *current;
                        current.reset();
                        stage = Stage::Computing;
                        std::vector<std::vector<KeyValue> > data = std::move(fetched);
                        pool.submit([&, task, data = std::move(data)] {
                            std::vector<KeyValue> result;
                            try {
                                auto fetch = [&data](std::size_t mapTask, std::size_t) {
                                    return std::move(data[mapTask]);
                                };
                                result = xmr::runReduceTask(task, fetch);
                            } catch (const std::exception& error) {
                                const std::string reason = error.what();
                                post([&, task, reason] {
                                    failTask(task, reason);
                                    stage = Stage::RequestTask;
                                });
                                return;
                            }
                            post([&, task, result = std::move(result)]() mutable {
                                sendResult(task, result);
                                sendDone(task);
                                stage = Stage::RequestTask;
                            });
                        });
                    }
                    return;
                }
                throw std::runtime_error("unexpected DATA reply");
            }
            throw std::runtime_error("unexpected master message");
        };

        // 发个探测协议
        xmr::protocol::Hello hello;
        hello.workerId = "worker-" + std::to_string(::getpid());
        hello.pid = static_cast<std::uint64_t>(::getpid());
        out.append(xmr::protocol::makeFrame(xmr::protocol::MessageType::Hello, 0, hello.encode()));
        flush();

        while (!stopped) {
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
                    if (peerClosed && !stopped) {
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
