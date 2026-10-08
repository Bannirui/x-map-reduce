#include"net/event_loop.h"
#include"net/net.h"
#include"net/timer.h"
#include"protocol/framing.h"
#include"protocol/messages.h"
#include"runtime/exit_code.h"
#include"runtime/plugin.h"
#include"runtime/task.h"
#include"runtime/task_codec.h"

#include<chrono>
#include<cstdint>
#include<iostream>
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

    enum class Stage {
        RequestTask,
        WaitingTask,
        WaitingInput,
        WaitingFetch,
    };

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

        std::uint32_t requestId = 0;
        Stage stage = Stage::RequestTask;
        std::optional<xmr::Task> current;
        std::vector<std::vector<KeyValue> > fetched;
        std::unordered_map<std::uint32_t, std::size_t> fetchIndex;
        std::size_t outstanding = 0;
        bool stopped = false;

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
                             const std::vector<std::uint8_t>& body) {
            out.append(xmr::protocol::makeFrame(type, rid, body));
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

        auto finishMap = [&](const xmr::Task& task, const std::string& content) {
            // map产出的中间结果 已经按照R分区好了 现在还放在worker的内存上 等着shuffle
            const auto parts = xmr::runMapTask(task, content);
            for (std::size_t r = 0; r < parts.size(); ++r) {
                // todo 论文里面master只负责管理元数据 业务数据是不管的 我们的架构里面先让master负责shuffle 把所有的中间结果网络发给master
                xmr::protocol::MapOutput output;
                output.mapTask = task.id;
                output.partition = r;
                output.offset = 0;
                output.payload = toBytes(xmr::serializeKeyValues(parts[r]));
                sendFrame(xmr::protocol::MessageType::MapOutput, 0, output.encode());
            }
        };

        auto finishReduce = [&](const xmr::Task& task) {
            auto fetch = [&](std::size_t mapTask, std::size_t) {
                return std::move(fetched[mapTask]);
            };
            const auto result = xmr::runReduceTask(task, fetch);
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
                const std::string payload = toString(xmr::protocol::DataMessage::decode(frame.body).payload);
                if (stage == Stage::WaitingInput) {
                    // worker收到master给的map任务数据
                    try {
                        finishMap(*current, payload);
                        xmr::protocol::Done done;
                        done.kind = xmr::protocol::WorkKind::Map;
                        done.taskId = current->id;
                        sendFrame(xmr::protocol::MessageType::Done, 0, done.encode());
                    } catch (const std::exception& error) {
                        failTask(*current, error.what());
                    }
                    current.reset();
                    stage = Stage::RequestTask;
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
                        try {
                            finishReduce(*current);
                            xmr::protocol::Done done;
                            done.kind = xmr::protocol::WorkKind::Reduce;
                            done.taskId = current->id;
                            sendFrame(xmr::protocol::MessageType::Done, 0, done.encode());
                        } catch (const std::exception& error) {
                            failTask(*current, error.what());
                        }
                        current.reset();
                        stage = Stage::RequestTask;
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
