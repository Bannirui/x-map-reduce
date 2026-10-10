#include"net/bootstrap.h"
#include"net/channel.h"
#include"net/channel_handler.h"
#include"net/channel_handler_context.h"
#include"net/channel_pipeline.h"
#include"net/event_loop.h"
#include"net/event_loop_group.h"
#include"net/net.h"
#include"net/thread_pool.h"
#include"protocol/frame_codec.h"
#include"protocol/framing.h"
#include"protocol/messages.h"
#include"runtime/exit_code.h"
#include"runtime/jobs.h"
#include"runtime/plugin.h"
#include"runtime/task.h"
#include"runtime/task_codec.h"

#include<algorithm>
#include<atomic>
#include<chrono>
#include<cstdint>
#include<filesystem>
#include<fstream>
#include<functional>
#include<future>
#include<iostream>
#include<memory>
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
        std::cerr << "Usage: " << program << " --master <host:port> [--plugin <path>]... [--plugin-cache <dir>]\n";
    }

    // 向某个地址发一个请求 流式收Data直到最后一块 返回blob 阻塞调用 只在线程池里跑
    std::string pullStream(const std::string& host, std::uint16_t port,
                           xmr::protocol::MessageType type, const std::vector<std::uint8_t>& body) {
        Connection connection = connectTo(host, port);
        const auto request = xmr::protocol::makeFrame(type, 1, body);
        sendAll(connection.fd(), request.data(), request.size());

        ByteBuffer in;
        std::string blob;
        while (true) {
            if (recvInto(connection.fd(), in) == IoStatus::Closed) {
                return blob;
            }
            xmr::protocol::FrameParser decoder(in);
            while (auto frame = decoder.next()) {
                if (frame->header.type != xmr::protocol::MessageType::Data) {
                    continue;
                }
                const auto data = xmr::protocol::DataMessage::decode(frame->body);
                blob.append(data.payload.begin(), data.payload.end());
                if ((frame->header.flags & static_cast<std::uint16_t>(xmr::protocol::Flag::More)) == 0) {
                    return blob;
                }
            }
        }
    }

    // 从另一个worker拉某个map任务的分区结果
    std::vector<KeyValue> pullPartition(const std::string& host, std::uint16_t port,
                                        std::size_t mapTask, std::size_t partition) {
        xmr::protocol::Pull pull;
        pull.mapTask = mapTask;
        pull.partition = partition;
        return xmr::deserializeKeyValues(pullStream(host, port, xmr::protocol::MessageType::Pull, pull.encode()));
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

    // 把插件字节落盘缓存(原子rename)并dlopen 返回是否成功
    bool installPlugin(const std::string& cacheDir, const std::string& hash,
                       const std::string& job, const std::string& bytes, std::string& reason) {
        try {
            const std::string path = cacheDir + "/" + hash + ".so";
            const std::string tmp = path + ".tmp-" + std::to_string(::getpid());
            {
                std::ofstream out(tmp, std::ios::binary);
                out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
            }
            std::error_code renameError;
            std::filesystem::rename(tmp, path, renameError);
            if (renameError) {
                std::filesystem::remove(tmp, renameError);
            }
            loadJobPlugin(path);
            if (findJob(job) == nullptr) {
                reason = "plugin does not provide job '" + job + "'";
                return false;
            }
            return true;
        } catch (const std::exception& error) {
            reason = error.what();
            return false;
        }
    }

    class ControlHandler : public ChannelInboundHandler {
    public:
        ControlHandler(std::function<void(const xmr::protocol::Frame&)> onFrame,
                       std::function<void()> onInactive)
            : onFrame_(std::move(onFrame)), onInactive_(std::move(onInactive)) {
        }

        void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
            xmr::protocol::Frame* frame = std::any_cast<xmr::protocol::Frame>(&message);
            if (frame != nullptr) {
                onFrame_(*frame);
            }
        }

        void channelInactive(ChannelHandlerContext& ctx) override {
            onInactive_();
        }

    private:
        std::function<void(const xmr::protocol::Frame&)> onFrame_;
        std::function<void()> onInactive_;
    };

    class DataHandler : public ChannelInboundHandler {
    public:
        DataHandler(std::unordered_map<std::size_t, std::vector<std::string> >* store,
                    std::mutex* storeMutex)
            : store_(store), storeMutex_(storeMutex) {
        }

        void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
            xmr::protocol::Frame* frame = std::any_cast<xmr::protocol::Frame>(&message);
            if (frame == nullptr || frame->header.type != xmr::protocol::MessageType::Pull) {
                return;
            }
            const auto request = xmr::protocol::Pull::decode(frame->body);
            std::string blob;
            {
                std::lock_guard<std::mutex> lock(*storeMutex_);
                const auto it = store_->find(request.mapTask);
                if (it != store_->end() && request.partition < it->second.size()) {
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
                xmr::protocol::Frame out;
                out.header.type = xmr::protocol::MessageType::Data;
                out.header.requestId = frame->header.requestId;
                out.header.flags = more ? static_cast<std::uint16_t>(xmr::protocol::Flag::More) : 0;
                out.body = data.encode();
                std::any outgoing = std::move(out);
                ctx.write(outgoing);
                offset += n;
            } while (offset < blob.size());
            ctx.close();
        }

    private:
        std::unordered_map<std::size_t, std::vector<std::string> >* store_;
        std::mutex* storeMutex_;
    };

    class Worker {
    public:
        Worker(std::string masterHost, std::uint16_t masterPort, std::string pluginCache)
            : masterHost_(std::move(masterHost)),
              masterPort_(masterPort),
              pluginCache_(std::move(pluginCache)) {
        }

        int run() {
            controlGroup_.start();
            dataGroup_.start();

            ClientBootstrap client;
            client.group(controlGroup_).handler([this](Channel& channel) {
                channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameEncoder>());
                channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameDecoder>());
                channel.pipeline().addLast(std::make_shared<ControlHandler>(
                    [this](const xmr::protocol::Frame& frame) { onFrame(frame); },
                    [this] { onControlInactive(); }));
            });
            control_ = client.connect(masterHost_, masterPort_);
            loop_ = control_->loop();

            dataServer_.group(dataGroup_).childHandler([this](Channel& channel) {
                channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameEncoder>());
                channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameDecoder>());
                channel.pipeline().addLast(std::make_shared<DataHandler>(&mapStore_, &mapStoreMutex_));
            });
            dataServer_.bind("0.0.0.0", 0);

            // 周期给master上报心跳
            loop_->addInterval(kHeartbeatInterval, [this] { sendPing(); });

            // 发个探测协议
            xmr::protocol::Hello hello;
            hello.workerId = "worker-" + std::to_string(::getpid());
            hello.pid = static_cast<std::uint64_t>(::getpid());
            sendFrame(xmr::protocol::MessageType::Hello, 0, hello.encode());

            // 上报数据面监听端口
            xmr::protocol::DataAddress dataAddress;
            dataAddress.port = dataServer_.port();
            sendFrame(xmr::protocol::MessageType::DataAddress, 0, dataAddress.encode());

            requestTaskIfIdle();

            shutdown_.get_future().wait();

            dataServer_.close();
            control_->close();
            controlGroup_.stop();
            dataGroup_.stop();

            if (error_) {
                std::rethrow_exception(error_);
            }
            return static_cast<int>(ExitCode::Success);
        }

    private:
        void signalShutdown() {
            if (!shutdownDone_.exchange(true)) {
                shutdown_.set_value();
            }
        }

        // 任意线程都可以投递 只有control的reactor线程会执行
        void post(std::function<void()> completion) {
            loop_->runInLoop(std::move(completion));
        }

        void sendFrame(xmr::protocol::MessageType type, std::uint32_t rid,
                       const std::vector<std::uint8_t>& body, std::uint16_t flags = 0) {
            xmr::protocol::Frame frame;
            frame.header.type = type;
            frame.header.flags = flags;
            frame.header.requestId = rid;
            frame.body = body;
            control_->write(std::any(std::move(frame)));
        }

        void requestTaskIfIdle() {
            if (stage_ == Stage::RequestTask) {
                // worker告诉master我空闲了 给我个任务
                sendFrame(xmr::protocol::MessageType::RequestTask, ++requestId_, {});
                stage_ = Stage::WaitingTask;
            }
        }

        void sendPing() {
            xmr::protocol::Ping ping;
            ping.nonce = ++heartbeatNonce_;
            sendFrame(xmr::protocol::MessageType::Ping, 0, ping.encode());
        }

        void sendResult(const xmr::Task& task, const std::vector<KeyValue>& result) {
            xmr::protocol::ResultMessage message;
            message.reduceTask = task.id;
            message.offset = 0;
            message.payload = toBytes(xmr::serializeKeyValues(result));
            sendFrame(xmr::protocol::MessageType::Result, 0, message.encode());
        }

        void failTask(const xmr::Task& task, const std::string& reason) {
            xmr::protocol::Fail fail;
            fail.kind = xmr::wireKind(task.kind);
            fail.taskId = task.id;
            fail.statusCode = xmr::protocol::StatusCode::Internal;
            fail.reason = reason;
            sendFrame(xmr::protocol::MessageType::Fail, 0, fail.encode());
        }

        void sendDone(const xmr::Task& task) {
            xmr::protocol::Done done;
            done.kind = xmr::wireKind(task.kind);
            done.taskId = task.id;
            sendFrame(xmr::protocol::MessageType::Done, 0, done.encode());
        }

        void sendProgress(const xmr::Task& task, double fraction) {
            xmr::protocol::Progress progress;
            progress.kind = xmr::wireKind(task.kind);
            progress.taskId = task.id;
            progress.fraction = static_cast<std::uint64_t>(fraction * 100.0);
            sendFrame(xmr::protocol::MessageType::Progress, 0, progress.encode());
        }

        // 复位本次job的状态 准备接下一个job
        void resetJob() {
            ++epoch_;
            current_.reset();
            computing_.reset();
            cancelled_ = false;
            {
                std::lock_guard<std::mutex> lock(mapStoreMutex_);
                mapStore_.clear();
            }
            stage_ = Stage::RequestTask;
        }

        void onControlInactive() {
            if (!shutdownDone_.load()) {
                error_ = std::make_exception_ptr(std::runtime_error("master disconnected"));
                signalShutdown();
            }
        }

        void onFrame(const xmr::protocol::Frame& frame) {
            try {
                if (frame.header.type == xmr::protocol::MessageType::Stop) {
                    // master告诉worker本job结束了 复位状态准备接下一个job
                    resetJob();
                    requestTaskIfIdle();
                    return;
                }
                if (frame.header.type == xmr::protocol::MessageType::Shutdown) {
                    // master要关停了 worker退出循环
                    signalShutdown();
                    return;
                }
                if (frame.header.type == xmr::protocol::MessageType::Pong) {
                    // master对心跳的应答 暂时不需要处理
                    return;
                }
                if (frame.header.type == xmr::protocol::MessageType::MasterData) {
                    // master数据面端口
                    masterDataPort_ = static_cast<std::uint16_t>(xmr::protocol::MasterData::decode(frame.body).port);
                    return;
                }
                if (frame.header.type == xmr::protocol::MessageType::NeedPlugin) {
                    needPlugin(xmr::protocol::NeedPlugin::decode(frame.body));
                    return;
                }
                if (frame.header.type == xmr::protocol::MessageType::Cancel) {
                    // master取消了当前任务的重复attempt 算完丢弃结果
                    const auto cancel = xmr::protocol::Cancel::decode(frame.body);
                    if (computing_ && computing_->kind == xmr::taskKind(cancel.kind) && computing_->id == cancel.taskId) {
                        cancelled_ = true;
                    }
                    return;
                }
                if (frame.header.type == xmr::protocol::MessageType::Task) {
                    // worker收到master派发的任务
                    const auto message = xmr::protocol::TaskMessage::decode(frame.body);
                    current_ = xmr::toTask(message);
                    if (current_->kind == xmr::TaskKind::Map) {
                        const xmr::Task task = *current_;
                        current_.reset();
                        stage_ = Stage::Computing;
                        startMap(task);
                    } else {
                        // reduce任务附带"去哪些worker拉"的计划
                        std::vector<Location> locations;
                        locations.reserve(message.locations.size());
                        for (const auto& text : message.locations) {
                            locations.push_back(parseLocation(text));
                        }
                        const xmr::Task task = *current_;
                        current_.reset();
                        stage_ = Stage::Computing;
                        startReduce(task, std::move(locations));
                    }
                    return;
                }
                throw std::runtime_error("unexpected master message");
            } catch (...) {
                error_ = std::current_exception();
                signalShutdown();
            }
        }

        // 收到map任务 在线程池里从master数据面拉输入 跑map 结果留在本地并上报Done
        void startMap(const xmr::Task& task) {
            const std::uint64_t job = epoch_;
            const std::string dataHost = masterHost_;
            const std::uint16_t dataPort = masterDataPort_;
            computing_ = task;
            cancelled_ = false;
            pool_.submit([this, task, job, dataHost, dataPort] {
                std::vector<std::vector<KeyValue> > parts;
                try {
                    xmr::protocol::PullInput request;
                    request.taskId = task.id;
                    const std::string content = pullStream(dataHost, dataPort,
                                                           xmr::protocol::MessageType::PullInput, request.encode());
                    // 进度回调节流上报
                    auto last = std::chrono::steady_clock::now();
                    auto reporter = [this, task, job, last](double fraction) mutable {
                        const auto now = std::chrono::steady_clock::now();
                        if (fraction < 1.0 && now - last < std::chrono::milliseconds(500)) {
                            return;
                        }
                        last = now;
                        post([this, task, fraction, job] {
                            if (job != epoch_) {
                                return;
                            }
                            sendProgress(task, fraction);
                        });
                    };
                    parts = xmr::runMapTask(task, content, reporter);
                } catch (const std::exception& error) {
                    const std::string reason = error.what();
                    post([this, task, reason, job] {
                        if (job != epoch_) {
                            return;
                        }
                        failTask(task, reason);
                        stage_ = Stage::RequestTask;
                        requestTaskIfIdle();
                    });
                    return;
                }
                post([this, task, parts = std::move(parts), job]() mutable {
                    if (job != epoch_) {
                        return;
                    }
                    computing_.reset();
                    if (cancelled_) {
                        // 这个attempt落败了 丢弃结果
                        stage_ = Stage::RequestTask;
                        requestTaskIfIdle();
                        return;
                    }
                    // 中间结果留在本地 等reduce的worker来拉
                    {
                        std::lock_guard<std::mutex> lock(mapStoreMutex_);
                        std::vector<std::string>& stored = mapStore_[task.id];
                        stored.clear();
                        stored.reserve(parts.size());
                        for (auto& part : parts) {
                            stored.push_back(xmr::serializeKeyValues(part));
                        }
                    }
                    sendDone(task);
                    stage_ = Stage::RequestTask;
                    requestTaskIfIdle();
                });
            });
        }

        // 收到reduce任务 在线程池里直接从各个map worker拉数据再reduce
        void startReduce(const xmr::Task& task, std::vector<Location> locations) {
            const std::uint64_t job = epoch_;
            computing_ = task;
            cancelled_ = false;
            pool_.submit([this, task, locations = std::move(locations), job] {
                std::vector<KeyValue> result;
                try {
                    std::size_t pulled = 0;
                    std::vector<std::vector<KeyValue> > fetched(task.maps);
                    for (const auto& location : locations) {
                        fetched[location.mapTask] = pullPartition(location.host, location.port,
                                                                  location.mapTask, task.id);
                        ++pulled;
                        const double fraction = task.maps == 0
                            ? 1.0
                            : static_cast<double>(pulled) / static_cast<double>(task.maps);
                        post([this, task, fraction, job] {
                            if (job != epoch_) {
                                return;
                            }
                            sendProgress(task, fraction);
                        });
                    }
                    auto fetch = [&fetched](std::size_t mapTask, std::size_t) {
                        return std::move(fetched[mapTask]);
                    };
                    result = xmr::runReduceTask(task, fetch);
                } catch (const std::exception& error) {
                    const std::string reason = error.what();
                    post([this, task, reason, job] {
                        if (job != epoch_) {
                            return;
                        }
                        failTask(task, reason);
                        stage_ = Stage::RequestTask;
                        requestTaskIfIdle();
                    });
                    return;
                }
                post([this, task, result = std::move(result), job]() mutable {
                    if (job != epoch_) {
                        return;
                    }
                    computing_.reset();
                    if (cancelled_) {
                        // 这个attempt落败了 丢弃结果
                        stage_ = Stage::RequestTask;
                        requestTaskIfIdle();
                        return;
                    }
                    sendResult(task, result);
                    sendDone(task);
                    stage_ = Stage::RequestTask;
                    requestTaskIfIdle();
                });
            });
        }

        // master让去拉插件 线程池里从master数据面拉下来落盘dlopen 再ack
        void needPlugin(const xmr::protocol::NeedPlugin& need) {
            const std::string dataHost = masterHost_;
            const std::uint16_t dataPort = masterDataPort_;
            const std::string cacheDir = pluginCache_;
            pool_.submit([this, need, dataHost, dataPort, cacheDir] {
                bool ok = true;
                std::string reason;
                try {
                    xmr::protocol::PullPlugin request;
                    request.hash = need.hash;
                    const std::string bytes = pullStream(dataHost, dataPort,
                                                         xmr::protocol::MessageType::PullPlugin, request.encode());
                    if (bytes.empty()) {
                        ok = false;
                        reason = "empty plugin";
                    } else {
                        ok = installPlugin(cacheDir, need.hash, need.job, bytes, reason);
                    }
                } catch (const std::exception& error) {
                    ok = false;
                    reason = error.what();
                }
                post([this, need, ok, reason] {
                    xmr::protocol::PluginAck ack;
                    ack.hash = need.hash;
                    ack.ok = ok;
                    ack.reason = reason;
                    sendFrame(xmr::protocol::MessageType::PluginAck, 0, ack.encode());
                });
            });
        }

        std::string masterHost_;
        std::uint16_t masterPort_;
        std::string pluginCache_;

        // 本worker产出的中间结果 mapTask -> 每个分区的序列化blob 供别的worker拉
        std::unordered_map<std::size_t, std::vector<std::string> > mapStore_;
        std::mutex mapStoreMutex_;

        std::uint32_t requestId_ = 0;
        Stage stage_ = Stage::RequestTask;
        std::optional<xmr::Task> current_;
        // 正在计算的任务 以及它是否被master取消(推测执行落败)
        std::optional<xmr::Task> computing_;
        bool cancelled_ = false;
        // job代号 换job后自增 用来丢弃上一个job迟到的计算结果
        std::uint64_t epoch_ = 0;
        // master数据面地址 由master通过MasterData告知
        std::uint16_t masterDataPort_ = 0;
        std::uint64_t heartbeatNonce_ = 0;

        EventLoopGroup controlGroup_{1};
        EventLoopGroup dataGroup_{2};
        std::shared_ptr<Channel> control_;
        EventLoop* loop_ = nullptr;
        ServerBootstrap dataServer_;
        std::promise<void> shutdown_;
        std::atomic<bool> shutdownDone_{false};
        std::exception_ptr error_;

        ThreadPool pool_{kComputeThreads};
    };
} // namespace

int main(int argc, char** argv) {
    // master的host:port
    std::string master;
    // job的插件
    std::vector<std::string> plugins;
    // 运行期收到的插件落盘缓存目录
    std::string pluginCache;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--master" && i + 1 < argc) {
            master = argv[++i];
        } else if (arg == "--plugin" && i + 1 < argc) {
            plugins.emplace_back(argv[++i]);
        } else if (arg == "--plugin-cache" && i + 1 < argc) {
            pluginCache = argv[++i];
        }
    }

    if (pluginCache.empty()) {
        const char* home = ::getenv("HOME");
        pluginCache = std::string(home != nullptr ? home : "/tmp") + "/.cache/xmr/plugins";
    }

    try {
        if (master.empty()) {
            throw UsageError("missing --master");
        }
        // 加载job插件
        for (const auto& plugin : plugins) {
            loadJobPlugin(plugin);
        }
        // 插件缓存目录
        std::error_code cacheError;
        std::filesystem::create_directories(pluginCache, cacheError);
        const auto [host,portText] = parseEndpoint(master);
        const std::uint16_t port = static_cast<std::uint16_t>(std::stoul(portText));

        Worker worker(host, port, pluginCache);
        return worker.run();
    } catch (const UsageError& error) {
        std::cerr << "worker: " << error.what() << '\n';
        usage(argv[0]);
        return static_cast<int>(ExitCode::Usage);
    } catch (const std::exception& error) {
        std::cerr << "worker: " << error.what() << '\n';
        return static_cast<int>(ExitCode::Failure);
    }
}
