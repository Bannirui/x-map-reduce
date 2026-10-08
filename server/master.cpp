#include"net/event_loop.h"
#include"net/event_loop_group.h"
#include"net/net.h"
#include"protocol/framing.h"
#include"protocol/messages.h"
#include"runtime/exit_code.h"
#include"runtime/scheduler.h"
#include"runtime/task.h"
#include"runtime/task_codec.h"
#include"runtime/worker_registry.h"
#include"mapreduce/mapreduce.h"

#include<algorithm>
#include<atomic>
#include<chrono>
#include<cstddef>
#include<cstdint>
#include<exception>
#include<fstream>
#include<future>
#include<iostream>
#include<iterator>
#include<memory>
#include<stdexcept>
#include<string>
#include<thread>
#include<unordered_map>
#include<utility>
#include<vector>

#include<sys/socket.h>
#include<unistd.h>

#include<arpa/inet.h>
#include<netinet/in.h>

namespace {
    // master判定worker心跳超时的阈值
    constexpr std::chrono::seconds kHeartbeatTimeout{6};
    // 单个任务执行超时的阈值 超了就重发
    constexpr std::chrono::seconds kTaskTimeout{30};
    // coordinator检查超时/派发的周期
    constexpr std::chrono::milliseconds kTick{20};
    // worker reactor的线程数
    constexpr std::size_t kIoThreads{4};

    using ConnId = std::uint64_t;

    struct Conn;
    class Coordinator;

    // master和单个连接 由某个worker reactor独占读写
    struct Conn {
        xmr::net::Connection connection;
        xmr::net::ByteBuffer in;
        xmr::net::ByteBuffer out;
        xmr::net::EventLoop* loop = nullptr;
        xmr::net::EventLoop* coordinatorLoop = nullptr;
        Coordinator* owner = nullptr;
        ConnId id = 0;
        // control连接的peer地址 用来给worker拼数据面地址
        std::string host;
        // coordinator线程写 用于回填派发时的请求id
        std::string workerId;
        std::uint32_t lastRequest = 0;
        // worker reactor线程独占
        bool broken = false;
    };

    void updateInterest(Conn& conn);

    void flushConn(Conn& conn);

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

    // 取一个已连接socket的对端IP
    std::string peerHost(int fd) {
        sockaddr_storage address{};
        socklen_t length = sizeof(address);
        if (::getpeername(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
            return {};
        }
        char buffer[INET6_ADDRSTRLEN] = {0};
        if (address.ss_family == AF_INET) {
            ::inet_ntop(AF_INET, &reinterpret_cast<sockaddr_in*>(&address)->sin_addr, buffer, sizeof(buffer));
        } else if (address.ss_family == AF_INET6) {
            ::inet_ntop(AF_INET6, &reinterpret_cast<sockaddr_in6*>(&address)->sin6_addr, buffer, sizeof(buffer));
        }
        return buffer;
    }

    // 一次提交进来的一次job运行的全部状态
    struct JobState {
        JobState(std::string job, std::vector<std::string> inputs, std::size_t reducers,
                 std::vector<std::string> inputData, std::size_t expectedWorkers, std::string output, ConnId client)
            : scheduler(std::move(job), inputs, reducers),
              inputData(std::move(inputData)),
              expectedWorkers(expectedWorkers),
              output(std::move(output)),
              client(client) {
        }

        xmr::Scheduler scheduler;
        std::vector<std::string> inputData;
        std::vector<KeyValue> merged;
        std::size_t expectedWorkers = 1;
        std::string output;
        ConnId client = 0;
    };

    // 业务协调者 独占任务调度和worker资源管理 只在自己的loop线程上跑
    class Coordinator {
    public:
        Coordinator() : registry_(kHeartbeatTimeout, kTaskTimeout) {
        }

        void setLoop(xmr::net::EventLoop* loop) {
            loop_ = loop;
        }

        void setOnShutdown(std::function<void()> onShutdown) {
            onShutdown_ = std::move(onShutdown);
        }

        void start() {
            loop_->addInterval(kTick, [this] { tick(); });
        }

        void onConnect(ConnId id, std::shared_ptr<Conn> conn) {
            conns_[id] = std::move(conn);
        }

        void onFrame(ConnId id, const xmr::protocol::Frame& frame);

        void onDisconnect(ConnId id);

        bool hasError() const {
            return !error_.empty();
        }

        std::string error() const {
            return error_;
        }

        std::unordered_map<ConnId, std::shared_ptr<Conn> >& conns() {
            return conns_;
        }

    private:
        void tick();

        void submitJob(ConnId client, const xmr::protocol::Submit& submit, std::uint32_t requestId);

        void finishJob();

        void dispatch();

        // 拼reduce任务要用的拉取计划 mapTask,host,port
        std::vector<std::string> fetchPlan() const;

        void send(ConnId id, xmr::protocol::MessageType type, std::uint32_t requestId,
                  const std::vector<std::uint8_t>& body, std::uint16_t flags = 0);

        void sendBlob(ConnId id, std::uint32_t requestId, const std::string& blob);

        void requeueTask(const std::string& workerId);

        // worker失效:重发它手里的任务 并作废它名下已完成的map(输出丢了)
        void recoverWorker(const std::string& workerId);

        void retryOrFail(const xmr::Task& task);

        void requestShutdown();

        void fail(std::string reason) {
            error_ = std::move(reason);
            requestShutdown();
        }

        xmr::net::EventLoop* loop_ = nullptr;
        xmr::WorkerRegistry registry_;
        std::unordered_map<ConnId, std::shared_ptr<Conn> > conns_;
        std::unordered_map<std::string, ConnId> byWorker_;
        // worker的数据面地址 host + port
        std::unordered_map<std::string, std::pair<std::string, std::uint16_t> > workerData_;
        // map任务的结果在哪个worker上
        std::unordered_map<std::size_t, std::string> mapOwner_;
        std::unique_ptr<JobState> job_;
        std::atomic<bool> stopping_{false};
        std::string error_;
        std::function<void()> onShutdown_;
    };

    void Coordinator::tick() {
        if (stopping_.load()) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        // worker存活/任务超时始终维护 和有没有job无关
        const auto expired = registry_.poll(now);
        for (const auto& workerId : expired.workers) {
            recoverWorker(workerId);
        }
        for (const auto& task : expired.tasks) {
            retryOrFail(task);
        }
        if (!job_) {
            return;
        }
        dispatch();
        if (job_->scheduler.finished() || job_->scheduler.failed()) {
            finishJob();
        }
    }

    void Coordinator::submitJob(ConnId client, const xmr::protocol::Submit& submit, std::uint32_t requestId) {
        auto reject = [&](xmr::protocol::StatusCode code, const std::string& reason) {
            xmr::protocol::SubmitAck ack;
            ack.statusCode = code;
            ack.reason = reason;
            send(client, xmr::protocol::MessageType::SubmitAck, requestId, ack.encode());
        };

        if (submit.job.empty() || submit.output.empty() || submit.inputs.empty()
            || submit.reducers == 0 || submit.workers == 0) {
            reject(xmr::protocol::StatusCode::InvalidArgument, "invalid submit");
            return;
        }

        // 现在没有文件系统 master读自己能访问的本地路径
        std::vector<std::string> inputData;
        inputData.reserve(submit.inputs.size());
        for (const auto& path : submit.inputs) {
            std::ifstream in(path, std::ios::binary);
            if (!in) {
                reject(xmr::protocol::StatusCode::NotFound, "failed to open input: " + path);
                return;
            }
            inputData.emplace_back(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }

        job_ = std::make_unique<JobState>(submit.job, submit.inputs, submit.reducers,
                                          std::move(inputData), submit.workers, submit.output, client);
        mapOwner_.clear();

        xmr::protocol::SubmitAck ack;
        ack.statusCode = xmr::protocol::StatusCode::Ok;
        send(client, xmr::protocol::MessageType::SubmitAck, requestId, ack.encode());
    }

    void Coordinator::finishJob() {
        const bool ok = !job_->scheduler.failed();
        // 通知worker本job结束 复位准备接下一个job
        xmr::protocol::Stop stop;
        for (const auto& [name, cid] : byWorker_) {
            send(cid, xmr::protocol::MessageType::Stop, 0, stop.encode());
        }

        xmr::protocol::SubmitResult result;
        if (ok) {
            auto& merged = job_->merged;
            std::stable_sort(merged.begin(), merged.end(),
                             [](const KeyValue& a, const KeyValue& b) { return a.first < b.first; });
            try {
                WriteKeyValues(job_->output, merged);
                result.statusCode = xmr::protocol::StatusCode::Ok;
                result.output = job_->output;
            } catch (const std::exception& error) {
                result.statusCode = xmr::protocol::StatusCode::Internal;
                result.reason = error.what();
            }
        } else {
            result.statusCode = xmr::protocol::StatusCode::Internal;
            result.reason = job_->scheduler.error();
        }
        if (job_->client != 0) {
            send(job_->client, xmr::protocol::MessageType::SubmitResult, 0, result.encode());
        }
        job_.reset();
    }

    void Coordinator::onFrame(ConnId id, const xmr::protocol::Frame& frame) {
        const auto it = conns_.find(id);
        if (it == conns_.end()) {
            return;
        }
        Conn& conn = *it->second;
        try {
            // 源文本里面第一个字段是命令名
            switch (frame.header.type) {
                case xmr::protocol::MessageType::Submit: {
                    // client提交一个job
                    if (job_) {
                        xmr::protocol::SubmitAck ack;
                        ack.statusCode = xmr::protocol::StatusCode::Unavailable;
                        ack.reason = "a job is already running";
                        send(id, xmr::protocol::MessageType::SubmitAck, frame.header.requestId, ack.encode());
                        break;
                    }
                    submitJob(id, xmr::protocol::Submit::decode(frame.body), frame.header.requestId);
                    break;
                }
                case xmr::protocol::MessageType::Shutdown: {
                    // client要求master关停
                    requestShutdown();
                    break;
                }
                case xmr::protocol::MessageType::Hello: {
                    // worker启动的时候给master发一下
                    conn.workerId = xmr::protocol::Hello::decode(frame.body).workerId;
                    registry_.add(conn.workerId, std::chrono::steady_clock::now());
                    byWorker_[conn.workerId] = id;
                    break;
                }
                case xmr::protocol::MessageType::DataAddress: {
                    // worker上报自己的数据面监听端口 供其它worker拉中间结果
                    const auto address = xmr::protocol::DataAddress::decode(frame.body);
                    workerData_[conn.workerId] = {conn.host, static_cast<std::uint16_t>(address.port)};
                    break;
                }
                case xmr::protocol::MessageType::RequestTask: {
                    // worker告诉master它空闲了 没job也先记着 有job时好直接派
                    registry_.markIdle(conn.workerId);
                    conn.lastRequest = frame.header.requestId;
                    break;
                }
                case xmr::protocol::MessageType::Done: {
                    if (!job_) {
                        break;
                    }
                    const auto done = xmr::protocol::Done::decode(frame.body);
                    const auto held = registry_.taskOf(conn.workerId);
                    if (held && held->kind == xmr::taskKind(done.kind) && held->id == done.taskId) {
                        job_->scheduler.markDone(held->kind, held->id, held->attempt);
                        if (held->kind == xmr::TaskKind::Map) {
                            // 记下这个map的结果在哪个worker上 供reduce去拉
                            mapOwner_[held->id] = conn.workerId;
                        }
                    }
                    registry_.complete(conn.workerId);
                    break;
                }
                case xmr::protocol::MessageType::Fail: {
                    if (!job_) {
                        break;
                    }
                    const auto fail = xmr::protocol::Fail::decode(frame.body);
                    const auto held = registry_.taskOf(conn.workerId);
                    if (held && held->kind == xmr::taskKind(fail.kind) && held->id == fail.taskId) {
                        // 任务失败按重试处理 次数用尽才判死整个job
                        retryOrFail(*held);
                    }
                    registry_.complete(conn.workerId);
                    break;
                }
                case xmr::protocol::MessageType::InputRequest: {
                    if (!job_) {
                        break;
                    }
                    // worker准备执行map函数了 跟master要map需要的文件数据
                    const auto request = xmr::protocol::InputRequest::decode(frame.body);
                    if (request.taskId >= job_->inputData.size()) {
                        throw std::runtime_error("INPUT out of range");
                    }
                    sendBlob(id, frame.header.requestId, job_->inputData[request.taskId]);
                    break;
                }
                case xmr::protocol::MessageType::Result: {
                    if (!job_) {
                        break;
                    }
                    // worker执行完了reduce 把最终结果给到了master
                    const auto result = xmr::protocol::ResultMessage::decode(frame.body);
                    auto pairs = xmr::deserializeKeyValues(std::string(result.payload.begin(), result.payload.end()));
                    job_->merged.insert(job_->merged.end(),
                                        std::make_move_iterator(pairs.begin()), std::make_move_iterator(pairs.end()));
                    break;
                }
                case xmr::protocol::MessageType::Ping: {
                    // worker周期心跳 master回Pong
                    const auto ping = xmr::protocol::Ping::decode(frame.body);
                    xmr::protocol::Pong pong;
                    pong.nonce = ping.nonce;
                    send(id, xmr::protocol::MessageType::Pong, frame.header.requestId, pong.encode());
                    break;
                }
                default:
                    throw std::runtime_error("unexpected control message");
            }
            if (registry_.contains(conn.workerId)) {
                registry_.touch(conn.workerId, std::chrono::steady_clock::now());
            }
        } catch (const std::exception& error) {
            fail(error.what());
        }
    }

    void Coordinator::onDisconnect(ConnId id) {
        const auto it = conns_.find(id);
        if (it == conns_.end()) {
            return;
        }
        const std::shared_ptr<Conn> conn = it->second;
        if (!conn->workerId.empty()) {
            recoverWorker(conn->workerId);
            registry_.remove(conn->workerId);
            byWorker_.erase(conn->workerId);
            workerData_.erase(conn->workerId);
        }
        conns_.erase(it);
        conn->loop->queueInLoop([conn] {
            conn->loop->remove(conn->connection.fd());
        });
    }

    std::vector<std::string> Coordinator::fetchPlan() const {
        std::vector<std::string> locations;
        for (std::size_t mapTask = 0; mapTask < job_->inputData.size(); ++mapTask) {
            const auto owner = mapOwner_.find(mapTask);
            if (owner == mapOwner_.end()) {
                continue;
            }
            const auto address = workerData_.find(owner->second);
            if (address == workerData_.end()) {
                continue;
            }
            locations.push_back(std::to_string(mapTask) + "," + address->second.first
                                + "," + std::to_string(address->second.second));
        }
        return locations;
    }

    void Coordinator::dispatch() {
        if (byWorker_.size() < job_->expectedWorkers) {
            return;
        }        while (registry_.hasIdle()) {
            auto task = job_->scheduler.takeTask();
            if (!task) {
                break;
            }
            const auto workerId = registry_.assignNext(*task, std::chrono::steady_clock::now());
            if (!workerId) {
                break;
            }
            const auto byName = byWorker_.find(*workerId);
            if (byName == byWorker_.end()) {
                continue;
            }
            const auto it = conns_.find(byName->second);
            if (it == conns_.end()) {
                continue;
            }
            // master向worker派发任务
            auto message = xmr::toTaskMessage(*task);
            if (task->kind == xmr::TaskKind::Reduce) {
                message.locations = fetchPlan();
            }
            send(byName->second, xmr::protocol::MessageType::Task, it->second->lastRequest, message.encode());
        }
    }

    void Coordinator::send(ConnId id, xmr::protocol::MessageType type, std::uint32_t requestId,
                           const std::vector<std::uint8_t>& body, std::uint16_t flags) {
        const auto it = conns_.find(id);
        if (it == conns_.end()) {
            return;
        }
        const std::shared_ptr<Conn> conn = it->second;
        const auto bytes = xmr::protocol::makeFrame(type, requestId, body, flags);
        conn->loop->queueInLoop([conn, bytes] {
            conn->out.append(bytes);
            flushConn(*conn);
        });
    }

    // 大blob分块发送 最后一块不带More
    void Coordinator::sendBlob(ConnId id, std::uint32_t requestId, const std::string& blob) {
        const std::size_t total = blob.size();
        std::size_t offset = 0;
        do {
            const std::size_t n = std::min<std::size_t>(xmr::protocol::kChunkBytes, total - offset);
            xmr::protocol::DataMessage data;
            data.offset = offset;
            data.total = total;
            data.payload.assign(blob.begin() + static_cast<std::ptrdiff_t>(offset),
                                blob.begin() + static_cast<std::ptrdiff_t>(offset + n));
            const bool more = offset + n < total;
            send(id, xmr::protocol::MessageType::Data, requestId, data.encode(),
                 more ? static_cast<std::uint16_t>(xmr::protocol::Flag::More) : 0);
            offset += n;
        } while (offset < total);
    }

    // 回收worker持有的任务 重发
    void Coordinator::requeueTask(const std::string& workerId) {
        if (!job_) {
            registry_.reclaim(workerId);
            return;
        }
        const auto task = registry_.reclaim(workerId);
        if (task) {
            retryOrFail(*task);
        }
    }

    void Coordinator::recoverWorker(const std::string& workerId) {
        requeueTask(workerId);
        if (!job_) {
            return;
        }
        // 该worker上已完成的map输出随它一起没了 需要重跑
        for (auto it = mapOwner_.begin(); it != mapOwner_.end();) {
            if (it->second == workerId) {
                job_->scheduler.invalidate(xmr::TaskKind::Map, it->first);
                it = mapOwner_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // 重发任务 次数用尽才判整个job失败
    void Coordinator::retryOrFail(const xmr::Task& task) {
        if (!job_) {
            return;
        }
        if (!job_->scheduler.retry(task)) {
            job_->scheduler.markFailed(task.kind, task.id, "attempts exhausted");
        }
    }

    void Coordinator::requestShutdown() {
        if (!stopping_.exchange(true) && onShutdown_) {
            onShutdown_();
        }
    }

    // 由worker reactor线程调用 更新这个连接的读写关注事件
    void updateInterest(Conn& conn) {
        std::uint32_t events = xmr::net::kReadable;
        if (!conn.out.empty()) {
            events |= xmr::net::kWritable;
        }
        conn.loop->modify(conn.connection.fd(), events);
    }

    void flushConn(Conn& conn) {
        if (conn.broken) {
            return;
        }
        while (!conn.out.empty()) {
            std::size_t sent = 0;
            const xmr::net::IoStatus status = xmr::net::sendFrom(
                conn.connection.fd(), conn.out.data(), conn.out.size(), sent);
            if (status == xmr::net::IoStatus::Ok) {
                conn.out.consume(sent);
            } else if (status == xmr::net::IoStatus::WouldBlock) {
                break;
            } else {
                conn.broken = true;
                conn.coordinatorLoop->queueInLoop([owner = conn.owner, id = conn.id] {
                    owner->onDisconnect(id);
                });
                return;
            }
        }
        updateInterest(conn);
    }

    // 注册连接上的读写处理 由这个连接归属的worker reactor调用
    void serveConn(std::shared_ptr<Conn> conn) {
        conn->loop->add(conn->connection.fd(), xmr::net::kReadable,
            [conn](std::uint32_t events) {
                if (conn->broken) {
                    return;
                }
                if (events & xmr::net::kWritable) {
                    flushConn(*conn);
                }
                if (events & (xmr::net::kReadable | xmr::net::kBroken)) {
                    bool closed = false;
                    while (true) {
                        const xmr::net::IoStatus status = xmr::net::recvInto(conn->connection.fd(), conn->in);
                        if (status == xmr::net::IoStatus::Ok) {
                            continue;
                        }
                        if (status == xmr::net::IoStatus::WouldBlock) {
                            break;
                        }
                        closed = true;
                        break;
                    }
                    xmr::protocol::FrameDecoder decoder(conn->in);
                    while (auto frame = decoder.next()) {
                        conn->coordinatorLoop->queueInLoop([conn, frame = *frame]() mutable {
                            conn->owner->onFrame(conn->id, frame);
                        });
                    }
                    if (closed) {
                        conn->broken = true;
                        conn->coordinatorLoop->queueInLoop([conn] {
                            conn->owner->onDisconnect(conn->id);
                        });
                    }
                }
            });
    }
} // namespace

int main(int argc, char** argv) {
    // master端口
    std::string listen = "127.0.0.1:9527";

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--listen" && i + 1 < argc) {
            listen = argv[++i];
        }
    }

    try {
        const auto [host,port] = parseEndpoint(listen);
        xmr::net::Listener listener(host, port);
        xmr::net::setNonBlocking(listener.fd());
        std::cout << "LISTENING " << host << ":" << listener.port() << std::endl;

        Coordinator coordinator;
        xmr::net::EventLoopGroup ioGroup(kIoThreads);
        xmr::net::EventLoop coordinatorLoop;
        xmr::net::EventLoop bossLoop;
        std::atomic<ConnId> nextConnId{1};

        std::promise<void> donePromise;
        std::future<void> doneFuture = donePromise.get_future();
        coordinator.setOnShutdown([&donePromise] { donePromise.set_value(); });
        coordinator.setLoop(&coordinatorLoop);

        std::thread coordinatorThread([&] { coordinatorLoop.run(); });
        ioGroup.start();
        std::thread bossThread([&] { bossLoop.run(); });

        coordinatorLoop.runInLoop([&] { coordinator.start(); });

        auto acceptHandler = [&](std::uint32_t) {
            try {
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
                    auto conn = std::make_shared<Conn>();
                    conn->connection = std::move(connection);
                    conn->host = peerHost(conn->connection.fd());
                    conn->id = nextConnId.fetch_add(1);
                    conn->owner = &coordinator;
                    conn->coordinatorLoop = &coordinatorLoop;
                    conn->loop = ioGroup.next();
                    xmr::net::EventLoop* workerLoop = conn->loop;
                    coordinatorLoop.runInLoop([&coordinator, conn, workerLoop] {
                        coordinator.onConnect(conn->id, conn);
                        workerLoop->runInLoop([conn] { serveConn(conn); });
                    });
                }
            } catch (const std::exception&) {
            }
        };
        bossLoop.runInLoop([&] {
            bossLoop.add(listener.fd(), xmr::net::kReadable, acceptHandler);
        });

        doneFuture.wait();

        bossLoop.stop();
        coordinatorLoop.stop();
        ioGroup.stop();
        bossThread.join();
        coordinatorThread.join();

        if (bossLoop.error()) {
            std::rethrow_exception(bossLoop.error());
        }
        if (coordinatorLoop.error()) {
            std::rethrow_exception(coordinatorLoop.error());
        }

        for (auto& [id, conn] : coordinator.conns()) {
            try {
                const auto frame = xmr::protocol::makeFrame(xmr::protocol::MessageType::Shutdown, 0,
                                                            xmr::protocol::Shutdown{}.encode());
                conn->out.append(frame);
                xmr::net::setBlocking(conn->connection.fd());
                while (!conn->out.empty()) {
                    const std::size_t pending = conn->out.size();
                    xmr::net::sendAll(conn->connection.fd(), conn->out.data(), pending);
                    conn->out.consume(pending);
                }
                ::shutdown(conn->connection.fd(), SHUT_WR);
                char buffer[256];
                while (::read(conn->connection.fd(), buffer, sizeof(buffer)) > 0) {
                }
            } catch (const std::exception&) {
                // 对端已断开 忽略
            }
        }

        if (coordinator.hasError()) {
            throw std::runtime_error(coordinator.error());
        }
    } catch (const std::exception& error) {
        std::cerr << "master: " << error.what() << '\n';
        return static_cast<int>(ExitCode::Failure);
    }

    return static_cast<int>(ExitCode::Success);
}
