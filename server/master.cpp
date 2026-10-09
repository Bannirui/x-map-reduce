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
#include<optional>
#include<stdexcept>
#include<string>
#include<thread>
#include<unordered_map>
#include<unordered_set>
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
    // 参考LangGraph Pregel的super-step 每隔多久一个loop
    constexpr std::chrono::milliseconds kTick{20};
    // worker reactor的线程数
    constexpr std::size_t kIoThreads{4};
    // 控制面默认端口
    constexpr std::uint16_t kDefaultCtlPort = 9527;
    // 数据面默认端口
    constexpr std::uint16_t kDefaultDataPort = 9331;
    // todo 长尾任务的判定 静态方式后面要改成动态判定 master根据worker上报的信息判断
    constexpr std::chrono::seconds kMinSpeculate{3};

    using ConnId = std::uint64_t;
    using TimePoint = std::chrono::steady_clock::time_point;

    struct Conn;
    class Coordinator;

    // 对TCP再包一层
    struct Conn {
        // TCP连接
        xmr::net::Connection connection;
        // 缓冲区放TCP里面读到的数据
        xmr::net::ByteBuffer in;
        // 缓冲区放要往TCP写的数据
        xmr::net::ByteBuffer out;
        // 负责TCP读写的线程
        xmr::net::EventLoop* loop = nullptr;
        // 独占的线程 只负责跑master自己
        xmr::net::EventLoop* coordinatorLoop = nullptr;
        // master
        Coordinator* owner = nullptr;
        // worker连接进来master的编号
        ConnId id = 0;
        // worker的ip master不负责数据 只管理元数据 将来shuffle的数据是worker跟worker传 所以master要保存worker的ip
        std::string host;
        // coordinator线程写 用于回填派发时的请求id
        std::string workerId;
        std::uint32_t lastRequest = 0;
        // 这个worker已经加载过的插件哈希
        std::unordered_set<std::string> plugins;
        // 数据连接上client上传插件的接收缓冲 coordinator线程独占
        std::vector<std::uint8_t> uploadBuffer;
        // TCP异常了 可能是客户端强关了 这种断掉的TCP连接就不要处理了
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

    /**
     * @param value 启动的时候--listen只传了端口号
     * @param defaultHost 给拼接成完整的host:port
     */
    std::pair<std::string, std::uint16_t> parseListen(const std::string& value, const std::string& defaultHost) {
        if (value.find(':') == std::string::npos) {
            return {defaultHost, static_cast<std::uint16_t>(std::stoul(value))};
        }
        return parseEndpoint(value);
    }

    void usage(const char* program) {
        std::cerr << "Usage: " << program << " --listen <port> [--data-listen <port>]\n";
    }

    /// @param fd TCP连接 本质是TCP连接在本端的那个socket 它就代表了TCP
    /// @return TCP连接对端的ip
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
                 std::vector<std::string> inputData, std::string output, ConnId client)
            : scheduler(job, inputs, reducers),
              name(std::move(job)),
              inputData(std::move(inputData)),
              output(std::move(output)),
              client(client) {
        }

        xmr::Scheduler scheduler;
        std::string name;
        std::vector<std::string> inputData;
        std::vector<KeyValue> merged;
        std::string output;
        ConnId client = 0;
    };

    /**
     * master的抽象
     * 负责worker管理 任务调度
     */
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

        void setDataAddress(std::string host, std::uint16_t port) {
            dataHost_ = std::move(host);
            dataPort_ = port;
        }

        // master端的线程模型 Pregel算法的super step 每隔多久一个loop
        void start() {
            loop_->addInterval(kTick, [this] { tick(); });
        }

        void onConnect(ConnId id, std::shared_ptr<Conn> conn) {
            conns_[id] = std::move(conn);
        }

        /// @brief master的控TCP收到了过来的消息 可能是控制端口 可能是数据端口
        ///          - 可能是worker发的
        ///          - 可能是client发的
        /// @param id 控制端口的TCP
        /// @param frame worker发过来的消息
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
        // master线程模型 每隔多久一个loop
        void tick();

        void startJob(ConnId client, const xmr::protocol::Submit& submit, std::vector<std::string> inputData);

        // 输入+插件都收齐了就开跑
        void maybeStartJob();

        /// @brief 给worker派发任务 含reduce的拉取计划
        /// @param workerId worker
        /// @param task 要派给worker的任务
        void sendTask(const std::string& workerId, const xmr::Task& task);

        // 推测执行:长尾任务复制一份到空闲worker
        void speculate();

        // 某个attempt胜出后 取消同一任务的其它attempt
        void cancelLosers(xmr::TaskKind kind, std::size_t id, const std::string& winner);

        // 把当前job的插件分块发给某个worker 它没有的话
        void sendPlugin(ConnId id);

        void finishJob();

        /**
         * 给master上管理的所有空闲worker派发任务 包含reduce任务的拉取计划
         */
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
        // worker管理器
        xmr::WorkerRegistry registry_;
        // master的控制端口的TCP 都有谁连接进来的 可能是client 可能是worker
        std::unordered_map<ConnId, std::shared_ptr<Conn> > conns_;
        // k=worker v=TCP连接
        std::unordered_map<std::string, ConnId> byWorker_;
        // worker的数据面地址 host + port
        std::unordered_map<std::string, std::pair<std::string, std::uint16_t> > workerData_;
        // map任务执行的中间结果在哪个worker上 k=任务id v=worker的id
        std::unordered_map<std::size_t, std::string> mapOwner_;

        // 正在等插件字节的提交 client + 元数据
        struct Pending {
            ConnId client = 0;
            xmr::protocol::Submit submit;
            std::uint32_t requestId = 0;
        };
        // client会把map阶段用到的input从数据端口传过来 master把input缓存起来
        std::vector<std::string> inputUpload_;
        // client还没传input进来
        std::size_t inputDone_ = 0;
        // 按内容哈希存插件二进制
        std::unordered_map<std::string, std::vector<std::uint8_t> > pluginStore_;
        // 当前job的插件哈希 空表示worker本地已预加载
        std::string pluginHash_;
        // 已经加载当前插件的worker数
        std::size_t pluginReady_ = 0;
        // master数据端口 供client上传插件/worker拉取map的input
        std::string dataHost_;
        std::uint16_t dataPort_ = 0;
        // 任务第一次派发的时间 用于推测执行
        std::unordered_map<std::size_t, TimePoint> mapStarted_;
        std::unordered_map<std::size_t, TimePoint> reduceStarted_;
        // 最快完成的任务耗时 作为推测阈值参考
        bool fastestSet_ = false;
        std::chrono::steady_clock::duration fastest_{};
        /**
         * master不支持同时接收多个job执行 master同一时刻只能有一个job
         * 正在跑或待收尾的job
         */
        std::unique_ptr<JobState> job_;
        // 某次提交已受理 还在等client传map阶段需要的输入或者job插件
        std::optional<Pending> pending_;
        // 表示master运行状态
        std::atomic<bool> stopping_{false};
        std::string error_;
        std::function<void()> onShutdown_;
    };

    void Coordinator::tick() {
        if (stopping_.load()) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        const auto expired = registry_.poll(now);
        // 没有按时发送心跳的worker
        for (const auto& workerId : expired.workers) {
            // 这些worker被master判定挂了 要把worker已经完成的任务回收过来重新派发给其他worker
            recoverWorker(workerId);
        }
        for (const auto& task : expired.tasks) {
            // 任务超时了 重新派发
            retryOrFail(task);
        }
        if (!job_) {
            return;
        }
        // 当前master的job在跑
        dispatch();
        speculate();
        if (job_->scheduler.finished() || job_->scheduler.failed()) {
            finishJob();
        }
    }

    void Coordinator::startJob(ConnId client, const xmr::protocol::Submit& submit,
                               std::vector<std::string> inputData) {
        job_ = std::make_unique<JobState>(submit.job, submit.inputs, submit.reducers,
                                          std::move(inputData), submit.output, client);
        mapOwner_.clear();
        pluginHash_ = submit.pluginHash;
        pluginReady_ = 0;
        // 有插件的话分发给还没加载的worker
        if (!pluginHash_.empty()) {
            for (const auto& entry : byWorker_) {
                const auto it = conns_.find(entry.second);
                if (it != conns_.end() && it->second->plugins.count(pluginHash_) != 0) {
                    ++pluginReady_;
                } else {
                    sendPlugin(entry.second);
                }
            }
        }
    }

    void Coordinator::maybeStartJob() {
        if (!pending_) {
            return;
        }
        if (inputDone_ < inputUpload_.size()) {
            return;
        }
        const std::string& hash = pending_->submit.pluginHash;
        if (!hash.empty() && pluginStore_.find(hash) == pluginStore_.end()) {
            return;
        }
        const ConnId client = pending_->client;
        const xmr::protocol::Submit submit = pending_->submit;
        std::vector<std::string> inputData = std::move(inputUpload_);
        inputUpload_.clear();
        inputDone_ = 0;
        pending_.reset();
        startJob(client, submit, std::move(inputData));
    }

    void Coordinator::sendPlugin(ConnId id) {
        // 让worker自己去master数据面拉插件
        if (pluginHash_.empty() || !job_) {
            return;
        }
        xmr::protocol::NeedPlugin need;
        need.hash = pluginHash_;
        need.job = job_->name;
        send(id, xmr::protocol::MessageType::NeedPlugin, 0, need.encode());
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
        // 看看是谁连接进来的
        const auto it = conns_.find(id);
        if (it == conns_.end()) {
            return;
        }
        // worker或者client跟master控制端口之间的TCP连接
        Conn& conn = *it->second;
        try {
            switch (frame.header.type) {
                case xmr::protocol::MessageType::Submit: {
                    // client提交一个job
                    if (job_ || pending_) {
                        // todo master不支持同时多个job在跑
                        xmr::protocol::SubmitAck ack;
                        ack.statusCode = xmr::protocol::StatusCode::Unavailable;
                        ack.reason = "a job is already running";
                        send(id, xmr::protocol::MessageType::SubmitAck, frame.header.requestId, ack.encode());
                        break;
                    }
                    // client给master提交的job
                    const auto submit = xmr::protocol::Submit::decode(frame.body);
                    xmr::protocol::SubmitAck ack;
                    // master的数据端口 让client传job插件和input过来
                    ack.dataHost = dataHost_;
                    ack.dataPort = dataPort_;
                    if (submit.job.empty() || submit.output.empty() || submit.inputs.empty() || submit.reducers == 0) {
                        ack.statusCode = xmr::protocol::StatusCode::InvalidArgument;
                        ack.reason = "invalid submit";
                        send(id, xmr::protocol::MessageType::SubmitAck, frame.header.requestId, ack.encode());
                        break;
                    }
                    // 标识master已经开始受理client的job了 现在开始等client把master需要的job插件和input传进来
                    pending_ = Pending{id, submit, frame.header.requestId};
                    // 将来接收client从数据端口传input
                    inputUpload_.assign(submit.inputs.size(), std::string());
                    // 总共M个数据 client会从数据端口传过来 每传过来一个就统计起来 保证map阶段依赖的数据是全的
                    inputDone_ = 0;
                    ack.statusCode = xmr::protocol::StatusCode::Ok;
                    send(id, xmr::protocol::MessageType::SubmitAck, frame.header.requestId, ack.encode());
                    break;
                }
                case xmr::protocol::MessageType::Plugin: {
                    // client在数据端口把插件字节传上来
                    const auto chunk = xmr::protocol::Plugin::decode(frame.body);
                    if (chunk.offset != conn.uploadBuffer.size()) {
                        fail("bad plugin upload");
                        break;
                    }
                    conn.uploadBuffer.insert(conn.uploadBuffer.end(), chunk.payload.begin(), chunk.payload.end());
                    if ((frame.header.flags & static_cast<std::uint16_t>(xmr::protocol::Flag::More)) != 0) {
                        break;
                    }
                    if (xmr::protocol::contentHash(conn.uploadBuffer) != chunk.hash) {
                        if (pending_ && pending_->submit.pluginHash == chunk.hash) {
                            xmr::protocol::SubmitAck error;
                            error.statusCode = xmr::protocol::StatusCode::Internal;
                            error.reason = "plugin hash mismatch";
                            send(pending_->client, xmr::protocol::MessageType::SubmitAck,
                                 pending_->requestId, error.encode());
                            pending_.reset();
                        }
                        conn.uploadBuffer.clear();
                        break;
                    }
                    pluginStore_[chunk.hash] = std::move(conn.uploadBuffer);
                    conn.uploadBuffer.clear();
                    maybeStartJob();
                    break;
                }
                case xmr::protocol::MessageType::InputBlob: {
                    // client在数据端口把map任务需要的输入input传上来
                    if (!pending_) {
                        break;
                    }
                    const auto chunk = xmr::protocol::InputBlob::decode(frame.body);
                    if (chunk.index >= inputUpload_.size()) {
                        fail("bad input index");
                        break;
                    }
                    std::string& buffer = inputUpload_[chunk.index];
                    if (chunk.offset != buffer.size()) {
                        fail("bad input chunk");
                        break;
                    }
                    buffer.append(chunk.payload.begin(), chunk.payload.end());
                    if ((frame.header.flags & static_cast<std::uint16_t>(xmr::protocol::Flag::More)) != 0) {
                        break;
                    }
                    ++inputDone_;
                    maybeStartJob();
                    break;
                }
                case xmr::protocol::MessageType::PullInput: {
                    if (!job_) {
                        break;
                    }
                    // worker从数据端口拉map输入
                    const auto request = xmr::protocol::PullInput::decode(frame.body);
                    if (request.taskId >= job_->inputData.size()) {
                        throw std::runtime_error("INPUT out of range");
                    }
                    sendBlob(id, frame.header.requestId, job_->inputData[request.taskId]);
                    break;
                }
                case xmr::protocol::MessageType::PullPlugin: {
                    // worker从数据端口拉插件
                    const auto request = xmr::protocol::PullPlugin::decode(frame.body);
                    const auto it = pluginStore_.find(request.hash);
                    if (it != pluginStore_.end()) {
                        const std::string blob(it->second.begin(), it->second.end());
                        sendBlob(id, frame.header.requestId, blob);
                    }
                    break;
                }
                case xmr::protocol::MessageType::PluginAck: {
                    // worker加载插件的结果
                    const auto ack = xmr::protocol::PluginAck::decode(frame.body);
                    if (pluginHash_.empty() || ack.hash != pluginHash_) {
                        break;
                    }
                    if (!ack.ok) {
                        fail("worker failed to load plugin: " + ack.reason);
                        break;
                    }
                    if (conn.plugins.insert(ack.hash).second) {
                        ++pluginReady_;
                    }
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
                    {
                        // 告诉worker master数据端口 供它拉输入/插件
                        xmr::protocol::MasterData data;
                        data.port = dataPort_;
                        send(id, xmr::protocol::MessageType::MasterData, 0, data.encode());
                    }
                    if (job_ && !pluginHash_.empty() && conn.plugins.count(pluginHash_) == 0) {
                        sendPlugin(id);
                    }
                    break;
                }
                case xmr::protocol::MessageType::DataAddress: {
                    // worker上报自己的数据监听端口 供其它worker拉中间结果
                    const auto address = xmr::protocol::DataAddress::decode(frame.body);
                    workerData_[conn.workerId] = {conn.host, static_cast<std::uint16_t>(address.port)};
                    break;
                }
                case xmr::protocol::MessageType::RequestTask: {
                    // worker告诉master它空闲了 希望master给它派任务
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
                        const bool won = job_->scheduler.markDone(held->kind, held->id, held->attempt);
                        if (won) {
                            // 记下耗时 作为推测阈值参考
                            auto& starts = held->kind == xmr::TaskKind::Map ? mapStarted_ : reduceStarted_;
                            const auto it = starts.find(held->id);
                            if (it != starts.end()) {
                                const auto span = std::chrono::steady_clock::now() - it->second;
                                if (!fastestSet_ || span < fastest_) {
                                    fastest_ = span;
                                    fastestSet_ = true;
                                }
                                starts.erase(it);
                            }
                            // 记下这个map的结果在哪个worker上 供reduce去拉
                            if (held->kind == xmr::TaskKind::Map) {
                                mapOwner_[held->id] = conn.workerId;
                            }
                            // first-完成wins 取消同任务的其它重复attempt
                            cancelLosers(held->kind, held->id, conn.workerId);
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
                case xmr::protocol::MessageType::Progress: {
                    // worker上报任务进度 目前用于观测/推测参考
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
            if (!pluginHash_.empty() && conn->plugins.count(pluginHash_) != 0 && pluginReady_ > 0) {
                --pluginReady_;
            }
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
    
    void Coordinator::sendTask(const std::string& workerId, const xmr::Task& task) {
        const auto byName = byWorker_.find(workerId);
        if (byName == byWorker_.end()) {
            return;
        }
        const auto it = conns_.find(byName->second);
        if (it == conns_.end()) {
            return;
        }
        // master向worker派发任务 用控制端口的TCP发个消息
        auto message = xmr::toTaskMessage(task);
        if (task.kind == xmr::TaskKind::Reduce) {
            // map中间结果按照R分区了 所以如果派发的是reduce任务 还得告诉它去哪些worker上的什么地方接数据 也就是woker的数据端口
            message.locations = fetchPlan();
        }
        send(byName->second, xmr::protocol::MessageType::Task, it->second->lastRequest, message.encode());
    }

    void Coordinator::dispatch() {
        if (byWorker_.empty()) {
            return;
        }
        // 用当前已连接的worker：等它们都就绪再派
        if (!pluginHash_.empty() && pluginReady_ < byWorker_.size()) {
            return;
        }
        // 找到在等任务的worker
        while (registry_.hasIdle()) {
            // 创建个任务
            auto task = job_->scheduler.takeTask();
            if (!task) {
                break;
            }
            const auto now = std::chrono::steady_clock::now();
            // 任务执行倒计时 worker得在限定时间内执行完
            const auto workerId = registry_.assignNext(*task, now);
            if (!workerId) {
                break;
            }
            // 记录第一次派发时间 用于推测长尾任务
            auto& starts = task->kind == xmr::TaskKind::Map ? mapStarted_ : reduceStarted_;
            starts.emplace(task->id, now);
            // 给worker派发任务
            sendTask(*workerId, *task);
        }
    }

    void Coordinator::speculate() {
        if (!job_ || !registry_.hasIdle()) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        std::chrono::steady_clock::duration threshold = kMinSpeculate;
        if (fastestSet_) {
            threshold = std::max(threshold, fastest_ * 2);
        }
        for (const auto& entry : byWorker_) {
            const auto task = registry_.taskOf(entry.first);
            if (!task) {
                continue;
            }
            const auto& starts = task->kind == xmr::TaskKind::Map ? mapStarted_ : reduceStarted_;
            const auto it = starts.find(task->id);
            if (it == starts.end() || now - it->second < threshold) {
                continue;
            }
            const auto extra = job_->scheduler.speculate(task->kind, task->id);
            if (!extra) {
                continue;
            }
            const auto workerId = registry_.assignNext(*extra, now);
            if (!workerId) {
                break;
            }
            sendTask(*workerId, *extra);
            break;
        }
    }

    void Coordinator::cancelLosers(xmr::TaskKind kind, std::size_t id, const std::string& winner) {
        xmr::protocol::Cancel cancel;
        cancel.kind = xmr::wireKind(kind);
        cancel.taskId = id;
        for (const auto& entry : byWorker_) {
            if (entry.first == winner) {
                continue;
            }
            const auto task = registry_.taskOf(entry.first);
            if (task && task->kind == kind && task->id == id) {
                send(entry.second, xmr::protocol::MessageType::Cancel, 0, cancel.encode());
            }
        }
    }

    /// @brief master用控制端口给TCP回复消息 可能是传给了client 也可能是传给了worker
    /// @param id 控制端口的TCP
    /// @param type 消息类型
    /// @param requestId 回复的是哪个请求
    /// @param body 消息内容
    /// @param flags 
    void Coordinator::send(ConnId id, xmr::protocol::MessageType type, std::uint32_t requestId, const std::vector<std::uint8_t>& body, std::uint16_t flags) {
        // 找到控制端口的TCP
        const auto it = conns_.find(id);
        if (it == conns_.end()) {
            return;
        }
        const std::shared_ptr<Conn> conn = it->second;
        const auto bytes = xmr::protocol::makeFrame(type, requestId, body, flags);
        // 丢到线程循环器 发送到TCP对端
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
        // woker没有心跳 被master判定下线了 回收派发给它的任务
        requeueTask(workerId);
        if (!job_) {
            return;
        }
        // 该worker上已完成的任务需要回收 master再派发给其他worker
        for (auto it = mapOwner_.begin(); it != mapOwner_.end();) {
            if (it->second == workerId) {
                // 回收任务
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
        // 重试的attempt作废 重新计时
        auto& starts = task.kind == xmr::TaskKind::Map ? mapStarted_ : reduceStarted_;
        starts.erase(task.id);
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

    /// @brief master用TCP连接给对端发消息
    /// @param conn TCP连接
    void flushConn(Conn& conn) {
        if (conn.broken) {
            return;
        }
        while (!conn.out.empty()) {
            // 每次TCP发出去了多少数据
            std::size_t sent = 0;
            const xmr::net::IoStatus status = xmr::net::sendFrom(conn.connection.fd(), conn.out.data(), conn.out.size(), sent);
            if (status == xmr::net::IoStatus::Ok) {
                // 把发出去的数据从缓冲区摘掉
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

    /// @brief 连接到master的TCP 可能连的是控制端口 也可能连的是数据端 不管是哪个端口的TCP连接 它的读写都放在就一起处理 可能通过消息类型区分出来
    /// @param conn 里面能拿到TCP连接
    void serveConn(std::shared_ptr<Conn> conn) {
        // 读写任务交给读写线程
        conn->loop->add(conn->connection.fd(), xmr::net::kReadable,
            [conn](std::uint32_t events) {
                // 不管是控制端口还是数据端口 只要有人向master发送消息 master这边就判断TCP收到数据了
                if (conn->broken) {
                    return;
                }
                // redis中有这种读写顺序防护
                if (events & xmr::net::kWritable) {
                    flushConn(*conn);
                }
                if (events & (xmr::net::kReadable | xmr::net::kBroken)) {
                    bool closed = false;
                    while (true) {
                        // 把TCP传过来的数据读到缓冲区
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
                    // 网络传过来的大端序解码
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
    // master控制面端口
    std::string listen;
    // master数据面端口
    std::string dataListen;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--listen" && i + 1 < argc) {
            listen = argv[++i];
        } else if (arg == "--data-listen" && i + 1 < argc) {
            dataListen = argv[++i];
        }
    }

    try {
        if (listen.empty()) {
            throw UsageError("missing --listen");
        }
        // 控制端口
        const auto [host,port] = parseListen(listen, "127.0.0.1");
        // 监听在控制端口上
        xmr::net::Listener listener(host, port);
        xmr::net::setNonBlocking(listener.fd());
        std::cout << "LISTENING " << host << ":" << listener.port() << std::endl;

        // 数据面端口 worker拉map的input/插件 client上传插件
        std::string dataHost = host;
        std::uint16_t dataPort = kDefaultDataPort;
        if (!dataListen.empty()) {
            const auto [dh, dp] = parseListen(dataListen, host);
            dataHost = dh;
            dataPort = dp;
        }
        xmr::net::Listener dataListener(dataHost, dataPort);
        xmr::net::setNonBlocking(dataListener.fd());
        std::cout << "DATA_LISTENING " << dataHost << ":" << dataListener.port() << std::endl;

        Coordinator coordinator;
        // 独占的线程 只负责跑master自己
        xmr::net::EventLoop coordinatorLoop;
        // 负责master接收新来的TCP连接 可能是连接的控制端口 也可能是连接的数据端口
        xmr::net::EventLoop bossLoop;
        // master真正干活的线程
        xmr::net::EventLoopGroup ioGroup(kIoThreads);
        // master给worker连接进来的TCP编号
        std::atomic<ConnId> nextConnId{1};

        std::promise<void> donePromise;
        std::future<void> doneFuture = donePromise.get_future();
        coordinator.setOnShutdown([&donePromise] { donePromise.set_value(); });
        coordinator.setLoop(&coordinatorLoop);
        coordinator.setDataAddress(dataHost, dataListener.port());

        std::thread coordinatorThread([&] { coordinatorLoop.run(); });
        ioGroup.start();
        std::thread bossThread([&] { bossLoop.run(); });

        // master启动起来
        coordinatorLoop.runInLoop([&] { coordinator.start(); });

        // 处理连接请求 可能是有人连master的控制端口 可能是有人连master的数据端口
        auto acceptFrom = [&](xmr::net::Listener& acceptor) {
            try {
                while (true) {
                    // 从服务端全连接队列拿TCP连接
                    xmr::net::Connection connection;
                    const xmr::net::IoStatus status = acceptor.acceptNonBlocking(connection);
                    if (status == xmr::net::IoStatus::WouldBlock) {
                        break;
                    }
                    if (status != xmr::net::IoStatus::Ok) {
                        throw std::runtime_error("accept failed");
                    }
                    xmr::net::setNonBlocking(connection.fd());
                    auto conn = std::make_shared<Conn>();
                    // tcp连接
                    conn->connection = std::move(connection);
                    // worker的ip
                    conn->host = peerHost(conn->connection.fd());
                    conn->id = nextConnId.fetch_add(1);
                    conn->owner = &coordinator;
                    conn->coordinatorLoop = &coordinatorLoop;
                    // 真正干活的线程负责TCP读写
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
            // 控制端口的连接请求处理
            bossLoop.add(listener.fd(), xmr::net::kReadable, [&](std::uint32_t) { acceptFrom(listener); });
            // 数据端口的连接请求处理
            bossLoop.add(dataListener.fd(), xmr::net::kReadable, [&](std::uint32_t) { acceptFrom(dataListener); });
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
                const auto frame = xmr::protocol::makeFrame(xmr::protocol::MessageType::Shutdown, 0, xmr::protocol::Shutdown{}.encode());
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
