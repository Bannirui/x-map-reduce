#pragma once

#include "mapreduce/mapreduce.h"
#include "master/block_placement.h"
#include "master/connection_table.h"
#include "master/plugin_distributor.h"
#include "master/types.h"
#include "protocol/framing.h"
#include "protocol/messages.h"
#include "runtime/scheduler.h"
#include "runtime/task.h"
#include "runtime/worker_registry.h"

#include <xnet/channel.h>
#include <xnet/event_loop.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace xmr::master {
    // master判定worker心跳超时的阈值
    inline constexpr std::chrono::seconds kHeartbeatTimeout{6};
    // 单个任务执行超时的阈值 超了就重发
    inline constexpr std::chrono::seconds kTaskTimeout{30};
    // 参考LangGraph Pregel的super-step 每隔多久一个loop
    inline constexpr std::chrono::milliseconds kTick{20};
    // worker reactor的线程数
    inline constexpr std::size_t kIoThreads{4};
    // 控制面默认端口
    inline constexpr std::uint16_t kDefaultCtlPort = 9527;
    // 数据面默认端口
    inline constexpr std::uint16_t kDefaultDataPort = 9331;
    // todo 长尾任务的判定 静态方式后面要改成动态判定 master根据worker上报的信息判断
    inline constexpr std::chrono::seconds kMinSpeculate{3};

    using TimePoint = std::chrono::steady_clock::time_point;

    // 一次提交进来的一次job运行的全部状态
    struct JobState {
        JobState(std::string job, std::vector<std::string> inputs, std::size_t reducers,
                 std::vector<std::vector<std::string> > mapBlocks, std::string output, ConnId client)
            : scheduler(job, inputs, reducers),
              name(std::move(job)),
              maps(inputs.size()),
              mapBlocks(std::move(mapBlocks)),
              output(std::move(output)),
              client(client) {}

        xmr::Scheduler scheduler;
        std::string name;
        std::size_t maps = 0;
        std::vector<std::vector<std::string> > mapBlocks;
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
        Coordinator() : registry_(kHeartbeatTimeout, kTaskTimeout) {}

        void setLoop(EventLoop* loop);

        void setOnShutdown(std::function<void()> onShutdown);

        void setDataAddress(std::string host, std::uint16_t port);

        // master端的线程模型 Pregel算法的super step 每隔多久一个loop
        void start();

        /// @brief 有人连接进来就给缓存起来
        void onConnect(std::shared_ptr<ConnState> conn);

        /// @brief master的控TCP收到了过来的消息 可能是控制端口 可能是数据端口
        ///          - 可能是worker发的
        ///          - 可能是client发的
        /// @param id 控制端口的TCP
        /// @param frame worker发过来的消息
        void onFrame(ConnId id, const xmr::protocol::Frame& frame);

        void onDisconnect(ConnId id);

        bool hasError() const;

        std::string error() const;

        std::unordered_map<ConnId, std::shared_ptr<ConnState> >& conns();

    private:
        // master线程模型 每隔多久一个loop
        void tick();

        /// @brief client给master提交了job master要把自己的数据端口回复给client
        /// 让client再把job的插件跟map需要的input传过来
        /// @param conn client跟master的TCP连接
        /// @param frame 提交job的消息
        void onSubmit(ConnState& conn, const xmr::protocol::Frame& frame);

        /// @brief client把Job插件传给了master
        /// @param conn client跟master通信用的TCP连接
        /// @param frame 消息
        void onPluginChunk(ConnState& conn, const xmr::protocol::Frame& frame);
        // worker跟master要Job插件 master用TCP回传给worker
        void onPullPlugin(ConnState& conn, const xmr::protocol::Frame& frame);
        // worker加载插件的结果
        void onPluginAck(ConnState& conn, const xmr::protocol::Frame& frame);
        // worker跟master建立连接
        void onHello(ConnState& conn, const xmr::protocol::Frame& frame);
        // worker把自己的数据面监听端口报给master
        void onDataAddress(ConnState& conn, const xmr::protocol::Frame& frame);
        // client把切好的block清单报给master 请master分配副本位置
        void onBlockManifest(ConnState& conn, const xmr::protocol::Frame& frame);
        // client把一个DFS路径对应的block清单提交给master 建立命名空间
        void onCommitFile(ConnState& conn, const xmr::protocol::Frame& frame);
        // worker上报自己本地持有的block清单
        void onBlockReport(ConnState& conn, const xmr::protocol::Frame& frame);
        // worker告诉master自己在等待任务
        void onRequestTask(ConnState& conn, const xmr::protocol::Frame& frame);
        // 任务成功
        void onDone(ConnState& conn, const xmr::protocol::Frame& frame);
        // 任务失败
        void onFail(ConnState& conn, const xmr::protocol::Frame& frame);
        // reduce结果
        void onReduceResult(const xmr::protocol::Frame& frame);
        // 心跳
        void onPing(ConnState& conn, const xmr::protocol::Frame& frame);

        void startJob(ConnId client, const xmr::protocol::Submit& submit,
                      std::vector<std::vector<std::string> > mapBlocks);

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

        // 数据本地性: 在持有该map任务输入block的空闲worker里挑一个 缺本地返回空
        std::optional<std::string> assignLocal(const xmr::Task& task, TimePoint now);

        // 拼reduce任务要用的拉取计划 mapTask,host,port
        std::vector<std::string> fetchPlan() const;

        // 有block清单且worker就绪时 给client分配副本位置并记录locator
        void tryPlaceBlocks();

        // 把每个输入文件的block位置(含副本)拼成派给map任务的形式
        std::vector<std::vector<std::string> > buildMapBlocks() const;

        // worker是否在线且没有被判定下线
        bool isLive(const std::string& workerId) const;

        // 忘掉某个worker持有的block 并清掉它的进行中复制
        void forgetWorker(const std::string& workerId);

        // 副本不足的block从存活副本重复制到其它在线worker
        void reReplicate();

        void requeueTask(const std::string& workerId);

        // worker失效:重发它手里的任务 并作废它名下已完成的map(输出丢了)
        void recoverWorker(const std::string& workerId);

        void retryOrFail(const xmr::Task& task);

        void requestShutdown();

        void fail(std::string reason);

        EventLoop* loop_ = nullptr;
        // worker管理器
        xmr::WorkerRegistry registry_;
        // master控制端口/数据端口的连接表
        ConnectionTable connections_;
        // block副本放置与复制
        BlockPlacement blocks_;
        // map任务执行的中间结果在哪个worker上 k=任务id v=worker的id
        std::unordered_map<std::size_t, std::string> mapOwner_;

        // 正在等插件字节的提交 client + 元数据
        struct Pending {
            ConnId client = 0;
            xmr::protocol::Submit submit;
            std::uint32_t requestId = 0;
        };

        // 每个输入文件的blockId列表 提交时由DFS命名空间解析得到
        std::vector<std::vector<std::string> > pendingInputBlocks_;
        // DFS命名空间: 路径 -> 按顺序的blockId列表(仅内存,不持久化)
        std::unordered_map<std::string, std::vector<std::string> > namespace_;
        PluginDistributor plugins_;
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
}  // namespace xmr::master
