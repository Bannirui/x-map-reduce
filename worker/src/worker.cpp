#include "worker/worker.h"

#include "dfs/block_server.h"
#include "dfs/block_store.h"
#include "handler/worker_control_handler.h"
#include "handler/worker_data_handler.h"
#include "protocol/frame_codec.h"
#include "protocol/messages.h"
#include "runtime/exit_code.h"
#include "runtime/task.h"
#include "runtime/task_codec.h"
#include "worker/block_reader.h"
#include "worker/partition_puller.h"
#include "worker/plugin_fetcher.h"

#include <xnet/bootstrap.h>
#include <xnet/channel.h>
#include <xnet/channel_handler.h>
#include <xnet/channel_pipeline.h>
#include <xnet/event_loop.h>
#include <xnet/event_loop_group.h>
#include <xnet/net.h>
#include <xnet/thread_pool.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

namespace xmr::worker {
    namespace {
        // worker上报心跳的周期
        constexpr std::chrono::seconds kHeartbeatInterval{2};
        constexpr std::chrono::seconds kBlockReportInterval{5};
        // 业务计算的线程池大小
        constexpr std::size_t kComputeThreads{4};

        enum class Stage {
            // worker空闲着 可以跟master要任务
            RequestTask,
            // 已经跟master要过任务了 正在等master给自己派发任务
            WaitingTask,
            // 收到了master派发的任务 正在线程池里面处理着
            Computing,
        };

        std::vector<std::uint8_t> toBytes(const std::string& text) {
            return std::vector<std::uint8_t>(text.begin(), text.end());
        }

        class Worker {
        public:
            /// @param masterHost master的控制端口ip
            /// @param masterPort master的控制端口port
            /// @param pluginCache 路径 缓存Job插件
            /// @param blockDir 落盘Block的路径
            Worker(std::string masterCtlHost, std::uint16_t masterCtlPort, std::string pluginCacheDir,
                   std::string blockDir)
                : masterCtlHost_(std::move(masterCtlHost)),
                  masterCtlPort_(masterCtlPort),
                  pluginCacheDir_(std::move(pluginCacheDir)),
                  blockDir_(std::move(blockDir)) {}

            int run() {
                controlGroup_.start();
                dataGroup_.start();

                // 客户端
                ClientBootstrap client;
                // 跟master连接的TCP
                control_ = client.group(controlGroup_)
                               .handler([this](Channel& channel) {
                                   channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameEncoder>());
                                   channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameDecoder>());
                                   // 解码器结果
                                   channel.pipeline().addLast(std::make_shared<WorkerControlHandler>(
                                       [this](const xmr::protocol::Frame& frame) {
                                           // 收到了master发来的消息
                                           onFrame(frame);
                                       },
                                       [this] {
                                           onControlInactive();
                                       }));
                               })
                               .connect(masterCtlHost_, masterCtlPort_);
                // 负责TCP读写的线程
                loop_ = control_->loop();
                // 其他worker来拉reduce分区的数据 map阶段的中间结果
                dataServer_.group(dataGroup_).childHandler([this](Channel& channel) {
                    channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameEncoder>());
                    channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameDecoder>());
                    // 其他worker要什么数据 整理出来交给encoder
                    channel.pipeline().addLast(std::make_shared<WorkerDataHandler>(&mapStore_, &mapStoreMutex_));
                });
                dataServer_.bind("0.0.0.0", 0);

                blockStore_ = std::make_unique<BlockStore>(blockDir_);
                blockServer_ = std::make_unique<BlockServer>(*blockStore_);
                blockPort_ = blockServer_->start("0.0.0.0", 0);

                // 周期给master上报心跳
                loop_->addInterval(kHeartbeatInterval, [this] {
                    sendPing();
                });
                // 定时给master上报自己的block清单
                loop_->addInterval(kBlockReportInterval, [this] {
                    sendBlockReport();
                });

                // 跟master建连
                xmr::protocol::Hello hello;
                hello.workerId = "worker-" + std::to_string(::getpid());
                hello.pid = static_cast<std::uint64_t>(::getpid());
                // 给master发个单向消息
                sendFrame(0, hello);

                // 上报自己的数据面监听端口
                xmr::protocol::DataAddress dataAddress;
                // map中间结果走的数据端口
                dataAddress.port = dataServer_.port();
                // dfs block端口
                dataAddress.blockPort = blockPort_;
                // 给master发个单向消息
                sendFrame(0, dataAddress);

                sendBlockReport();
                // worker已经初始化好了 去跟master要任务
                requestTaskIfIdle();

                // 卡住主线程
                shutdown_.get_future().wait();

                blockServer_->stop();
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

            /// @brief 给master发送控制消息
            /// @param type 消息类型
            /// @param rid request id 0表示单向消息
            void sendFrame(xmr::protocol::MessageType type, std::uint32_t rid, const std::vector<std::uint8_t>& body,
                           std::uint16_t flags = 0) {
                xmr::protocol::Frame frame;
                frame.header.type = type;
                frame.header.flags = flags;
                // 0表示单向消息
                frame.header.requestId = rid;
                frame.body = body;
                control_->write(std::any(std::move(frame)));
            }

            /// @brief 给master发送控制消息
            /// @tparam Message 消息类型
            /// @param rid request id 0表示单向消息
            template <class Message>
            void sendFrame(std::uint32_t rid, const Message& message, std::uint16_t flags = 0) {
                sendFrame(Message::kType, rid, message.encode(), flags);
            }

            // worker要是空闲就去跟master要任务
            void requestTaskIfIdle() {
                if (stage_ == Stage::RequestTask) {
                    // worker空闲着 可以跟master要任务
                    sendFrame(++requestId_, xmr::protocol::RequestTask{});
                    // 推进状态
                    stage_ = Stage::WaitingTask;
                }
            }

            void sendPing() {
                xmr::protocol::Ping ping;
                ping.nonce = ++heartbeatNonce_;
                sendFrame(0, ping);
            }

            void sendResult(const xmr::Task& task, const std::vector<KeyValue>& result) {
                xmr::protocol::ReduceResult message;
                message.reduceTask = task.id;
                message.offset = 0;
                message.payload = toBytes(xmr::serializeKeyValues(result));
                sendFrame(0, message);
            }

            void failTask(const xmr::Task& task, const std::string& reason) {
                xmr::protocol::Fail fail;
                fail.kind = xmr::wireKind(task.kind);
                fail.taskId = task.id;
                fail.statusCode = xmr::protocol::StatusCode::Internal;
                fail.reason = reason;
                sendFrame(0, fail);
            }

            void sendDone(const xmr::Task& task) {
                xmr::protocol::Done done;
                done.kind = xmr::wireKind(task.kind);
                done.taskId = task.id;
                sendFrame(0, done);
            }

            void sendProgress(const xmr::Task& task, double fraction) {
                xmr::protocol::Progress progress;
                progress.kind = xmr::wireKind(task.kind);
                progress.taskId = task.id;
                progress.fraction = static_cast<std::uint64_t>(fraction * 100.0);
                sendFrame(0, progress);
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

            // 处理控制端口TCP断掉的情况
            void onControlInactive() {
                if (!shutdownDone_.load()) {
                    error_ = std::make_exception_ptr(std::runtime_error("master disconnected"));
                    signalShutdown();
                }
            }

            // master给worker发消息了
            void onFrame(const xmr::protocol::Frame& frame) {
                try {
                    if (frame.header.type == xmr::protocol::MessageType::Pong) {
                        // master对心跳的应答 不需要处理
                        return;
                    }
                    if (frame.header.type == xmr::protocol::MessageType::Shutdown) {
                        // master要关停了 worker退出循环
                        signalShutdown();
                        return;
                    }
                    if (frame.header.type == xmr::protocol::MessageType::MasterData) {
                        // master数据面端口
                        masterDataPort_ =
                            static_cast<std::uint16_t>(xmr::protocol::MasterData::decode(frame.body).port);
                        return;
                    }
                    if (frame.header.type == xmr::protocol::MessageType::TaskAssignment) {
                        // worker收到master派发的任务
                        const auto message = xmr::protocol::TaskAssignment::decode(frame.body);
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
                    if (frame.header.type == xmr::protocol::MessageType::Cancel) {
                        // master取消了当前任务的重复attempt 算完丢弃结果
                        const auto cancel = xmr::protocol::Cancel::decode(frame.body);
                        if (computing_ && computing_->kind == xmr::taskKind(cancel.kind) &&
                            computing_->id == cancel.taskId) {
                            cancelled_ = true;
                        }
                        return;
                    }
                    if (frame.header.type == xmr::protocol::MessageType::FetchPlugin) {
                        // master告诉worker去拉插件
                        needPlugin(xmr::protocol::FetchPlugin::decode(frame.body));
                        return;
                    }
                    if (frame.header.type == xmr::protocol::MessageType::JobFinished) {
                        // master告诉worker本job结束了 复位状态准备接下一个job
                        resetJob();
                        requestTaskIfIdle();
                        return;
                    }
                    if (frame.header.type == xmr::protocol::MessageType::ReplicateBlock) {
                        replicateBlock(xmr::protocol::ReplicateBlock::decode(frame.body));
                        return;
                    }
                    if (frame.header.type == xmr::protocol::MessageType::DeleteBlock) {
                        const auto request = xmr::protocol::DeleteBlock::decode(frame.body);
                        if (blockStore_) {
                            blockStore_->remove(request.blockId);
                        }
                        return;
                    }
                    throw std::runtime_error("unexpected master message");
                } catch (...) {
                    error_ = std::current_exception();
                    signalShutdown();
                }
            }

            void startMap(const xmr::Task& task) {
                const std::uint64_t job = epoch_;
                computing_ = task;
                cancelled_ = false;
                pool_.submit([this, task, job] {
                    std::vector<std::vector<KeyValue> > parts;
                    try {
                        const std::string content = readBlocks(*blockStore_, task.blocks);
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
                            fetched[location.mapTask] =
                                pullPartition(location.host, location.port, location.mapTask, task.id);
                            ++pulled;
                            const double fraction =
                                task.maps == 0 ? 1.0 : static_cast<double>(pulled) / static_cast<double>(task.maps);
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

            // 给master上报本地持有的block清单 master据此重建locator
            void sendBlockReport() {
                xmr::protocol::BlockReport report;
                if (blockStore_) {
                    report.blocks = blockStore_->list();
                }
                sendFrame(0, report);
            }

            // master让从别的副本复制一个block过来 复制完再上报一次清单
            void replicateBlock(const xmr::protocol::ReplicateBlock& request) {
                const std::string blockId = request.blockId;
                const std::string host = request.sourceHost;
                const auto port = static_cast<std::uint16_t>(request.sourcePort);
                pool_.submit([this, blockId, host, port] {
                    if (blockStore_) {
                        copyBlock(*blockStore_, blockId, host, port);
                    }
                    post([this] {
                        sendBlockReport();
                    });
                });
            }

            // master让去拉插件 线程池里从master数据面拉下来落盘dlopen 再ack
            void needPlugin(const xmr::protocol::FetchPlugin& need) {
                const std::string dataHost = masterCtlHost_;
                const std::uint16_t dataPort = masterDataPort_;
                const std::string cacheDir = pluginCacheDir_;
                pool_.submit([this, need, dataHost, dataPort, cacheDir] {
                    std::string reason;
                    const bool ok = fetchPlugin(dataHost, dataPort, cacheDir, need.hash, need.job, reason);
                    post([this, need, ok, reason] {
                        xmr::protocol::PluginAck ack;
                        ack.hash = need.hash;
                        ack.ok = ok;
                        ack.reason = reason;
                        sendFrame(0, ack);
                    });
                });
            }

            // 负责连接master的控制端口
            std::string masterCtlHost_;
            std::uint16_t masterCtlPort_;
            // 缓存Job插件的路径
            std::string pluginCacheDir_;
            // 缓存Block的路径
            std::string blockDir_;

            // 本worker产出map阶段的中间结果 每个分区的序列化blob 供别的worker拉
            std::unordered_map<std::size_t, std::vector<std::string> > mapStore_;
            std::mutex mapStoreMutex_;

            // 给master发送的消息编号 用来跟收到master消息进行关联 request&&response
            std::uint32_t requestId_ = 0;
            // worker的状态机
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

            // 负责跟master控制端口通信的TCP连接
            std::shared_ptr<Channel> control_;
            // 负责跟master控制端口通信的TCP连接绑定的线程
            EventLoop* loop_ = nullptr;

            /**
             * worker上开放数据端口
                 - master给worker传job插件
             *   - worker-worker传map output
             */
            ServerBootstrap dataServer_;
            // dfs block服务端口
            std::unique_ptr<BlockStore> blockStore_;
            std::unique_ptr<BlockServer> blockServer_;
            std::uint16_t blockPort_ = 0;
            std::promise<void> shutdown_;
            std::atomic<bool> shutdownDone_{false};
            std::exception_ptr error_;

            ThreadPool pool_{kComputeThreads};
        };
    }  // namespace

    int runWorker(WorkerConfig config) {
        Worker worker(std::move(config.masterCtlHost), config.masterCtlPort, std::move(config.pluginCacheDir),
                      std::move(config.blockDir));
        return worker.run();
    }
}  // namespace xmr::worker
