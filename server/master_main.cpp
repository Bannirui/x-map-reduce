#include "mapreduce/mapreduce.h"
#include "master/coordinator.h"
#include "master/master_handlers.h"
#include "protocol/frame_codec.h"
#include "protocol/framing.h"
#include "protocol/messages.h"
#include "runtime/endpoint.h"
#include "runtime/exit_code.h"
#include "runtime/scheduler.h"
#include "runtime/task.h"
#include "runtime/task_codec.h"
#include "runtime/worker_registry.h"

#include <xnet/bootstrap.h>
#include <xnet/channel.h>
#include <xnet/channel_handler.h>
#include <xnet/channel_handler_context.h>
#include <xnet/channel_pipeline.h>
#include <xnet/event_loop.h>
#include <xnet/event_loop_group.h>
#include <xnet/net.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <fstream>
#include <future>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {
    // 启动服务端需要指定参数 提示参数
    void usage(const char* program) {
        std::cerr << "Usage: " << program << " --listen <port> [--data-listen <port>]\n";
    }
}  // namespace

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
        const auto [host, port] = xmr::parseHostPort(listen, "127.0.0.1");

        xmr::master::Coordinator coordinator;
        // 独占的线程 只负责跑master自己
        EventLoop coordinatorLoop;
        // 负责master接收新来的TCP连接的主reactor 可能是连接的控制端口 也可能是连接的数据端口
        EventLoopGroup acceptGroup(1);
        // 负责读写的从reactor
        EventLoopGroup ioGroup(xmr::master::kIoThreads);
        // master给worker连接进来的TCP编号
        std::atomic<xmr::master::ConnId> nextConnId{1};

        std::promise<void> donePromise;
        std::future<void> doneFuture = donePromise.get_future();
        coordinator.setOnShutdown([&donePromise] {
            donePromise.set_value();
        });
        coordinator.setLoop(&coordinatorLoop);

        std::thread coordinatorThread([&] {
            coordinatorLoop.run();
        });
        acceptGroup.start();
        ioGroup.start();

        // master启动起来
        coordinatorLoop.runInLoop([&] {
            coordinator.start();
        });

        // 监听在控制端口上
        ServerBootstrap controlServer;
        controlServer.group(acceptGroup, ioGroup)
            .childHandler([&](Channel& channel) {
                // 控制端口上TCP的读写处理
                auto conn = std::make_shared<xmr::master::ConnState>();
                conn->id = nextConnId.fetch_add(1);
                conn->host = xmr::master::peerHost(channel.fd());
                conn->channel = channel.shared_from_this();
                // 连接断开的时候要处理
                channel.pipeline().addLast(
                    std::make_shared<xmr::master::ConnectionLifecycleHandler>(conn, &coordinator, &coordinatorLoop));
                channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameEncoder>());
                channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameDecoder>());
                channel.pipeline().addLast(
                    std::make_shared<xmr::master::MasterConnHandler>(conn, &coordinator, &coordinatorLoop));
                coordinatorLoop.runInLoop([&coordinator, conn] {
                    coordinator.onConnect(conn);
                });
            })
            .bind(host, port);
        std::cout << "LISTENING " << host << ":" << controlServer.port() << std::endl;

        // 数据面端口 worker拉map的input/插件 client上传插件
        std::string dataHost = host;
        std::uint16_t dataPort = xmr::master::kDefaultDataPort;
        if (!dataListen.empty()) {
            const auto [dh, dp] = xmr::parseHostPort(dataListen, host);
            dataHost = dh;
            dataPort = dp;
        }
        ServerBootstrap dataServer;
        dataServer.group(acceptGroup, ioGroup)
            .childHandler([&](Channel& channel) {
                // 数据端口上TCP的读写处理
                auto conn = std::make_shared<xmr::master::ConnState>();
                conn->id = nextConnId.fetch_add(1);
                conn->host = xmr::master::peerHost(channel.fd());
                conn->channel = channel.shared_from_this();
                // 连接断开的时候要处理
                channel.pipeline().addLast(
                    std::make_shared<xmr::master::ConnectionLifecycleHandler>(conn, &coordinator, &coordinatorLoop));
                channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameEncoder>());
                channel.pipeline().addLast(std::make_shared<xmr::protocol::FrameDecoder>());
                channel.pipeline().addLast(
                    std::make_shared<xmr::master::MasterConnHandler>(conn, &coordinator, &coordinatorLoop));
                coordinatorLoop.runInLoop([&coordinator, conn] {
                    coordinator.onConnect(conn);
                });
            })
            .bind(dataHost, dataPort);
        std::cout << "DATA_LISTENING " << dataHost << ":" << dataServer.port() << std::endl;

        coordinatorLoop.runInLoop([&coordinator, dataHost, port = dataServer.port()] {
            coordinator.setDataAddress(dataHost, port);
        });

        doneFuture.wait();

        controlServer.close();
        dataServer.close();
        coordinatorLoop.stop();
        ioGroup.stop();
        acceptGroup.stop();
        coordinatorThread.join();

        // master的关机流程 停掉循环后 直接用阻塞IO给每条连接发Shutdown 等对端关掉
        for (auto& [id, conn] : coordinator.conns()) {
            try {
                // 关机消息转换成帧字节流
                const auto frame = xmr::protocol::makeFrame(xmr::protocol::MessageType::Shutdown, 0,
                                                            xmr::protocol::Shutdown{}.encode());
                const int fd = conn->channel->fd();
                setBlocking(fd);
                // 用TCP发出去
                sendAll(fd, frame.data(), frame.size());
                ::shutdown(fd, SHUT_WR);
                char buffer[256];
                while (::read(fd, buffer, sizeof(buffer)) > 0) {
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
