xnet
---

一个基于 epoll + reactor 的网络库，模仿 Netty 的线程模型。

平台：仅 Linux（epoll）。不做跨平台（kqueue / IOCP 不在范围内）。

当前在 x-map-reduce 仓库内开发，作为独立库的雏形；API 稳定后会用
`git subtree split -P net` 拆到独立仓库。

## 1 定位与边界

`net/` 是纯叶子库，**只依赖 `Threads::Threads`**：

- 不认识 MapReduce，也不认识 XMRP 协议。
- 对外只暴露字节流与通用框架（Channel/Pipeline/Codec/Bootstrap）。
- 具体协议的消息结构（`Header`/`MessageType`/TLV/`Frame`）留在应用的
  `protocol/`，是库中通用 codec 的配置，不进库。

判断标准：某个类型换个项目还能用 -> 进 `net/`；只有 XMRP 才懂 -> 留在
`protocol/`。

命名约定：整个 `net/` 库不使用 namespace，所有类型与函数直接放全局作用域
（`ByteBuffer`/`EventLoop`/`Connection`/`Channel` 等）。

## 2 现有结构

| 文件 | 职责 |
|------|------|
| `include/net/buffer.h` | `ByteBuffer`，仿 Netty ByteBuf 的读写游标缓冲 |
| `include/net/net.h` | socket 原语：`setNonBlocking`/`recvInto`/`sendFrom`/`sendAll`、`Connection`、`Listener`、`connectTo` |
| `include/net/notifier.h` | eventfd 唤醒，供 `EventLoop` 跨线程提交任务用 |
| `include/net/timer.h` | `TimerQueue`，小根堆定时任务队列（一次性/周期性，惰性取消） |
| `include/net/event_loop.h` | `Poller`（epoll 封装）+ `EventLoop`（Reactor 线程：IO 事件 + 定时器 + 跨线程任务队列） |
| `include/net/event_loop_group.h` | `EventLoopGroup`，仿 Netty 的 boss/worker 线程组 |
| `include/net/thread_pool.h` | `ThreadPool`，把耗时业务移出 reactor 线程 |
| `include/net/channel_handler.h` | `ChannelHandler` 基类 + `ChannelInboundHandler` / `ChannelOutboundHandler` |
| `include/net/channel_handler_context.h` | `ChannelHandlerContext`，逐级传播+回写 |
| `include/net/channel_pipeline.h` | `ChannelPipeline`，逐级触发 handler 链 |
| `include/net/channel.h` | `Channel`，连接：fd + EventLoop + 读写缓冲 + 管道 + 写水位 |
| `include/net/idle_state_handler.h` | `IdleStateHandler`，读写空闲检测（`userEventTriggered`） |
| `include/net/bootstrap.h` | `ServerBootstrap` / `ClientBootstrap`，用户层入口（含 `option`/`childOption`/`backlog`）|
| `include/net/codec/byte_to_message_decoder.h` | 累积字节、循环 `decode` 的入站解码基类 |
| `include/net/codec/length_field_frame_decoder.h` | `LengthFieldBasedFrameDecoder`，长度域拆包 |
| `include/net/codec/delimiter_based_frame_decoder.h` | `DelimiterBasedFrameDecoder`，分隔符拆包 |
| `include/net/codec/message_to_byte_encoder.h` | `MessageToByteEncoder<T>`，出站 `T -> ByteBuffer` |

管道消息是 `std::any`（对应 Netty 的 `Object`）：字节层是 `ByteBuffer`，解码器可产出
任意类型 `T` 向下游传播，编码器把 `T` 还原成 `ByteBuffer`。

## 3 构建

`net/` 既可作独立顶层项目，也可被父仓库 `add_subdirectory` 引入。

独立构建（产物 `libxnet.a`，测试在 `net/tests/`）：

```sh
cmake -B build -S net
cmake --build build -j
ctest --test-dir build
```

安装后别的工程可 `find_package(xnet)` + `target_link_libraries(app PRIVATE xnet::xnet)`：

```sh
cmake --install build --prefix /your/prefix
```

## 4 已实现能力

transport 原语：非阻塞 socket、`ByteBuffer`、`Poller`(epoll)、`EventLoop`、
`EventLoopGroup`、`TimerQueue`、`ThreadPool`、`Notifier`。

框架层（仿 Netty）：

- `Channel` / `ChannelPipeline` / `ChannelHandlerContext`，inbound + outbound
  双向 handler 链；出站从 tail 向 head、入站从 head 向 tail，未 override 的
  出站 handler 默认透传。
- 写缓冲水位（backpressure）、空闲检测（`IdleStateHandler`）、延迟关闭与半关闭、
  fd 生命周期与事件移除竞态收口。
- `ServerBootstrap` / `ClientBootstrap` 入口，支持 `option` / `childOption` / `backlog`。

codec：`ByteToMessageDecoder`、`MessageToByteEncoder<T>`、
`LengthFieldBasedFrameDecoder`、`DelimiterBasedFrameDecoder`。

回归测试见 `tests/`（channel/pipeline/outbound/bootstrap/codec/idle/watermark/
half-close/option 等），分布式端到端由 `tests/control_plane_test.cpp` 覆盖。

## 5 最小使用示例

```cpp
#include "net/bootstrap.h"
#include "net/channel.h"
#include "net/channel_handler.h"
#include "net/channel_handler_context.h"
#include "net/channel_pipeline.h"
#include "net/event_loop_group.h"

#include <any>
#include <memory>

// 回显：把收到的字节帧原样写回
class Echo : public ChannelInboundHandler {
public:
    void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
        ctx.write(message);
    }
};

int main() {
    EventLoopGroup group(4);
    group.start();

    ServerBootstrap server;
    server.group(group)
          .childOption(SocketOption::TcpNoDelay, 1)
          .childHandler([](Channel& channel) {
              // 有具体协议时，解码器产出 T、编码器把 T 还原成 ByteBuffer
              channel.pipeline().addLast(std::make_shared<Echo>());
          });
    server.bind("0.0.0.0", 9000);
    // 运行中；关闭时 server.close() + group.stop()
}
```

## 6 独立化计划

1. [完成] 在 `net/` 内原地开发框架层，用 `server/` 的 master/worker 当第一个消费者验证。
2. [完成] target 改名 `xnet`（alias `xnet::xnet`）；命名空间已去，`protocol/` 的前向声明
   已同步为全局 `class ByteBuffer;`。
3. [完成] 独立 CMake 骨架 + `install()`/`export()`，`find_package(xnet)` 已验证；测试迁到
   `net/tests/`。
4. [待做] `git subtree split -P net` 拆库并 push 到独立 GitHub 仓库；MapReduce 侧改用
   submodule 或 `FetchContent` 引用。
