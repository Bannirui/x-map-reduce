#pragma once

#include<cstddef>
#include<cstdint>
#include<string>
#include<vector>

/**
 * 消息编码分2层
 * 第1层 net分帧 [4字节大端长度N][N字节payload]
 * 第2层 payload文本协议 字段用\t分隔 第一个字段是命令名
 *
 *   方向	        消息类型	字段
 * worker→master	HELLO	无
 * worker→master	REQUEST	无
 * worker→master	DONE	KIND、id
 * worker→master	FAIL	KIND、id、reason
 * worker→master	MAPOUT	mapTask、partition、blob
 * worker→master	FETCH	mapTask、partition
 * worker→master	INPUT	mapTaskId
 * worker→master	RESULT	reduceId、blob
 * master→worker	TASK	序列化后的Task
 * master→worker	DATA	对INPUT/FETCH的回复
 * master→worker	STOP	无
 *
 * 交互时序
 *  worker                         master
 *  |---- HELLO ------------------->|   注册（当前什么都不做）
 *  ---- REQUEST ----------------->|   worker.idle = true
 *  <--- TASK\tMAP\t0\t... --------|   dispatch(): 取任务、回发、idle=false
 *    (执行)
 *  ---- DONE\tMAP\t0 ------------>|   scheduler.markDone()
 *  ---- REQUEST ----------------->|   idle = true
 *  <--- TASK\t... ----------------|
 *  ..                              ...
 *  <--- STOP ---------------------|   任务全完成，关停
 */
namespace xmr::net {
    // net层只搬运消息 约定消息协议 [4字节放数据长度][该长度字节的payload] 但是我们协议约定消息大小上限制是64MB
    inline constexpr std::uint32_t kMaxMessageBytes = 64u * 1024u * 1024u;

    /**
     * @param fd 发送给谁
     * @param data 发送的数据
     * @param size 发送的这个数据多大
     */
    void sendAll(int fd, const void* data, std::size_t size);

    /**
     * @param data 缓冲区用来收数据
     * @param size 要收数据的大小
     */
    void recvAll(int fd, void* data, std::size_t size);

    /**
     * @param fd 发给谁
     * @param data 要发送的数据
     * @param size 要发送的数据多大 把它按照协议编码 [数据大小][数据]
     */
    void sendMessage(int fd, const void* data, std::size_t size);

    void sendMessage(int fd, const std::vector<std::uint8_t>& message);

    std::vector<std::uint8_t> receiveMessage(int fd);

    /**
     * TCP连接
     * 只感知纯文本协议
     * 协议格式是 字段\t数据
     * 第1个字段是命令名
     */
    class Connection {
    public:
        Connection() = default;

        explicit Connection(int fd);

        ~Connection();

        Connection(Connection&& other);

        Connection& operator=(Connection&& other);

        Connection(const Connection&) = delete;

        Connection& operator=(const Connection&) = delete;

        bool valid() const noexcept {
            return fd_ >= 0;
        }

        int fd() const noexcept {
            return fd_;
        }

        void send(const void* data, std::size_t size) const;

        void send(const std::vector<std::uint8_t>& message) const;

        // 接收到的数据
        std::vector<std::uint8_t> receive() const;

    private:
        int fd_{-1};
    };

    class Listener {
    public:
        Listener(const std::string& host, std::uint16_t port);

        ~Listener();

        Listener(Listener&& other) noexcept;

        Listener& operator=(Listener&& other) noexcept;

        Listener(const Listener&) = delete;

        Listener& operator=(const Listener&) = delete;

        std::uint16_t port() const noexcept {
            return port_;
        }

        int fd() const noexcept {
            return fd_;
        }

        // Block until a client connects and return the connected socket.
        Connection accept() const;

    private:
        // master监听的socket
        int fd_{-1};
        std::uint16_t port_{0};
    };

    // Connect to host:port, blocking until connected. Throws std::runtime_error on
    // failure.
    Connection connectTo(const std::string& host, std::uint16_t port);
} // namespace xmr::net