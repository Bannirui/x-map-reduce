#pragma once

#include"net/buffer.h"

#include<cstddef>
#include<cstdint>
#include<string>
#include<vector>

enum class IoStatus {
    Ok,
    WouldBlock,
    Closed,
};

enum class SocketOption {
    TcpNoDelay,
    KeepAlive,
    ReuseAddress,
    ReusePort,
    ReceiveBuffer,
    SendBuffer,
};

void setSocketOption(int fd, SocketOption option, int value);

/// @brief 设置socket非阻塞
/// @param fd 哪个socket
void setNonBlocking(int fd);

void setBlocking(int fd);

/// @param fd 代表TCP连接的本端socket
/// @param buffer 把TCP传过来的数据收到buffer里面
IoStatus recvInto(int fd, ByteBuffer& buffer);

IoStatus sendFrom(int fd, const void* data, std::size_t size, std::size_t& sent);

/**
 * @param fd 发送给谁
 * @param data 发送的数据
 * @param size 发送的这个数据多大
 */
void sendAll(int fd, const void* data, std::size_t size);

// TCP连接
class Connection {
public:
    Connection() = default;

    explicit Connection(int fd);

    ~Connection();

    Connection(Connection&& other);

    Connection& operator=(Connection&& other);

    Connection(const Connection&) = delete;

    Connection& operator=(const Connection&) = delete;

    int fd() const noexcept {
        return fd_;
    }

    // 交出fd的所有权 之后Connection不再负责close它
    int release() noexcept {
        const int value = fd_;
        fd_ = -1;
        return value;
    }

private:
    /**
     * 代表TCP连接的那个socket 这个fd是本端的那个socket
     * TCP连接的是双端
     * 在服务端 这个fd就是服务端的socket 它的对端就是客户端
     * 在客户端 这个fd就是客户端的socket 它的对端就是服务端
     */
    int fd_{-1};
};

// 服务端
class Listener {
public:
    // 监听端口 backlog<0时用系统默认(SOMAXCONN)
    Listener(const std::string& host, std::uint16_t port, int backlog = -1);

    ~Listener();

    Listener(Listener&& other);

    Listener& operator=(Listener&& other);

    Listener(const Listener&) = delete;

    Listener& operator=(const Listener&) = delete;

    std::uint16_t port() const noexcept {
        return port_;
    }

    int fd() const noexcept {
        return fd_;
    }

    // 从listen系统调用指定的backlog全连接队列掏一个对端连接出来
    Connection accept() const;

    // 全连接队列空时 不要阻塞在那儿
    IoStatus acceptNonBlocking(Connection& out) const;

private:
    // 监听的socket
    int fd_{-1};
    // 监听在哪个端口上
    std::uint16_t port_{0};
};

// Connect to host:port, blocking until connected. Throws std::runtime_error on
// failure.
Connection connectTo(const std::string& host, std::uint16_t port);
