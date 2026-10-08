#pragma once

#include"net/buffer.h"

#include<cstddef>
#include<cstdint>
#include<string>
#include<vector>

namespace xmr::net {
    enum class IoStatus {
        Ok,
        WouldBlock,
        Closed,
    };

    void setNonBlocking(int fd);

    void setBlocking(int fd);

    IoStatus recvInto(int fd, ByteBuffer& buffer);

    IoStatus sendFrom(int fd, const void* data, std::size_t size, std::size_t& sent);

    /**
     * @param fd 发送给谁
     * @param data 发送的数据
     * @param size 发送的这个数据多大
     */
    void sendAll(int fd, const void* data, std::size_t size);

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

        int fd() const noexcept {
            return fd_;
        }

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

        IoStatus acceptNonBlocking(Connection& out) const;

    private:
        // master监听的socket
        int fd_{-1};
        std::uint16_t port_{0};
    };

    // Connect to host:port, blocking until connected. Throws std::runtime_error on
    // failure.
    Connection connectTo(const std::string& host, std::uint16_t port);
} // namespace xmr::net