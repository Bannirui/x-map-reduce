#include"net/net.h"

#include<cerrno>
#include<cstring>
#include<stdexcept>
#include<string>
#include<utility>

#include<arpa/inet.h>
#include<fcntl.h>
#include<netdb.h>
#include<netinet/in.h>
#include<netinet/tcp.h>
#include<sys/socket.h>
#include<unistd.h>

namespace {
    std::runtime_error systemError(const std::string& what) {
        return std::runtime_error(what + ": " + std::strerror(errno));
    }

    // fd资源用对象生命周期管理
    struct ScopedFd {
        // -1是哨兵值 资源被转移后要置成-1
        int fd = -1;

        ScopedFd() = default;

        explicit ScopedFd(int value) : fd(value) {
        }

        ~ScopedFd() {
            if (fd >= 0) {
                ::close(fd);
            }
        }

        ScopedFd(const ScopedFd&) = delete;

        ScopedFd& operator=(const ScopedFd&) = delete;

        ScopedFd(ScopedFd&& other) noexcept : fd(other.fd) {
            other.fd = -1;
        }

        ScopedFd& operator=(ScopedFd&& other) noexcept {
            if (this != &other) {
                if (fd >= 0) {
                    ::close(fd);
                }
                fd = other.fd;
                other.fd = -1;
            }
            return *this;
        }

        // 转移资源
        int release() {
            const int value = fd;
            fd = -1;
            return value;
        }
    };

    // RAII for getaddrinfo's result list.
    struct AddrInfo {
        addrinfo* info = nullptr;

        ~AddrInfo() {
            if (info != nullptr) {
                ::freeaddrinfo(info);
            }
        }
    };

    /**
     * @param host 为空就是监听所有网卡
     * @param port socket的端口
     * @param passive 控制socket的模式 给客户端用还是服务端用
     *                true-被动socket 给服务端用
     *                false-主动socket 给客户端用
     */
    AddrInfo resolve(const std::string& host, std::uint16_t port, bool passive) {
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        if (passive) {
            hints.ai_flags = AI_PASSIVE;
        }
        const std::string portText = std::to_string(port);
        addrinfo* results = nullptr;
        const int rc = ::getaddrinfo(host.empty() ? nullptr : host.c_str(), portText.c_str(), &hints, &results);
        if (rc != 0) {
            throw std::runtime_error(std::string("getaddrinfo: ") + ::gai_strerror(rc));
        }
        return AddrInfo{results};
    }

    /// @brief 可能创建socket的时候没有指定端口 那么系统会随机指派 现在返回过来拿着socket去确认它是哪个端口
    /// @param fd 哪个socket
    /// @return socket的真实端口
    std::uint16_t boundPort(int fd) {
        sockaddr_storage address{};
        socklen_t length = sizeof(address);
        if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
            return 0;
        }
        if (address.ss_family == AF_INET) {
            return ntohs(reinterpret_cast<sockaddr_in*>(&address)->sin_port);
        }
        if (address.ss_family == AF_INET6) {
            return ntohs(reinterpret_cast<sockaddr_in6*>(&address)->sin6_port);
        }
        return 0;
    }
} // namespace

void setNonBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        throw systemError("fcntl(F_GETFL) failed");
    }
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw systemError("fcntl(F_SETFL) failed");
    }
}

void setBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        throw systemError("fcntl(F_GETFL) failed");
    }
    if (::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) < 0) {
        throw systemError("fcntl(F_SETFL) failed");
    }
}

void setSocketOption(int fd, SocketOption option, int value) {
    int level = SOL_SOCKET;
    int name = 0;
    switch (option) {
        case SocketOption::TcpNoDelay:
            level = IPPROTO_TCP;
            name = TCP_NODELAY;
            break;
        case SocketOption::KeepAlive:
            name = SO_KEEPALIVE;
            break;
        case SocketOption::ReuseAddress:
            name = SO_REUSEADDR;
            break;
        case SocketOption::ReusePort:
            name = SO_REUSEPORT;
            break;
        case SocketOption::ReceiveBuffer:
            name = SO_RCVBUF;
            break;
        case SocketOption::SendBuffer:
            name = SO_SNDBUF;
            break;
    }
    if (::setsockopt(fd, level, name, &value, sizeof(value)) != 0) {
        throw systemError("setsockopt failed");
    }
}

IoStatus recvInto(int fd, ByteBuffer& buffer) {
    std::uint8_t chunk[65536];
    while (true) {
        // 把TCP传过来的数据收进来放到buffer里面
        const ssize_t received = ::recv(fd, chunk, sizeof(chunk), 0);
        if (received > 0) {
            buffer.append(chunk, static_cast<std::size_t>(received));
            return IoStatus::Ok;
        }
        if (received == 0) {
            return IoStatus::Closed;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return IoStatus::WouldBlock;
        }
        if (errno == ECONNRESET || errno == EPIPE) {
            return IoStatus::Closed;
        }
        throw systemError("recv failed");
    }
}

/// @param fd 代表了TCP 它是本端的socket 用TCP跟对端socket连接了
/// @param data 要发送的数据 缓冲区
/// @param size 有多少数据要发送出去的
/// @param sent 实际发送出去了多少
IoStatus sendFrom(int fd, const void* data, std::size_t size, std::size_t& sent) {
    while (true) {
        // 用TCP给对端发数据
        const ssize_t written = ::send(fd, data, size, MSG_NOSIGNAL);
        if (written > 0) {
            sent = static_cast<std::size_t>(written);
            return IoStatus::Ok;
        }
        if (written == 0) {
            sent = 0;
            return IoStatus::Closed;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return IoStatus::WouldBlock;
        }
        if (errno == EPIPE || errno == ECONNRESET) {
            return IoStatus::Closed;
        }
        throw systemError("send failed");
    }
}

void sendAll(int fd, const void* data, std::size_t size) {
    // 要发送的数据缓冲
    const auto* bytes = static_cast<const std::uint8_t*>(data);
    while (size > 0) {
        const ssize_t written = ::send(fd, bytes, size,MSG_NOSIGNAL);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw systemError("send failed");
        }
        if (written == 0) {
            throw std::runtime_error("send returned 0");
        }
        bytes += written;
        size -= static_cast<std::size_t>(written);
    }
}

Connection::Connection(int fd) : fd_(fd) {
}

Connection::~Connection() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Connection::Connection(Connection&& other) : fd_(other.fd_) {
    other.fd_ = -1;
}

Connection& Connection::operator=(Connection&& other) {
    if (this != &other) {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

    Listener::Listener(const std::string& host, std::uint16_t port, int backlog) {
    // 要创建被动socket给服务端用
    AddrInfo results = resolve(host, port, true);
    for (addrinfo* entry = results.info; entry != nullptr; entry = entry->ai_next) {
        ScopedFd candidate(::socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol));
        if (candidate.fd < 0) {
            continue;
        }
        int reuse = 1;
        ::setsockopt(candidate.fd, SOL_SOCKET,SO_REUSEADDR, &reuse, sizeof(reuse));
        if (::bind(candidate.fd, entry->ai_addr, entry->ai_addrlen) != 0) {
            continue;
        }
            if (::listen(candidate.fd, backlog < 0 ? SOMAXCONN : backlog) != 0) {
            continue;
        }
        // 防止创建socket的时候没有指定端口 用的是系统随机分配的 拿到真正监听在哪个端口上
        port_ = boundPort(candidate.fd);
        fd_ = candidate.release();
        return;
    }
    throw std::runtime_error("failed to bind listener on " + host + ":" + std::to_string(port));
}

Listener::~Listener() {
    // 关闭socket
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Listener::Listener(Listener&& other): fd_(other.fd_), port_(other.port_) {
    // 释放右值资源
    other.fd_ = -1;
    other.port_ = 0;
}

Listener& Listener::operator=(Listener&& other) {
    if (this != &other) {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = other.fd_;
        port_ = other.port_;
        // 释放右值资源
        other.fd_ = -1;
        other.port_ = 0;
    }
    return *this;
}

Connection Listener::accept() const {
    while (true) {
        // 从服务端的全连接队列拿出来的这个socket此时已经是TCP的一端了 它的另一端就是客户端的socket 它已经代表了TCP
        const int client = ::accept(fd_, nullptr, nullptr);
        if (client >= 0) {
            // TCP封装起来
            return Connection(client);
        }
        if (errno != EINTR) {
            throw systemError("accept failed");
        }
    }
}

IoStatus Listener::acceptNonBlocking(Connection& out) const {
    while (true) {
        const int client = ::accept(fd_, nullptr, nullptr);
        if (client >= 0) {
            out = Connection(client);
            return IoStatus::Ok;
        }
        if (errno == EINTR) {
            continue;
        }
        // 全连接队列为空时会返回EAGAIN
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return IoStatus::WouldBlock;
        }
        throw systemError("accept failed");
    }
}

/**
 * 建立TCP连接 要连谁
 */
Connection connectTo(const std::string& host, std::uint16_t port) {
    AddrInfo results = resolve(host, port,/*passive=*/false);
    for (addrinfo* entry = results.info; entry != nullptr; entry = entry->ai_next) {
        ScopedFd candidate(::socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol));
        if (candidate.fd < 0) {
            continue;
        }
        if (::connect(candidate.fd, entry->ai_addr, entry->ai_addrlen) == 0) {
            return Connection(candidate.release());
        }
    }
    throw std::runtime_error("failed to connect to " + host + ":" + std::to_string(port));
}
