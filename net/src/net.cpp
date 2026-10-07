#include"net/net.h"

#include<cerrno>
#include<cstring>
#include<stdexcept>
#include<string>
#include<utility>

#include<arpa/inet.h>
#include<netdb.h>
#include<netinet/in.h>
#include<sys/socket.h>
#include<unistd.h>

namespace xmr::net {
    namespace {
        std::runtime_error systemError(const std::string& what) {
            return std::runtime_error(what + ": " + std::strerror(errno));
        }

        // RAII for a raw fd used only during setup, before it is handed to a Connection.
        struct ScopedFd {
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

        // Resolve host:port into a connect/bind result list. `passive` selects the
        // bind (server) flavour; `host` empty means "any local address".
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

    void recvAll(int fd, void* data, std::size_t size) {
        auto* bytes = static_cast<std::uint8_t*>(data);
        while (size > 0) {
            const ssize_t received = ::recv(fd, bytes, size, 0);
            if (received < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw systemError("recv failed");
            }
            if (received == 0) {
                throw std::runtime_error("connection closed while receiving");
            }
            bytes += received;
            size -= static_cast<std::size_t>(received);
        }
    }

    void sendMessage(int fd, const void* data, std::size_t size) {
        if (size > kMaxMessageBytes) {
            throw std::runtime_error("message exceeds kMaxMessageBytes");
        }
        const std::uint32_t prefix = htonl(static_cast<std::uint32_t>(size));
        sendAll(fd, &prefix, sizeof(prefix));
        if (size > 0) {
            sendAll(fd, data, size);
        }
    }

    void sendMessage(int fd, const std::vector<std::uint8_t>& message) {
        sendMessage(fd, message.data(), message.size());
    }

    std::vector<std::uint8_t> receiveMessage(int fd) {
        // 消息协议 [4字节大小][实际数据]
        std::uint32_t prefix = 0;
        recvAll(fd, &prefix, sizeof(prefix));
        // 网络大端序转主机小端序
        const std::uint32_t size = ntohl(prefix);
        if (size > kMaxMessageBytes) {
            throw std::runtime_error("message exceeds kMaxMessageBytes");
        }
        std::vector<std::uint8_t> message(size);
        if (size > 0) {
            recvAll(fd, message.data(), size);
        }
        return message;
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

    void Connection::send(const void* data, std::size_t size) const {
        sendMessage(fd_, data, size);
    }

    void Connection::send(const std::vector<std::uint8_t>& message) const {
        sendMessage(fd_, message);
    }

    std::vector<std::uint8_t> Connection::receive() const {
        return receiveMessage(fd_);
    }

    Listener::Listener(const std::string& host, std::uint16_t port) {
        AddrInfo results = resolve(host, port,/*passive=*/true);
        for (addrinfo* entry = results.info; entry != nullptr; entry = entry->ai_next) {
            ScopedFd candidate(::socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol));
            if (candidate.fd < 0) {
                continue;
            }
            int reuse = 1;
            ::setsockopt(candidate.fd,SOL_SOCKET,SO_REUSEADDR, &reuse, sizeof(reuse));
            if (::bind(candidate.fd, entry->ai_addr, entry->ai_addrlen) != 0) {
                continue;
            }
            if (::listen(candidate.fd,SOMAXCONN) != 0) {
                continue;
            }
            port_ = boundPort(candidate.fd);
            fd_ = candidate.release();
            return;
        }
        throw std::runtime_error("failed to bind listener on " + host + ":" + std::to_string(port));
    }

    Listener::~Listener() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    Listener::Listener(Listener&& other) noexcept : fd_(other.fd_), port_(other.port_) {
        other.fd_ = -1;
        other.port_ = 0;
    }

    Listener& Listener::operator=(Listener&& other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) {
                ::close(fd_);
            }
            fd_ = other.fd_;
            port_ = other.port_;
            other.fd_ = -1;
            other.port_ = 0;
        }
        return *this;
    }

    Connection Listener::accept() const {
        while (true) {
            const int client = ::accept(fd_, nullptr, nullptr);
            if (client >= 0) {
                return Connection(client);
            }
            if (errno != EINTR) {
                throw systemError("accept failed");
            }
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
} // namespace xmr::net