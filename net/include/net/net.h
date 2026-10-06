#pragma once

#include<cstddef>
#include<cstdint>
#include<string>
#include<vector>

// Standalone TCP primitives for the V4 network layer. This module deliberately
// knows nothing about MapReduce: it just moves length-prefixed messages over a
// socket, so it can be unit-tested without a coordinator or a worker.
namespace xmr::net {

// Framing: every message is a 4-byte big-endian length followed by that many
// payload bytes. This lets callers receive whole messages instead of guessing
// how a stream of bytes splits into records. The cap guards against corrupt or
// hostile length prefixes.
inline constexpr std::uint32_t kMaxMessageBytes=64u*1024u*1024u;

// Write exactly `size` bytes on `fd`, retrying on EINTR. Throws
// std::runtime_error on failure. Uses MSG_NOSIGNAL so a closed peer does not
// terminate the process.
void sendAll(int fd,const void* data,std::size_t size);

// Read exactly `size` bytes from `fd`. Throws std::runtime_error on failure or
// if the peer closes the connection before `size` bytes arrive.
void recvAll(int fd,void* data,std::size_t size);

// Send one length-prefixed message.
void sendMessage(int fd,const void* data,std::size_t size);
void sendMessage(int fd,const std::vector<std::uint8_t>& message);

// Receive exactly one length-prefixed message.
std::vector<std::uint8_t> receiveMessage(int fd);

// Move-only owner of a connected socket; closes it on destruction.
class Connection {
public:
    Connection()=default;
    explicit Connection(int fd) noexcept;
    ~Connection();

    Connection(Connection&& other) noexcept;
    Connection& operator=(Connection&& other) noexcept;
    Connection(const Connection&)=delete;
    Connection& operator=(const Connection&)=delete;

    [[nodiscard]] bool valid() const noexcept{ return fd_>=0; }
    [[nodiscard]] int fd() const noexcept{ return fd_; }

    void send(const void* data,std::size_t size) const;
    void send(const std::vector<std::uint8_t>& message) const;
    std::vector<std::uint8_t> receive() const;

private:
    int fd_{-1};
};

// Listening socket bound to a host:port. Binding to port 0 picks an ephemeral
// port; port() then reports the real one, which makes tests self-contained.
class Listener {
public:
    Listener(const std::string& host,std::uint16_t port);
    ~Listener();

    Listener(Listener&& other) noexcept;
    Listener& operator=(Listener&& other) noexcept;
    Listener(const Listener&)=delete;
    Listener& operator=(const Listener&)=delete;

    [[nodiscard]] std::uint16_t port() const noexcept{ return port_; }
    [[nodiscard]] int fd() const noexcept{ return fd_; }

    // Block until a client connects and return the connected socket.
    Connection accept() const;

private:
    int fd_{-1};
    std::uint16_t port_{0};
};

// Connect to host:port, blocking until connected. Throws std::runtime_error on
// failure.
Connection connectTo(const std::string& host,std::uint16_t port);

}  // namespace xmr::net
