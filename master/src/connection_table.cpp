#include "master/connection_table.h"

#include "protocol/messages.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <any>
#include <cstddef>
#include <utility>

namespace xmr::master {
    std::string peerHost(int fd) {
        sockaddr_storage address{};
        socklen_t length = sizeof(address);
        if (::getpeername(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
            return {};
        }
        char buffer[INET6_ADDRSTRLEN] = {0};
        if (address.ss_family == AF_INET) {
            ::inet_ntop(AF_INET, &reinterpret_cast<sockaddr_in*>(&address)->sin_addr, buffer, sizeof(buffer));
        } else if (address.ss_family == AF_INET6) {
            ::inet_ntop(AF_INET6, &reinterpret_cast<sockaddr_in6*>(&address)->sin6_addr, buffer, sizeof(buffer));
        }
        return buffer;
    }

    void ConnectionTable::add(std::shared_ptr<ConnState> conn) {
        conns_[conn->id] = std::move(conn);
    }

    std::shared_ptr<ConnState> ConnectionTable::get(ConnId id) const {
        const auto it = conns_.find(id);
        return it == conns_.end() ? nullptr : it->second;
    }

    void ConnectionTable::remove(ConnId id) {
        conns_.erase(id);
    }

    void ConnectionTable::bindWorker(const std::string& workerId, ConnId id) {
        byWorker_[workerId] = id;
    }

    void ConnectionTable::unbindWorker(const std::string& workerId) {
        byWorker_.erase(workerId);
    }

    std::optional<ConnId> ConnectionTable::connOf(const std::string& workerId) const {
        const auto it = byWorker_.find(workerId);
        if (it == byWorker_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    bool ConnectionTable::hasWorker(const std::string& workerId) const {
        return byWorker_.find(workerId) != byWorker_.end();
    }

    std::size_t ConnectionTable::workerCount() const {
        return byWorker_.size();
    }

    const std::unordered_map<std::string, ConnId>& ConnectionTable::workers() const {
        return byWorker_;
    }

    void ConnectionTable::send(ConnId id, xmr::protocol::MessageType type, std::uint32_t requestId,
                               const std::vector<std::uint8_t>& body, std::uint16_t flags) {
        // 找到控制端口的TCP
        const auto it = conns_.find(id);
        if (it == conns_.end()) {
            return;
        }
        xmr::protocol::Frame frame;
        frame.header.type = type;
        frame.header.flags = flags;
        frame.header.requestId = requestId;
        frame.body = body;
        // 丢到它自己的reactor线程 编码后发送到TCP对端
        it->second->channel->write(std::any(std::move(frame)));
    }

    // 大blob分块发送 最后一块不带More
    void ConnectionTable::sendBlob(ConnId id, std::uint32_t requestId, const std::string& blob) {
        const std::size_t total = blob.size();
        std::size_t offset = 0;
        do {
            const std::size_t n = std::min<std::size_t>(xmr::protocol::kChunkBytes, total - offset);
            xmr::protocol::Chunk data;
            data.offset = offset;
            data.total = total;
            data.payload.assign(blob.begin() + static_cast<std::ptrdiff_t>(offset),
                                blob.begin() + static_cast<std::ptrdiff_t>(offset + n));
            const bool more = offset + n < total;
            send(id, requestId, data, more ? static_cast<std::uint16_t>(xmr::protocol::Flag::More) : 0);
            offset += n;
        } while (offset < total);
    }

    std::unordered_map<ConnId, std::shared_ptr<ConnState> >& ConnectionTable::all() {
        return conns_;
    }
}  // namespace xmr::master
