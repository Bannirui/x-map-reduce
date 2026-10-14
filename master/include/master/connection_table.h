#pragma once

#include "master/types.h"
#include "protocol/framing.h"
#include "protocol/protocol.h"

#include <xnet/channel.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace xmr::master {
    // 对连接再包一层
    struct ConnState {
        // worker连接进来master的编号 master可以知道是哪个worker
        ConnId id = 0;
        // worker的ip master不负责数据 只管理元数据 将来shuffle的数据是worker跟worker传 所以master要保存worker的ip
        std::string host;
        // coordinator线程写 用于回填派发时的请求id
        std::string workerId;
        std::uint32_t lastRequest = 0;
        // 这个worker已经加载过的插件哈希
        std::unordered_set<std::string> plugins;
        // 数据连接上client上传插件的接收缓冲 coordinator线程独占
        std::vector<std::uint8_t> uploadBuffer;
        // 这条连接对应的channel 发送时直接用
        std::shared_ptr<Channel> channel;
    };

    /// @param fd TCP连接 本质是TCP连接在本端的那个socket 它就代表了TCP
    /// @return TCP连接对端的ip
    std::string peerHost(int fd);

    class ConnectionTable {
    public:
        void add(std::shared_ptr<ConnState> conn);

        std::shared_ptr<ConnState> get(ConnId id) const;

        void remove(ConnId id);

        void bindWorker(const std::string& workerId, ConnId id);

        void unbindWorker(const std::string& workerId);

        std::optional<ConnId> connOf(const std::string& workerId) const;

        bool hasWorker(const std::string& workerId) const;

        std::size_t workerCount() const;

        const std::unordered_map<std::string, ConnId>& workers() const;

        void send(ConnId id, xmr::protocol::MessageType type, std::uint32_t requestId,
                  const std::vector<std::uint8_t>& body, std::uint16_t flags = 0);

        template <class Message>
        void send(ConnId id, std::uint32_t requestId, const Message& message, std::uint16_t flags = 0) {
            send(id, Message::kType, requestId, message.encode(), flags);
        }

        void sendBlob(ConnId id, std::uint32_t requestId, const std::string& blob);

        std::unordered_map<ConnId, std::shared_ptr<ConnState> >& all();

    private:
        std::unordered_map<ConnId, std::shared_ptr<ConnState> > conns_;
        std::unordered_map<std::string, ConnId> byWorker_;
    };
}  // namespace xmr::master
