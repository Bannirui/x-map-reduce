#include "worker/partition_puller.h"

#include "protocol/frame_codec.h"
#include "protocol/framing.h"
#include "protocol/messages.h"
#include "runtime/task.h"

#include <xnet/net.h>

#include <stdexcept>

namespace xmr::worker {
    // "mapTask,host,port" -> Location
    Location parseLocation(const std::string& text) {
        Location location;
        const auto first = text.find(',');
        const auto second = text.find(',', first == std::string::npos ? 0 : first + 1);
        if (first == std::string::npos || second == std::string::npos) {
            throw std::runtime_error("bad location '" + text + "'");
        }
        location.mapTask = static_cast<std::size_t>(std::stoull(text.substr(0, first)));
        location.host = text.substr(first + 1, second - first - 1);
        location.port = static_cast<std::uint16_t>(std::stoul(text.substr(second + 1)));
        return location;
    }

    // 向某个地址发一个请求 流式收Data直到最后一块 返回blob 阻塞调用 只在线程池里跑
    std::string pullStream(const std::string& host, std::uint16_t port, protocol::MessageType type,
                           const std::vector<std::uint8_t>& body) {
        Connection connection = connectTo(host, port);
        const auto request = protocol::makeFrame(type, 1, body);
        sendAll(connection.fd(), request.data(), request.size());

        ByteBuffer in;
        std::string blob;
        while (true) {
            if (recvInto(connection.fd(), in) == IoStatus::Closed) {
                return blob;
            }
            protocol::FrameParser decoder(in);
            while (auto frame = decoder.next()) {
                if (frame->header.type != protocol::MessageType::Chunk) {
                    continue;
                }
                const auto data = protocol::Chunk::decode(frame->body);
                blob.append(data.payload.begin(), data.payload.end());
                if ((frame->header.flags & static_cast<std::uint16_t>(protocol::Flag::More)) == 0) {
                    return blob;
                }
            }
        }
    }

    // 从另一个worker拉某个map任务的分区结果
    std::vector<KeyValue> pullPartition(const std::string& host, std::uint16_t port, std::size_t mapTask,
                                        std::size_t partition) {
        protocol::Pull pull;
        pull.mapTask = mapTask;
        pull.partition = partition;
        return deserializeKeyValues(pullStream(host, port, protocol::MessageType::Pull, pull.encode()));
    }
}  // namespace xmr::worker
