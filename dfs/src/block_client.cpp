#include "dfs/block_client.h"

#include "dfs/dfs_messages.h"

#include <xnet/buffer.h>
#include <xnet/net.h>

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

BlockClient::BlockClient(std::string host, std::uint16_t port) : host_(std::move(host)), port_(port) {}

bool BlockClient::writeBlock(const std::string& blockId, const std::vector<std::uint8_t>& data) {
    Connection connection = connectTo(host_, port_);

    std::size_t offset = 0;
    do {
        const std::size_t n = std::min<std::size_t>(kDfsChunkBytes, data.size() - offset);
        WriteBlock chunk;
        chunk.blockId = blockId;
        chunk.offset = offset;
        chunk.payload.assign(data.begin() + static_cast<std::ptrdiff_t>(offset),
                             data.begin() + static_cast<std::ptrdiff_t>(offset + n));
        const bool more = offset + n < data.size();
        const std::vector<std::uint8_t> frame = makeDfsFrame(WriteBlock::kType, 1, chunk.encode(),
                                                             more ? static_cast<std::uint16_t>(DfsFlag::More) : 0);
        sendAll(connection.fd(), frame.data(), frame.size());
        offset += n;
    } while (offset < data.size());

    ByteBuffer in;
    while (true) {
        if (recvInto(connection.fd(), in) == IoStatus::Closed) {
            return false;
        }
        DfsFrameParser parser(in);
        while (auto frame = parser.next()) {
            if (frame->header.type == DfsMessageType::WriteAck) {
                return WriteAck::decode(frame->body).ok;
            }
        }
    }
}

std::optional<std::vector<std::uint8_t>> BlockClient::readBlock(const std::string& blockId) {
    Connection connection = connectTo(host_, port_);

    ReadBlock request;
    request.blockId = blockId;
    request.offset = 0;
    const std::vector<std::uint8_t> frame = makeDfsFrame(ReadBlock::kType, 1, request.encode());
    sendAll(connection.fd(), frame.data(), frame.size());

    std::vector<std::uint8_t> result;
    ByteBuffer in;
    while (true) {
        if (recvInto(connection.fd(), in) == IoStatus::Closed) {
            return std::nullopt;
        }
        DfsFrameParser parser(in);
        while (auto message = parser.next()) {
            if (message->header.type != DfsMessageType::BlockData) {
                continue;
            }
            const BlockData data = BlockData::decode(message->body);
            if (!data.found) {
                return std::nullopt;
            }
            result.insert(result.end(), data.payload.begin(), data.payload.end());
            if ((message->header.flags & static_cast<std::uint16_t>(DfsFlag::More)) == 0) {
                return result;
            }
        }
    }
}
