#include"xmr/client/client.h"

#include"protocol/framing.h"
#include"protocol/messages.h"

#include<algorithm>
#include<fstream>
#include<iterator>
#include<stdexcept>
#include<utility>

namespace xmr::client {
    namespace {
        std::pair<std::string, std::uint16_t> parseEndpoint(const std::string& endpoint) {
            const auto colon = endpoint.rfind(':');
            if (colon == std::string::npos) {
                throw std::runtime_error("expected host:port in '" + endpoint + "'");
            }
            return {endpoint.substr(0, colon),
                    static_cast<std::uint16_t>(std::stoul(endpoint.substr(colon + 1)))};
        }
    } // namespace

    Client::Client(const std::string& endpoint) {
        const auto [host,port] = parseEndpoint(endpoint);
        connection_ = net::connectTo(host, port);
    }

    void Client::send(protocol::MessageType type, std::uint32_t requestId,
                      const std::vector<std::uint8_t>& body, std::uint16_t flags) {
        const auto frame = protocol::makeFrame(type, requestId, body, flags);
        net::sendAll(connection_.fd(), frame.data(), frame.size());
    }

    protocol::Frame Client::receive() {
        while (true) {
            protocol::FrameDecoder decoder(in_);
            if (auto frame = decoder.next()) {
                return std::move(*frame);
            }
            if (net::recvInto(connection_.fd(), in_) == net::IoStatus::Closed) {
                throw std::runtime_error("master disconnected");
            }
        }
    }

    bool Client::submit(const SubmitRequest& request, std::string& reason) {
        // 读本地插件 算出内容哈希
        std::vector<std::uint8_t> plugin;
        if (!request.pluginPath.empty()) {
            std::ifstream in(request.pluginPath, std::ios::binary);
            if (!in) {
                reason = "failed to open plugin: " + request.pluginPath;
                return false;
            }
            plugin.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }

        protocol::Submit submit;
        submit.job = request.job;
        submit.reducers = request.reducers;
        submit.output = request.output;
        submit.inputs = request.inputs;
        if (!plugin.empty()) {
            submit.pluginHash = protocol::contentHash(plugin);
        }
        send(protocol::MessageType::Submit, 1, submit.encode());

        // 等master受理 它会把数据面地址带回来
        protocol::SubmitAck ack;
        while (true) {
            const protocol::Frame frame = receive();
            if (frame.header.type == protocol::MessageType::SubmitAck) {
                ack = protocol::SubmitAck::decode(frame.body);
                break;
            }
            if (frame.header.type == protocol::MessageType::SubmitResult) {
                const auto result = protocol::SubmitResult::decode(frame.body);
                reason = result.reason;
                return result.statusCode == protocol::StatusCode::Ok;
            }
        }
        if (ack.statusCode != protocol::StatusCode::Ok) {
            reason = ack.reason;
            return false;
        }

        // 插件走数据面单独连接上传
        if (!plugin.empty()) {
            net::Connection data = net::connectTo(ack.dataHost,
                                                  static_cast<std::uint16_t>(ack.dataPort));
            std::size_t offset = 0;
            do {
                const std::size_t n = std::min<std::size_t>(protocol::kChunkBytes, plugin.size() - offset);
                protocol::Plugin chunk;
                chunk.hash = submit.pluginHash;
                chunk.job = request.job;
                chunk.offset = offset;
                chunk.payload.assign(plugin.begin() + static_cast<std::ptrdiff_t>(offset),
                                     plugin.begin() + static_cast<std::ptrdiff_t>(offset + n));
                const bool more = offset + n < plugin.size();
                const auto frame = protocol::makeFrame(protocol::MessageType::Plugin, 0, chunk.encode(),
                                                       more ? static_cast<std::uint16_t>(protocol::Flag::More) : 0);
                net::sendAll(data.fd(), frame.data(), frame.size());
                offset += n;
            } while (offset < plugin.size());
        }
        return true;
    }

    SubmitResult Client::wait() {
        while (true) {
            const protocol::Frame frame = receive();
            if (frame.header.type == protocol::MessageType::SubmitResult) {
                const auto result = protocol::SubmitResult::decode(frame.body);
                return SubmitResult{result.statusCode, result.output, result.reason};
            }
        }
    }

    void Client::shutdown() {
        send(protocol::MessageType::Shutdown, 0, protocol::Shutdown{}.encode());
    }
} // namespace xmr::client
