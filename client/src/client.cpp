#include"xmr/client/client.h"

#include"protocol/framing.h"
#include"protocol/messages.h"

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
                      const std::vector<std::uint8_t>& body) {
        const auto frame = protocol::makeFrame(type, requestId, body);
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
        protocol::Submit submit;
        submit.job = request.job;
        submit.reducers = request.reducers;
        submit.workers = request.workers;
        submit.output = request.output;
        submit.inputs = request.inputs;
        send(protocol::MessageType::Submit, 1, submit.encode());

        while (true) {
            const protocol::Frame frame = receive();
            if (frame.header.type == protocol::MessageType::SubmitAck) {
                const auto ack = protocol::SubmitAck::decode(frame.body);
                reason = ack.reason;
                return ack.statusCode == protocol::StatusCode::Ok;
            }
            if (frame.header.type == protocol::MessageType::SubmitResult) {
                const auto result = protocol::SubmitResult::decode(frame.body);
                reason = result.reason;
                return result.statusCode == protocol::StatusCode::Ok;
            }
        }
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
