#include"xmr/client/client.h"

#include"net/bootstrap.h"
#include"net/channel_handler.h"
#include"net/channel_handler_context.h"
#include"net/channel_pipeline.h"
#include"protocol/frame_codec.h"
#include"protocol/messages.h"

#include<algorithm>
#include<any>
#include<fstream>
#include<functional>
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

        class InboxHandler : public ChannelInboundHandler {
        public:
            InboxHandler(std::function<void(protocol::Frame)> onFrame, std::function<void()> onDisconnected)
                : onFrame_(std::move(onFrame)), onDisconnected_(std::move(onDisconnected)) {
            }

            void channelRead(ChannelHandlerContext& ctx, std::any& message) override {
                protocol::Frame* frame = std::any_cast<protocol::Frame>(&message);
                if (frame != nullptr) {
                    onFrame_(std::move(*frame));
                }
            }

            void channelInactive(ChannelHandlerContext& ctx) override {
                onDisconnected_();
            }

        private:
            std::function<void(protocol::Frame)> onFrame_;
            std::function<void()> onDisconnected_;
        };
    } // namespace

    Client::Client(const std::string& endpoint) : group_(1) {
        const auto [host,port] = parseEndpoint(endpoint);
        group_.start();
        ClientBootstrap bootstrap;
        bootstrap.group(group_).handler([this](Channel& channel) {
            channel.pipeline().addLast(std::make_shared<protocol::FrameEncoder>());
            channel.pipeline().addLast(std::make_shared<protocol::FrameDecoder>());
            channel.pipeline().addLast(std::make_shared<InboxHandler>(
                [this](protocol::Frame frame) { onFrame(std::move(frame)); },
                [this] { onDisconnected(); }));
        });
        control_ = bootstrap.connect(host, port);
    }

    Client::~Client() {
        if (control_) {
            control_->close();
        }
        group_.stop();
    }

    void Client::onFrame(protocol::Frame frame) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            inbox_.push_back(std::move(frame));
        }
        received_.notify_one();
    }

    void Client::onDisconnected() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            disconnected_ = true;
        }
        received_.notify_all();
    }

    void Client::send(protocol::MessageType type, std::uint32_t requestId,
                      const std::vector<std::uint8_t>& body, std::uint16_t flags) {
        protocol::Frame frame;
        frame.header.type = type;
        frame.header.flags = flags;
        frame.header.requestId = requestId;
        frame.body = body;
        control_->write(std::any(std::move(frame)));
    }

    protocol::Frame Client::receive() {
        std::unique_lock<std::mutex> lock(mutex_);
        received_.wait(lock, [this] { return !inbox_.empty() || disconnected_; });
        if (inbox_.empty()) {
            throw std::runtime_error("master disconnected");
        }
        protocol::Frame frame = std::move(inbox_.front());
        inbox_.pop_front();
        return frame;
    }

    bool Client::submit(const SubmitRequest& request, std::string& reason) {
        // 读本地输入文件
        std::vector<std::vector<std::uint8_t> > inputs;
        inputs.reserve(request.inputs.size());
        for (const auto& path : request.inputs) {
            std::ifstream in(path, std::ios::binary);
            if (!in) {
                reason = "failed to open input: " + path;
                return false;
            }
            inputs.emplace_back(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }
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

        // 输入和插件都走数据面单独连接上传
        ClientBootstrap dataBootstrap;
        dataBootstrap.group(group_).handler([](Channel& channel) {
            channel.pipeline().addLast(std::make_shared<protocol::FrameEncoder>());
        });
        std::shared_ptr<Channel> data = dataBootstrap.connect(ack.dataHost, static_cast<std::uint16_t>(ack.dataPort));
        for (std::size_t index = 0; index < inputs.size(); ++index) {
            const std::vector<std::uint8_t>& blob = inputs[index];
            std::size_t offset = 0;
            do {
                const std::size_t n = std::min<std::size_t>(protocol::kChunkBytes, blob.size() - offset);
                protocol::InputBlob chunk;
                chunk.index = index;
                chunk.offset = offset;
                chunk.payload.assign(blob.begin() + static_cast<std::ptrdiff_t>(offset),
                                     blob.begin() + static_cast<std::ptrdiff_t>(offset + n));
                const bool more = offset + n < blob.size();
                protocol::Frame frame;
                frame.header.type = protocol::MessageType::InputBlob;
                frame.header.flags = more ? static_cast<std::uint16_t>(protocol::Flag::More) : 0;
                frame.body = chunk.encode();
                data->write(std::any(std::move(frame)));
                offset += n;
            } while (offset < blob.size());
        }
        if (!plugin.empty()) {
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
                protocol::Frame frame;
                frame.header.type = protocol::MessageType::Plugin;
                frame.header.flags = more ? static_cast<std::uint16_t>(protocol::Flag::More) : 0;
                frame.body = chunk.encode();
                data->write(std::any(std::move(frame)));
                offset += n;
            } while (offset < plugin.size());
        }
        // 数据面写完后延迟关闭 排空出站缓冲再真正关掉
        data->close();
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
