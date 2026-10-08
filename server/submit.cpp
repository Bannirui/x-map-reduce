#include"net/net.h"
#include"protocol/framing.h"
#include"protocol/messages.h"
#include"runtime/exit_code.h"

#include<cstdint>
#include<iostream>
#include<stdexcept>
#include<string>
#include<utility>
#include<vector>

#include<unistd.h>

namespace {
    std::pair<std::string, std::string> parseEndpoint(const std::string& endpoint) {
        const auto colon = endpoint.rfind(':');
        if (colon == std::string::npos) {
            throw std::runtime_error("expected host:port in '" + endpoint + "'");
        }
        return {endpoint.substr(0, colon), endpoint.substr(colon + 1)};
    }

    void usage(const char* program) {
        std::cerr << "Usage: " << program
            << " --master <host:port> --job <name> [--reducers <R>] [--workers <N>]"
            << " --output <file> <input...>\n";
    }
} // namespace

int main(int argc, char** argv) {
    std::string master;
    std::string job;
    std::string output;
    std::size_t reducers = 1;
    std::size_t workers = 1;
    std::vector<std::string> inputs;
    bool shutdownAfter = false;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--master" && i + 1 < argc) {
            master = argv[++i];
        } else if (arg == "--job" && i + 1 < argc) {
            job = argv[++i];
        } else if (arg == "--reducers" && i + 1 < argc) {
            reducers = std::stoul(argv[++i]);
        } else if (arg == "--workers" && i + 1 < argc) {
            workers = std::stoul(argv[++i]);
        } else if (arg == "--output" && i + 1 < argc) {
            output = argv[++i];
        } else if (arg == "--shutdown") {
            shutdownAfter = true;
        } else {
            inputs.emplace_back(arg);
        }
    }

    try {
        if (master.empty() || job.empty() || output.empty() || inputs.empty()) {
            throw UsageError("invalid or missing arguments");
        }
        const auto [host,portText] = parseEndpoint(master);
        const std::uint16_t port = static_cast<std::uint16_t>(std::stoul(portText));
        xmr::net::Connection connection = xmr::net::connectTo(host, port);

        xmr::protocol::Submit submit;
        submit.job = job;
        submit.reducers = reducers;
        submit.workers = workers;
        submit.output = output;
        submit.inputs = inputs;
        const auto request = xmr::protocol::makeFrame(xmr::protocol::MessageType::Submit, 1, submit.encode());
        xmr::net::sendAll(connection.fd(), request.data(), request.size());

        xmr::net::ByteBuffer in;
        int exitCode = static_cast<int>(ExitCode::Failure);
        bool done = false;
        while (!done) {
            if (xmr::net::recvInto(connection.fd(), in) == xmr::net::IoStatus::Closed) {
                std::cerr << "submit: master disconnected\n";
                break;
            }
            xmr::protocol::FrameDecoder decoder(in);
            while (auto frame = decoder.next()) {
                if (frame->header.type == xmr::protocol::MessageType::SubmitAck) {
                    const auto ack = xmr::protocol::SubmitAck::decode(frame->body);
                    if (ack.statusCode != xmr::protocol::StatusCode::Ok) {
                        std::cerr << "submit: rejected: " << ack.reason << '\n';
                        done = true;
                        break;
                    }
                    std::cout << "submitted '" << job << "'" << std::endl;
                } else if (frame->header.type == xmr::protocol::MessageType::SubmitResult) {
                    const auto result = xmr::protocol::SubmitResult::decode(frame->body);
                    if (result.statusCode == xmr::protocol::StatusCode::Ok) {
                        std::cout << "result: " << result.output << std::endl;
                        exitCode = static_cast<int>(ExitCode::Success);
                    } else {
                        std::cerr << "submit: job failed: " << result.reason << '\n';
                    }
                    done = true;
                    break;
                }
            }
        }

        if (shutdownAfter) {
            const auto shutdown = xmr::protocol::makeFrame(
                xmr::protocol::MessageType::Shutdown, 0, xmr::protocol::Shutdown{}.encode());
            xmr::net::sendAll(connection.fd(), shutdown.data(), shutdown.size());
        }
        return exitCode;
    } catch (const UsageError& error) {
        std::cerr << "submit: " << error.what() << '\n';
        usage(argv[0]);
        return static_cast<int>(ExitCode::Usage);
    } catch (const std::exception& error) {
        std::cerr << "submit: " << error.what() << '\n';
        return static_cast<int>(ExitCode::Failure);
    }
}
