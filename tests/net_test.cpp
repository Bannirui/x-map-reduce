#include "net/buffer.h"
#include "net/net.h"

#include <atomic>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

std::atomic<int> failures{0};

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

std::string bufferString(const xmr::net::ByteBuffer& buffer) {
    if (buffer.empty()) {
        return {};
    }
    return std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size());
}

}  // namespace

int main() {
    using namespace xmr::net;

    try {
        // Bind to port 0 so the test never collides with another process.
        Listener listener("127.0.0.1", 0);
        const std::uint16_t port = listener.port();
        check(port != 0, "listener should report a real port when bound to 0");

        const std::string request = "hello mapreduce";
        const std::string reply(200000, 'x');

        std::thread server([&] {
            try {
                Connection connection = listener.accept();
                ByteBuffer inbound;
                while (inbound.size() < request.size()) {
                    if (recvInto(connection.fd(), inbound) == IoStatus::Closed) {
                        break;
                    }
                }
                check(bufferString(inbound) == request, "server should receive the request");
                sendAll(connection.fd(), reply.data(), reply.size());
            } catch (const std::exception& error) {
                std::cerr << "server error: " << error.what() << '\n';
                ++failures;
            }
        });

        Connection client = connectTo("127.0.0.1", port);
        sendAll(client.fd(), request.data(), request.size());

        ByteBuffer inbound;
        while (inbound.size() < reply.size()) {
            if (recvInto(client.fd(), inbound) == IoStatus::Closed) {
                break;
            }
        }
        check(bufferString(inbound) == reply, "client should receive the reply");

        server.join();
    } catch (const std::exception& error) {
        std::cerr << "unexpected exception: " << error.what() << '\n';
        ++failures;
    }

    if (failures != 0) {
        std::cerr << failures.load() << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
