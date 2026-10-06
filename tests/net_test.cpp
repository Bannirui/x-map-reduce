#include "net/net.h"

#include <atomic>
#include <cstdint>
#include <iostream>
#include <stdexcept>
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

std::vector<std::uint8_t> toBytes(const std::string& text) {
    return std::vector<std::uint8_t>(text.begin(), text.end());
}

std::string toString(const std::vector<std::uint8_t>& bytes) {
    return std::string(bytes.begin(), bytes.end());
}

}  // namespace

int main() {
    using namespace xmr::net;

    try {
        // Bind to port 0 so the test never collides with another process.
        Listener listener("127.0.0.1", 0);
        const std::uint16_t port = listener.port();
        check(port != 0, "listener should report a real port when bound to 0");

        const std::string small = "hello mapreduce";
        const std::string empty;
        const std::string large(200000, 'x');

        // The server echoes three messages: they exercise the small framing
        // path, the zero-length path, and a payload larger than a socket buffer.
        std::thread server([&] {
            try {
                Connection connection = listener.accept();
                for (int i = 0; i < 3; ++i) {
                    connection.send(connection.receive());
                }
            } catch (const std::exception& error) {
                std::cerr << "server error: " << error.what() << '\n';
                ++failures;
            }
        });

        Connection client = connectTo("127.0.0.1", port);

        client.send(toBytes(small));
        check(toString(client.receive()) == small, "small message should round-trip");

        client.send(toBytes(empty));
        check(toString(client.receive()).empty(), "empty message should round-trip");

        client.send(toBytes(large));
        check(toString(client.receive()) == large, "large message should round-trip");

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
