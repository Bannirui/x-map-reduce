#include "dfs/block_client.h"
#include "dfs/block_server.h"
#include "dfs/block_store.h"
#include "dfs/dfs_messages.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {

    std::atomic<int> failures{0};

    void check(bool condition, const std::string& message) {
        if (!condition) {
            std::cerr << "FAIL: " << message << '\n';
            ++failures;
        }
    }

    std::vector<std::uint8_t> bytesOf(const std::string& text) {
        return std::vector<std::uint8_t>(text.begin(), text.end());
    }

    std::string textOf(const std::vector<std::uint8_t>& data) {
        return std::string(data.begin(), data.end());
    }

} // namespace

int main() {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::filesystem::path root =
            std::filesystem::temp_directory_path() / ("xmr_block_server_test_" + std::to_string(stamp));

    try {
        {
            BlockStore store(root.string());
            BlockServer server(store);
            const std::uint16_t port = server.start("127.0.0.1", 0);
            check(port != 0, "server binds an ephemeral port");
            check(server.port() == port, "reported port matches bind");

            BlockClient client("127.0.0.1", port);

            check(client.writeBlock("blk1", bytesOf("hello dfs")), "write block round-trips");
            const auto small = client.readBlock("blk1");
            check(small.has_value(), "read returns a value");
            check(small && textOf(*small) == "hello dfs", "read returns the written bytes");
            check(store.has("blk1"), "block lands in the server store");

            check(!client.readBlock("missing").has_value(), "missing block reads as empty");

            check(client.writeBlock("empty", {}), "write empty block");
            const auto empty = client.readBlock("empty");
            check(empty.has_value() && empty->empty(), "empty block round-trips");

            std::vector<std::uint8_t> big(3u * kDfsChunkBytes + 123u, 0x5a);
            for (std::size_t i = 0; i < big.size(); i += 997) {
                big[i] = static_cast<std::uint8_t>(i & 0xffu);
            }
            check(client.writeBlock("big", big), "write multi-chunk block");
            const auto bigBack = client.readBlock("big");
            check(bigBack.has_value() && *bigBack == big, "multi-chunk block round-trips");

            check(client.writeBlock("blk1", bytesOf("replaced")), "overwrite through the wire");
            const auto replaced = client.readBlock("blk1");
            check(replaced && textOf(*replaced) == "replaced", "overwrite is visible over the wire");

            server.stop();
        }
    } catch (const std::exception& e) {
        std::cerr << "FAIL: unexpected exception: " << e.what() << '\n';
        ++failures;
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);

    if (failures != 0) {
        std::cerr << failures << " checks failed\n";
        return 1;
    }
    std::cout << "block_server_test passed\n";
    return 0;
}
