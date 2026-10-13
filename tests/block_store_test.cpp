#include "dfs/block_store.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
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
            std::filesystem::temp_directory_path() / ("xmr_block_store_test_" + std::to_string(stamp));

    try {
        {
            bool threw = false;
            try {
                BlockStore store("");
            } catch (const std::exception&) {
                threw = true;
            }
            check(threw, "empty root is rejected");
        }

        check(BlockStore::isValidBlockId("block-1_A"), "alphanumeric block id is valid");
        check(!BlockStore::isValidBlockId(""), "empty block id is invalid");
        check(!BlockStore::isValidBlockId("../etc/passwd"), "path traversal is invalid");
        check(!BlockStore::isValidBlockId("a/b"), "separator is invalid");
        check(!BlockStore::isValidBlockId("a.b"), "dot is invalid");
        check(!BlockStore::isValidBlockId(std::string(129, 'a')), "oversized block id is invalid");

        {
            BlockStore store(root.string());
            check(store.count() == 0, "store starts empty");
            check(store.list().empty(), "empty store lists nothing");
            check(std::filesystem::is_directory(root), "root directory is created");

            check(store.put("b2", bytesOf("second")), "put b2");
            check(store.put("a1", bytesOf("first")), "put a1");
            check(store.put("c3", bytesOf("third")), "put c3");
            check(store.count() == 3, "count tracks blocks");
            check(store.has("a1"), "a1 is present");

            const std::vector<BlockId> listed = store.list();
            check(listed.size() == 3, "list returns all blocks");
            check(listed[0] == "a1" && listed[1] == "b2" && listed[2] == "c3", "list is sorted");

            const auto value = store.get("b2");
            check(value.has_value(), "get returns the stored value");
            check(value && textOf(*value) == "second", "value bytes round-trip");

            const auto size = store.size("c3");
            check(size.has_value() && *size == 5, "size reports byte length");

            check(!store.get("missing").has_value(), "missing block returns empty");
            check(!store.has("missing"), "missing block is absent");
            check(!store.get("a/b").has_value(), "invalid id is never read");
            check(!store.put("bad/id", bytesOf("x")), "put with invalid id fails");
            check(store.count() == 3, "invalid put does not add a block");

            check(store.put("a1", bytesOf("updated")), "overwrite a1");
            const auto updated = store.get("a1");
            check(updated && textOf(*updated) == "updated", "overwrite replaces bytes");
            check(store.count() == 3, "overwrite does not duplicate");

            check(store.put("crc1", bytesOf("payload")), "put crc1");
            {
                std::ofstream corrupt((root / "crc1").string(), std::ios::binary | std::ios::app);
                corrupt.put('\x00');
            }
            check(!store.get("crc1").has_value(), "corrupted block fails checksum");
            check(store.has("crc1"), "corrupted block still present until removed");
            check(store.remove("crc1"), "remove corrupted block");

            check(store.remove("b2"), "remove b2");
            check(!store.remove("b2"), "removing twice fails");
            check(store.count() == 2, "remove drops a block");
        }

        {
            BlockStore reopened(root.string());
            check(reopened.count() == 2, "blocks persist across store instances");
            const auto value = reopened.get("c3");
            check(value && textOf(*value) == "third", "persisted bytes survive reopen");
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
    std::cout << "block_store_test passed\n";
    return 0;
}
