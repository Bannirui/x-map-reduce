#include "dfs/block_store.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <system_error>
#include <utility>

static std::atomic<std::uint64_t> tempCounter{0};

static std::uint64_t checksumOf(const std::uint8_t* data, std::size_t size) {
    std::uint64_t hash = 1469598103934665603ull;
    for (std::size_t i = 0; i < size; ++i) {
        hash ^= data[i];
        hash *= 1099511628211ull;
    }
    return hash;
}

static void appendBE64(std::vector<std::uint8_t>& out, std::uint64_t value) {
    for (int i = 7; i >= 0; --i) {
        out.push_back(static_cast<std::uint8_t>(value >> (i * 8)));
    }
}

static std::uint64_t readBE64(const std::uint8_t* data) {
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value = (value << 8) | data[i];
    }
    return value;
}

static std::uint64_t nextTempSuffix() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return static_cast<std::uint64_t>(now) ^ (tempCounter.fetch_add(1, std::memory_order_relaxed) + 1);
}

BlockStore::BlockStore(std::string rootDir) : rootDir_(std::move(rootDir)) {
    if (rootDir_.empty()) {
        throw std::invalid_argument("block store root must not be empty");
    }
    std::error_code ec;
    const std::filesystem::path root(rootDir_);
    if (!std::filesystem::exists(root, ec)) {
        std::filesystem::create_directories(root, ec);
        if (ec) {
            throw std::runtime_error("cannot create block store root: " + ec.message());
        }
    }
    if (!std::filesystem::is_directory(root, ec)) {
        throw std::runtime_error("block store root is not a directory");
    }
    rootDir_ = std::filesystem::absolute(root, ec).string();
}

bool BlockStore::isValidBlockId(const BlockId& blockId) {
    if (blockId.empty() || blockId.size() > 128) {
        return false;
    }
    for (const char c : blockId) {
        const bool ok =
            (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_';
        if (!ok) {
            return false;
        }
    }
    return true;
}

std::string BlockStore::pathOf(const BlockId& blockId) const {
    return rootDir_ + "/" + blockId;
}

std::string BlockStore::tempPath() const {
    return rootDir_ + "/.tmp-" + std::to_string(nextTempSuffix());
}

bool BlockStore::put(const BlockId& blockId, const std::vector<std::uint8_t>& data) {
    if (!isValidBlockId(blockId)) {
        return false;
    }
    const std::string temp = tempPath();
    {
        std::ofstream out(temp, std::ios::binary | std::ios::trunc);
        if (!out) {
            return false;
        }
        std::vector<std::uint8_t> header;
        appendBE64(header, checksumOf(data.data(), data.size()));
        out.write(reinterpret_cast<const char*>(header.data()), static_cast<std::streamsize>(header.size()));
        if (!data.empty()) {
            out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
        }
        out.flush();
        if (!out) {
            std::error_code ec;
            std::filesystem::remove(temp, ec);
            return false;
        }
    }
    std::error_code ec;
    std::filesystem::rename(temp, pathOf(blockId), ec);
    if (ec) {
        std::filesystem::remove(temp, ec);
        return false;
    }
    return true;
}

bool BlockStore::has(const BlockId& blockId) const {
    if (!isValidBlockId(blockId)) {
        return false;
    }
    std::error_code ec;
    return std::filesystem::is_regular_file(pathOf(blockId), ec);
}

std::optional<std::vector<std::uint8_t>> BlockStore::get(const BlockId& blockId) const {
    if (!isValidBlockId(blockId)) {
        return std::nullopt;
    }
    const std::string path = pathOf(blockId);
    std::error_code ec;
    if (!std::filesystem::is_regular_file(path, ec)) {
        return std::nullopt;
    }
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in) {
        return std::nullopt;
    }
    const std::streamoff length = in.tellg();
    if (length < 0) {
        return std::nullopt;
    }
    std::vector<std::uint8_t> data(static_cast<std::size_t>(length));
    in.seekg(0, std::ios::beg);
    if (length > 0) {
        in.read(reinterpret_cast<char*>(data.data()), length);
        if (!in) {
            return std::nullopt;
        }
    }
    if (data.size() < 8) {
        return std::nullopt;
    }
    const std::uint64_t expected = readBE64(data.data());
    const std::uint64_t actual = checksumOf(data.data() + 8, data.size() - 8);
    if (expected != actual) {
        return std::nullopt;
    }
    return std::vector<std::uint8_t>(data.begin() + 8, data.end());
}

bool BlockStore::remove(const BlockId& blockId) {
    if (!isValidBlockId(blockId)) {
        return false;
    }
    std::error_code ec;
    return std::filesystem::remove(pathOf(blockId), ec) && !ec;
}

std::vector<BlockId> BlockStore::list() const {
    std::vector<BlockId> blocks;
    std::error_code ec;
    std::filesystem::directory_iterator it(rootDir_, ec);
    if (ec) {
        return blocks;
    }
    for (const auto& entry : it) {
        std::error_code entryEc;
        if (!entry.is_regular_file(entryEc)) {
            continue;
        }
        const std::string name = entry.path().filename().string();
        if (isValidBlockId(name)) {
            blocks.push_back(name);
        }
    }
    std::sort(blocks.begin(), blocks.end());
    return blocks;
}

std::size_t BlockStore::count() const {
    return list().size();
}

std::optional<std::uint64_t> BlockStore::size(const BlockId& blockId) const {
    if (!isValidBlockId(blockId)) {
        return std::nullopt;
    }
    std::error_code ec;
    const auto bytes = std::filesystem::file_size(pathOf(blockId), ec);
    if (ec || bytes < 8) {
        return std::nullopt;
    }
    return static_cast<std::uint64_t>(bytes - 8);
}
