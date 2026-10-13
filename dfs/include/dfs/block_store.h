#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

using BlockId = std::string;

class BlockStore {
public:
    explicit BlockStore(std::string rootDir);

    bool put(const BlockId& blockId, const std::vector<std::uint8_t>& data);

    bool has(const BlockId& blockId) const;

    std::optional<std::vector<std::uint8_t>> get(const BlockId& blockId) const;

    bool remove(const BlockId& blockId);

    std::vector<BlockId> list() const;

    std::size_t count() const;

    std::optional<std::uint64_t> size(const BlockId& blockId) const;

    const std::string& root() const noexcept {
        return rootDir_;
    }

    static bool isValidBlockId(const BlockId& blockId);

private:
    std::string pathOf(const BlockId& blockId) const;

    std::string tempPath() const;

    std::string rootDir_;
};
