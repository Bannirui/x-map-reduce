#pragma once

#include <cstdint>
#include <string>
#include <vector>

class BlockStore;

namespace xmr::worker {
    struct BlockRef {
        std::string blockId;
        std::string host;
        std::uint16_t port = 0;
    };

    BlockRef parseBlockRef(const std::string& text);

    std::string readBlocks(BlockStore& store, const std::vector<std::string>& refs);

    bool copyBlock(BlockStore& store, const std::string& blockId, const std::string& host, std::uint16_t port);
}  // namespace xmr::worker
