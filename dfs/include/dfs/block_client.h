#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

class BlockClient {
public:
    BlockClient(std::string host, std::uint16_t port);

    bool writeBlock(const std::string& blockId, const std::vector<std::uint8_t>& data);

    std::optional<std::vector<std::uint8_t>> readBlock(const std::string& blockId);

private:
    std::string host_;
    std::uint16_t port_;
};
