#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

namespace xmr {

    /// @param endpoint 格式host:port
    inline std::pair<std::string, std::uint16_t> parseEndpoint(const std::string& endpoint) {
        const auto colon = endpoint.rfind(':');
        if (colon == std::string::npos) {
            throw std::runtime_error("expected host:port in '" + endpoint + "'");
        }
        return {endpoint.substr(0, colon), static_cast<std::uint16_t>(std::stoul(endpoint.substr(colon + 1)))};
    }
}  // namespace xmr
