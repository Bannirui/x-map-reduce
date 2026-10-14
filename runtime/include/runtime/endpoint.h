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

    /// @param s 格式ip:port或者只指定了port
    /// @param defaultHost 没有指定ip就用默认的
    inline std::pair<std::string, std::uint16_t> parseHostPort(const std::string& s, const std::string& defaultHost) {
        if (s.find(':') == std::string::npos) {
            return {defaultHost, static_cast<std::uint16_t>(std::stoul(s))};
        }
        return parseEndpoint(s);
    }
}  // namespace xmr
