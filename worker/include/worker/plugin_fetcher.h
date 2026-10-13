#pragma once

#include <cstdint>
#include <string>

namespace xmr::worker {
    bool installPlugin(const std::string& cacheDir, const std::string& hash, const std::string& job,
                       const std::string& bytes, std::string& reason);

    bool fetchPlugin(const std::string& dataHost, std::uint16_t dataPort, const std::string& cacheDir,
                     const std::string& hash, const std::string& job, std::string& reason);
}  // namespace xmr::worker
