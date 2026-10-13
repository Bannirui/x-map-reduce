#include "worker/plugin_fetcher.h"

#include "protocol/messages.h"
#include "runtime/jobs.h"
#include "runtime/plugin.h"
#include "worker/partition_puller.h"

#include <filesystem>
#include <fstream>
#include <unistd.h>

namespace xmr::worker {
    // 把插件字节落盘缓存(原子rename)并dlopen 返回是否成功
    bool installPlugin(const std::string& cacheDir, const std::string& hash, const std::string& job,
                       const std::string& bytes, std::string& reason) {
        try {
            const std::string path = cacheDir + "/" + hash + ".so";
            const std::string tmp = path + ".tmp-" + std::to_string(::getpid());
            {
                std::ofstream out(tmp, std::ios::binary);
                out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
            }
            std::error_code renameError;
            std::filesystem::rename(tmp, path, renameError);
            if (renameError) {
                std::filesystem::remove(tmp, renameError);
            }
            loadJobPlugin(path);
            if (findJob(job) == nullptr) {
                reason = "plugin does not provide job '" + job + "'";
                return false;
            }
            return true;
        } catch (const std::exception& error) {
            reason = error.what();
            return false;
        }
    }

    bool fetchPlugin(const std::string& dataHost, std::uint16_t dataPort, const std::string& cacheDir,
                     const std::string& hash, const std::string& job, std::string& reason) {
        try {
            protocol::PullPlugin request;
            request.hash = hash;
            const std::string bytes =
                pullStream(dataHost, dataPort, protocol::MessageType::PullPlugin, request.encode());
            if (bytes.empty()) {
                reason = "empty plugin";
                return false;
            }
            return installPlugin(cacheDir, hash, job, bytes, reason);
        } catch (const std::exception& error) {
            reason = error.what();
            return false;
        }
    }
}  // namespace xmr::worker
