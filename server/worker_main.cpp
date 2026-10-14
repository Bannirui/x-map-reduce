#include "runtime/endpoint.h"
#include "runtime/exit_code.h"
#include "worker/worker.h"

#include <filesystem>
#include <iostream>
#include <string>
#include <unistd.h>

namespace {
    void usage(const char* program) {
        std::cerr << "Usage: " << program << " --master <host:port> [--plugin-cache <dir>] [--data-dir <dir>]\n";
    }
}  // namespace

int main(int argc, char** argv) {
    // master的host:port
    std::string master;
    // 目录 缓存Job插件
    std::string pluginCacheDir;
    // 存放block文件的路径 所有的block文件都放在这儿
    std::string dataDir;
    // 解析启动参数
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--master" && i + 1 < argc) {
            master = argv[++i];
        } else if (arg == "--plugin-cache" && i + 1 < argc) {
            pluginCacheDir = argv[++i];
        } else if (arg == "--data-dir" && i + 1 < argc) {
            dataDir = argv[++i];
        }
    }
    // job插件缓存路径
    if (pluginCacheDir.empty()) {
        const char* home = ::getenv("HOME");
        pluginCacheDir = std::string(home != nullptr ? home : "/tmp") + "/.cache/xmr/plugins";
    }
    if (dataDir.empty()) {
        const char* home = ::getenv("HOME");
        dataDir = std::string(home != nullptr ? home : "/tmp") + "/.cache/xmr/blocks";
    }

    try {
        if (master.empty()) {
            throw UsageError("missing --master");
        }
        // 插件缓存目录
        std::error_code cacheError;
        std::filesystem::create_directories(pluginCacheDir, cacheError);
        const auto [host, port] = xmr::parseEndpoint(master);

        return xmr::worker::runWorker({host, port, pluginCacheDir, dataDir});
    } catch (const UsageError& error) {
        std::cerr << "worker: " << error.what() << '\n';
        usage(argv[0]);
        return static_cast<int>(ExitCode::Usage);
    } catch (const std::exception& error) {
        std::cerr << "worker: " << error.what() << '\n';
        return static_cast<int>(ExitCode::Failure);
    }
}
