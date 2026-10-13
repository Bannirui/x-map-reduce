#pragma once

#include <cstdint>
#include <string>

// worker节点
namespace xmr::worker {
    // worker节点启动配置
    struct WorkerConfig {
        // master节点控制面端口ip:port
        std::string masterCtlHost;
        std::uint16_t masterCtlPort = 0;
        // job插件的缓存路径
        std::string pluginCacheDir;
        // block清单
        std::string blockDir;
    };

    int runWorker(WorkerConfig config);
}  // namespace xmr::worker
