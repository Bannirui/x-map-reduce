#include "client/client.h"

#include <iostream>
#include <string>

namespace {
    void usage(const char* program) {
        std::cerr << "Usage: " << program << " put --master <host:port> <local-file> <dfs-path>\n";
    }
}  // namespace

int main(int argc, char** argv) {
    if (argc < 2 || std::string(argv[1]) != "put") {
        usage(argv[0]);
        return 2;
    }

    std::string master;
    std::string localPath;
    std::string dfsPath;
    for (int i = 2; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--master" && i + 1 < argc) {
            master = argv[++i];
        } else if (localPath.empty()) {
            localPath = arg;
        } else if (dfsPath.empty()) {
            dfsPath = arg;
        }
    }

    if (master.empty() || localPath.empty() || dfsPath.empty()) {
        usage(argv[0]);
        return 2;
    }

    try {
        xmr::client::Client client(master);
        std::string reason;
        if (!client.put(localPath, dfsPath, reason)) {
            std::cerr << "dfs put: " << reason << '\n';
            return 1;
        }
        std::cout << "put " << localPath << " -> " << dfsPath << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "dfs put: " << error.what() << '\n';
        return 1;
    }
}
