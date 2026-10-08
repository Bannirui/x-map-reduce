#include"xmr/client/client.h"

#include<cstdint>
#include<iostream>
#include<string>
#include<vector>

namespace {
    void usage(const char* program) {
        std::cerr << "Usage: " << program
            << " --master <host:port> --job <name> [--plugin <path>] [--reducers <R>]"
            << " [--workers <N>] [--shutdown] --output <file> <input...>\n";
    }
} // namespace

int main(int argc, char** argv) {
    std::string master;
    std::string job;
    std::string output;
    std::string pluginPath;
    std::uint64_t reducers = 1;
    std::uint64_t workers = 1;
    std::vector<std::string> inputs;
    bool shutdownAfter = false;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--master" && i + 1 < argc) {
            master = argv[++i];
        } else if (arg == "--job" && i + 1 < argc) {
            job = argv[++i];
        } else if (arg == "--plugin" && i + 1 < argc) {
            pluginPath = argv[++i];
        } else if (arg == "--reducers" && i + 1 < argc) {
            reducers = std::stoull(argv[++i]);
        } else if (arg == "--workers" && i + 1 < argc) {
            workers = std::stoull(argv[++i]);
        } else if (arg == "--output" && i + 1 < argc) {
            output = argv[++i];
        } else if (arg == "--shutdown") {
            shutdownAfter = true;
        } else {
            inputs.emplace_back(arg);
        }
    }

    if (master.empty() || job.empty() || output.empty() || inputs.empty()) {
        usage(argv[0]);
        return 2;
    }

    try {
        xmr::client::Client client(master);
        xmr::client::SubmitRequest request;
        request.job = job;
        request.reducers = reducers;
        request.workers = workers;
        request.output = output;
        request.inputs = inputs;
        request.pluginPath = pluginPath;

        std::string reason;
        if (!client.submit(request, reason)) {
            std::cerr << "submit: rejected: " << reason << '\n';
            return 1;
        }
        std::cout << "submitted '" << job << "'" << std::endl;

        const auto result = client.wait();
        if (result.status != xmr::protocol::StatusCode::Ok) {
            std::cerr << "submit: job failed: " << result.reason << '\n';
            if (shutdownAfter) {
                client.shutdown();
            }
            return 1;
        }
        std::cout << "result: " << result.output << std::endl;
        if (shutdownAfter) {
            client.shutdown();
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "submit: " << error.what() << '\n';
        return 1;
    }
}
