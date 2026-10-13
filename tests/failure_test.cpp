#include "mapreduce/mapreduce.h"

#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace {

    int failures = 0;

    void check(bool condition, const std::string& message) {
        if (!condition) {
            std::cerr << "FAIL: " << message << '\n';
            ++failures;
        }
    }

    pid_t startProcess(const std::string& program,
                       const std::vector<std::string>& args,
                       int stdoutFd = -1) {
        const pid_t pid = fork();
        if (pid < 0) {
            return -1;
        }
        if (pid == 0) {
            if (stdoutFd >= 0) {
                ::dup2(stdoutFd, STDOUT_FILENO);
            }
            std::vector<char*> argv;
            argv.push_back(const_cast<char*>(program.c_str()));
            for (const auto& arg : args) {
                argv.push_back(const_cast<char*>(arg.c_str()));
            }
            argv.push_back(nullptr);
            execv(program.c_str(), argv.data());
            _exit(127);
        }
        return pid;
    }

    int waitProcess(pid_t pid) {
        int status = 0;
        while (::waitpid(pid, &status, 0) == -1) {
            if (errno != EINTR) {
                return -1;
            }
        }
        return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    }

    std::string readLine(int fd) {
        std::string line;
        char c = '\0';
        while (true) {
            const ssize_t n = ::read(fd, &c, 1);
            if (n <= 0 || c == '\n') {
                break;
            }
            line.push_back(c);
        }
        return line;
    }

    std::map<std::string, std::string> readOutput(const fs::path& path) {
        std::map<std::string, std::string> result;
        std::ifstream input(path);
        std::string line;
        while (std::getline(input, line)) {
            const auto tab = line.find('\t');
            if (tab != std::string::npos) {
                result[line.substr(0, tab)] = line.substr(tab + 1);
            }
        }
        return result;
    }

    bool submitJob(const std::string& submit, const std::string& address, const std::string& plugin,
                   const fs::path& output, const std::vector<fs::path>& inputs, bool shutdown) {
        std::vector<std::string> args = {
            "--master", address,
            "--job", "word_count",
            "--plugin", plugin,
            "--reducers", "3",
            "--output", output.string(),
        };
        if (shutdown) {
            args.push_back("--shutdown");
        }
        for (const auto& input : inputs) {
            args.push_back(input.string());
        }
        const pid_t pid = startProcess(submit, args);
        return pid > 0 && waitProcess(pid) == 0;
    }

}  // namespace

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "Usage: failure_test <path-to-master> <path-to-worker> <path-to-submit> <path-to-job-plugin>\n";
        return 2;
    }
    const std::string master = argv[1];
    const std::string worker = argv[2];
    const std::string submit = argv[3];
    const std::string plugin = argv[4];

    const fs::path dir = fs::temp_directory_path() / "x-map-reduce-failure-test";
    fs::remove_all(dir);
    fs::create_directories(dir);

    const std::vector<fs::path> inputs = {dir / "in0.txt", dir / "in1.txt", dir / "in2.txt"};
    {
        std::ofstream(inputs[0]) << "hello world\nhello mapreduce\n";
        std::ofstream(inputs[1]) << "hello distributed\n";
        std::ofstream(inputs[2]) << "mapreduce scales\n";
    }

    int pipeFds[2];
    if (::pipe(pipeFds) != 0) {
        std::cerr << "FAIL: pipe failed\n";
        return 1;
    }
    const pid_t masterPid = startProcess(master, {"--listen", "127.0.0.1:0"}, pipeFds[1]);
    ::close(pipeFds[1]);
    check(masterPid > 0, "master should start");

    const std::string listenLine = readLine(pipeFds[0]);
    ::close(pipeFds[0]);
    const std::string prefix = "LISTENING ";
    check(listenLine.rfind(prefix, 0) == 0, "master should announce its address");
    const std::string address =
        listenLine.size() > prefix.size() ? listenLine.substr(prefix.size()) : "";

    const fs::path pluginCache = dir / "plugins";
    std::vector<pid_t> workerPids;
    for (int i = 0; i < 2; ++i) {
        const fs::path dataDir = dir / ("blocks" + std::to_string(i));
        workerPids.push_back(startProcess(worker, {"--master", address,
                                                   "--plugin-cache", pluginCache.string(),
                                                   "--data-dir", dataDir.string()}));
    }

    const fs::path out1 = dir / "out1.txt";
    check(submitJob(submit, address, plugin, out1, inputs, false), "first job succeeds with two workers");

    ::kill(workerPids[1], SIGKILL);
    waitProcess(workerPids[1]);
    check(::kill(workerPids[1], 0) != 0, "killed worker is gone");
    ::usleep(500 * 1000);

    const fs::path out2 = dir / "out2.txt";
    check(submitJob(submit, address, plugin, out2, inputs, true), "second job succeeds after a worker died");

    check(waitProcess(masterPid) == 0, "master exits 0");
    check(waitProcess(workerPids[0]) == 0, "surviving worker exits 0");

    const auto counts = readOutput(out2);
    check(counts.size() == 5, "expected 5 distinct words after recovery");
    check(counts.count("hello") == 1 && counts.at("hello") == "3", "hello counted three times");
    check(counts.count("mapreduce") == 1 && counts.at("mapreduce") == "2", "mapreduce counted twice");

    fs::remove_all(dir);

    if (failures != 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
