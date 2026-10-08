#include "mapreduce/mapreduce.h"

#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

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

// Fork/exec `program`; when stdoutFd >= 0 the child's stdout is redirected to
// it (used to capture the master's LISTENING line).
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
        _exit(127);  // exec failed
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

}  // namespace

// V6.1 integration: a client submits a job to the running master, the master
// distributes work to two persistent workers over TCP (no shared work dir);
// only --output is a file.
int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "Usage: control_plane_test <path-to-master> <path-to-worker> <path-to-submit> <path-to-job-plugin>\n";
        return 2;
    }
    const std::string master = argv[1];
    const std::string worker = argv[2];
    const std::string submit = argv[3];
    const std::string plugin = argv[4];

    const fs::path dir = fs::temp_directory_path() / "x-map-reduce-control-plane-test";
    fs::remove_all(dir);
    fs::create_directories(dir);

    const std::size_t reducers = 3;
    const std::vector<fs::path> inputs = {dir / "in0.txt", dir / "in1.txt", dir / "in2.txt"};
    {
        std::ofstream(inputs[0]) << "hello world\nhello mapreduce\n";
        std::ofstream(inputs[1]) << "hello distributed\n";
        std::ofstream(inputs[2]) << "mapreduce scales\n";
    }

    const fs::path output = dir / "out.txt";

    // Start the master, capturing stdout so we can read its bound port.
    int pipeFds[2];
    if (::pipe(pipeFds) != 0) {
        std::cerr << "FAIL: pipe failed\n";
        return 1;
    }
    std::vector<std::string> masterArgs = {
        "--listen", "127.0.0.1:0",
    };

    const pid_t masterPid = startProcess(master, masterArgs, pipeFds[1]);
    ::close(pipeFds[1]);
    check(masterPid > 0, "master should start");

    const std::string listenLine = readLine(pipeFds[0]);
    ::close(pipeFds[0]);
    const std::string prefix = "LISTENING ";
    check(listenLine.rfind(prefix, 0) == 0,
          "master should announce its address, got: '" + listenLine + "'");
    const std::string address =
        listenLine.size() > prefix.size() ? listenLine.substr(prefix.size()) : "";
    check(!address.empty(), "master should report a non-empty address");

    // Launch the persistent workers; they get the plugin at runtime from the master.
    const fs::path pluginCache = dir / "plugins";
    std::vector<pid_t> workerPids;
    for (int i = 0; i < 2; ++i) {
        workerPids.push_back(startProcess(worker, {"--master", address, "--plugin-cache", pluginCache.string()}));
    }

    // Submit the job (uploading the plugin) and wait for the result.
    std::vector<std::string> submitArgs = {
        "--master", address,
        "--job", "word_count",
        "--plugin", plugin,
        "--reducers", std::to_string(reducers),
        "--output", output.string(),
        "--shutdown",
    };
    for (const auto& input : inputs) {
        submitArgs.push_back(input.string());
    }
    const pid_t submitPid = startProcess(submit, submitArgs);
    check(submitPid > 0, "submit client should start");
    check(waitProcess(submitPid) == 0, "submit client should exit 0");

    const int masterExit = waitProcess(masterPid);
    check(masterExit == 0, "master should exit 0, got " + std::to_string(masterExit));
    for (const pid_t pid : workerPids) {
        check(waitProcess(pid) == 0, "worker should exit 0");
    }

    const auto counts = readOutput(output);
    check(counts.size() == 5, "expected 5 distinct words");
    check(counts.count("hello") == 1 && counts.at("hello") == "3", "hello should be counted three times");
    check(counts.count("world") == 1 && counts.at("world") == "1", "world should be counted once");
    check(counts.count("mapreduce") == 1 && counts.at("mapreduce") == "2", "mapreduce should be counted twice");
    check(counts.count("distributed") == 1 && counts.at("distributed") == "1", "distributed should be counted once");
    check(counts.count("scales") == 1 && counts.at("scales") == "1", "scales should be counted once");

    fs::remove_all(dir);

    if (failures != 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
