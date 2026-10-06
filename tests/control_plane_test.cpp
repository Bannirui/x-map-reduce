#include "mapreduce/mapreduce.h"

#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
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
// it (used to capture the coordinator's LISTENING line).
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

std::set<std::string> keysIn(const fs::path& path) {
    std::set<std::string> keys;
    std::ifstream input(path);
    std::string line;
    while (std::getline(input, line)) {
        const auto tab = line.find('\t');
        if (tab != std::string::npos) {
            keys.insert(line.substr(0, tab));
        }
    }
    return keys;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: control_plane_test <path-to-coordinator> <path-to-worker>\n";
        return 2;
    }
    const std::string coordinator = argv[1];
    const std::string worker = argv[2];

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
    const std::vector<std::set<std::string>> expectedInputKeys = {
        {"hello", "world", "mapreduce"},
        {"hello", "distributed"},
        {"mapreduce", "scales"},
    };

    const fs::path workDir = dir / "work";
    const fs::path output = dir / "out.txt";

    // Start the coordinator, capturing stdout so we can read its bound port.
    int pipeFds[2];
    if (::pipe(pipeFds) != 0) {
        std::cerr << "FAIL: pipe failed\n";
        return 1;
    }
    std::vector<std::string> coordinatorArgs = {
        "--job", "word_count",
        "--reducers", std::to_string(reducers),
        "--workers", "2",
        "--listen", "127.0.0.1:0",
        "--work-dir", workDir.string(),
        "--output", output.string(),
    };
    for (const auto& input : inputs) {
        coordinatorArgs.push_back(input.string());
    }

    const pid_t coordinatorPid = startProcess(coordinator, coordinatorArgs, pipeFds[1]);
    ::close(pipeFds[1]);
    check(coordinatorPid > 0, "coordinator should start");

    const std::string listenLine = readLine(pipeFds[0]);
    ::close(pipeFds[0]);
    const std::string prefix = "LISTENING ";
    check(listenLine.rfind(prefix, 0) == 0,
          "coordinator should announce its address, got: '" + listenLine + "'");
    const std::string address =
        listenLine.size() > prefix.size() ? listenLine.substr(prefix.size()) : "";
    check(!address.empty(), "coordinator should report a non-empty address");

    // Launch the persistent workers the coordinator is waiting for.
    std::vector<pid_t> workerPids;
    for (int i = 0; i < 2; ++i) {
        workerPids.push_back(startProcess(worker, {"--coordinator", address}));
    }

    const int coordinatorExit = waitProcess(coordinatorPid);
    check(coordinatorExit == 0, "coordinator should exit 0, got " + std::to_string(coordinatorExit));
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

    // Map side: map-i-part-r must contain only keys where partitionOf(key, R) == r.
    for (std::size_t i = 0; i < inputs.size(); ++i) {
        std::map<std::string, std::size_t> foundIn;
        for (std::size_t r = 0; r < reducers; ++r) {
            const fs::path path = workDir / ("map-" + std::to_string(i) + "-part-" + std::to_string(r) + ".txt");
            check(fs::exists(path), "map partition should exist: " + path.filename().string());
            for (const auto& key : keysIn(path)) {
                ++foundIn[key];
                check(partitionOf(key, reducers) == r,
                      key + " is in the wrong map partition");
            }
        }
        check(foundIn.size() == expectedInputKeys[i].size(),
              "map task " + std::to_string(i) + " should cover its input keys");
        for (const auto& [key, count] : foundIn) {
            check(count == 1, "key must appear in exactly one map partition: " + key);
        }
    }

    // Reduce side: part-r outputs are disjoint per partition and cover all keys.
    std::map<std::string, std::size_t> reduceFoundIn;
    for (std::size_t r = 0; r < reducers; ++r) {
        const fs::path path = workDir / ("part-" + std::to_string(r) + ".txt");
        check(fs::exists(path), "reduce part should exist: " + path.filename().string());
        for (const auto& key : keysIn(path)) {
            ++reduceFoundIn[key];
        }
    }
    check(reduceFoundIn.size() == 5, "reduce parts should cover all 5 keys");
    for (const auto& [key, count] : reduceFoundIn) {
        check(count == 1, "key must appear in exactly one reduce part: " + key);
    }

    fs::remove_all(dir);

    if (failures != 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
