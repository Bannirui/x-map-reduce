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

int run(const std::string& program, const std::vector<std::string>& args) {
    const pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "FAIL: fork failed\n";
        return -1;
    }
    if (pid == 0) {
        std::vector<char*> argv;
        argv.push_back(const_cast<char*>(program.c_str()));
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        argv.push_back(nullptr);
        execv(program.c_str(), argv.data());
        _exit(127);  // exec failed
    }
    int status = 0;
    while (waitpid(pid, &status, 0) == -1) {
        // retry on EINTR
    }
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
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

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: coordinator_test <path-to-coordinator>\n";
        return 2;
    }
    const std::string coordinator = argv[1];

    const fs::path dir = fs::temp_directory_path() / "x-map-reduce-coordinator-test";
    fs::remove_all(dir);
    fs::create_directories(dir);

    const std::vector<fs::path> inputs = {dir / "in0.txt", dir / "in1.txt", dir / "in2.txt"};
    {
        std::ofstream(inputs[0]) << "hello world\nhello mapreduce\n";
        std::ofstream(inputs[1]) << "hello distributed\n";
        std::ofstream(inputs[2]) << "mapreduce scales\n";
    }

    const fs::path workDir = dir / "work";
    const fs::path output = dir / "out.txt";

    std::vector<std::string> args = {
        "--job", "word_count",
        "--reducers", "3",
        "--work-dir", workDir.string(),
        "--output", output.string(),
    };
    for (const auto& input : inputs) {
        args.push_back(input.string());
    }

    const int exitCode = run(coordinator, args);
    check(exitCode == 0, "coordinator should exit 0, got " + std::to_string(exitCode));

    const auto counts = readOutput(output);
    check(counts.size() == 5, "expected 5 distinct words");
    check(counts.count("hello") == 1 && counts.at("hello") == "3", "hello should be counted three times");
    check(counts.count("world") == 1 && counts.at("world") == "1", "world should be counted once");
    check(counts.count("mapreduce") == 1 && counts.at("mapreduce") == "2", "mapreduce should be counted twice");
    check(counts.count("distributed") == 1 && counts.at("distributed") == "1", "distributed should be counted once");
    check(counts.count("scales") == 1 && counts.at("scales") == "1", "scales should be counted once");
    check(fs::exists(workDir / "map-0.txt") && fs::exists(workDir / "map-1.txt")
              && fs::exists(workDir / "map-2.txt"),
          "one intermediate file per input should be produced");

    // Each key must land in exactly one reduce partition.
    const std::vector<fs::path> parts = {
        workDir / "part-0.txt", workDir / "part-1.txt", workDir / "part-2.txt"};
    auto keysIn = [](const fs::path& path) {
        std::set<std::string> keys;
        std::ifstream in(path);
        std::string line;
        while (std::getline(in, line)) {
            const auto tab = line.find('\t');
            if (tab != std::string::npos) {
                keys.insert(line.substr(0, tab));
            }
        }
        return keys;
    };
    std::vector<std::set<std::string>> partitions;
    for (const auto& part : parts) {
        check(fs::exists(part), "reduce part should exist: " + part.filename().string());
        partitions.push_back(keysIn(part));
    }
    std::map<std::string, int> keyCounts;
    for (const auto& keys : partitions) {
        for (const auto& key : keys) {
            ++keyCounts[key];
        }
    }
    check(keyCounts.size() == 5, "partitions should cover all 5 keys");
    for (const auto& [key, count] : keyCounts) {
        check(count == 1, "key must appear in exactly one partition: " + key);
    }

    fs::remove_all(dir);

    if (failures != 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
