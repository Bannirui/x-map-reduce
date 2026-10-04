#include "mapreduce.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

int failures = 0;

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

std::vector<KeyValue> wordMapper(const Key&, const Value& line) {
    std::vector<KeyValue> pairs;
    std::istringstream stream(line);
    std::string word;
    while (stream >> word) {
        pairs.emplace_back(word, "1");
    }
    return pairs;
}

std::vector<KeyValue> countReducer(const Key& key, const std::vector<Value>& values) {
    int count = 0;
    for (const auto& value : values) {
        count += std::stoi(value);
    }
    return std::vector<KeyValue>{{key, std::to_string(count)}};
}

// Parses the "key\tvalue\n" output written by MapReduce::reducePhase.
std::map<std::string, std::string> readOutput(const std::filesystem::path& path) {
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

int main() {
    namespace fs = std::filesystem;

    const fs::path dir = fs::temp_directory_path() / "x-map-reduce-test";
    fs::remove_all(dir);
    fs::create_directories(dir);

    // M = 3 map tasks (one per input file), as in the V2 API.
    const std::vector<fs::path> inputs = {dir / "in0.txt", dir / "in1.txt", dir / "in2.txt"};
    {
        std::ofstream(inputs[0]) << "hello world\nhello mapreduce\n";
        std::ofstream(inputs[1]) << "hello distributed\n";
        std::ofstream(inputs[2]) << "mapreduce scales\n";
    }

    std::vector<std::string> inputPaths;
    for (const auto& path : inputs) {
        inputPaths.push_back(path.string());
    }

    const fs::path serialOutput = dir / "serial.txt";
    const fs::path parallelOutput = dir / "parallel.txt";
    MapReduce serialJob(wordMapper, countReducer, 1);
    MapReduce parallelJob(wordMapper, countReducer, 4);
    serialJob.Run(inputPaths, serialOutput.string());
    parallelJob.Run(inputPaths, parallelOutput.string());

    const auto counts = readOutput(serialOutput);
    check(counts.size() == 5, "expected 5 distinct words");
    check(counts.count("hello") == 1 && counts.at("hello") == "3", "hello should be counted three times");
    check(counts.count("world") == 1 && counts.at("world") == "1", "world should be counted once");
    check(counts.count("mapreduce") == 1 && counts.at("mapreduce") == "2", "mapreduce should be counted twice");
    check(counts.count("distributed") == 1 && counts.at("distributed") == "1", "distributed should be counted once");
    check(counts.count("scales") == 1 && counts.at("scales") == "1", "scales should be counted once");
    check(readOutput(parallelOutput) == counts, "parallel map must match serial output");

    bool threw = false;
    try {
        parallelJob.Run({(dir / "missing.txt").string()}, parallelOutput.string());
    } catch (const std::runtime_error&) {
        threw = true;
    }
    check(threw, "a missing input file should throw std::runtime_error");

    fs::remove_all(dir);

    if (failures != 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
