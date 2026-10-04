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

    const fs::path inputFile = dir / "input.txt";
    const fs::path outputFile = dir / "output.txt";
    {
        std::ofstream input(inputFile);
        input << "hello world\nhello mapreduce\n";
    }

    MapReduce job(wordMapper, countReducer);
    job.Run({inputFile.string()}, outputFile.string());

    const auto counts = readOutput(outputFile);
    check(counts.size() == 3, "expected 3 distinct words");
    check(counts.count("hello") == 1 && counts.at("hello") == "2", "hello should be counted twice");
    check(counts.count("world") == 1 && counts.at("world") == "1", "world should be counted once");
    check(counts.count("mapreduce") == 1 && counts.at("mapreduce") == "1", "mapreduce should be counted once");

    bool threw = false;
    try {
        job.Run({(dir / "missing.txt").string()}, outputFile.string());
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
