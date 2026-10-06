#pragma once

#include<cstddef>
#include<string>
#include<vector>

namespace mrapp {

enum class TaskKind { Map, Reduce };

// One unit of work handed to a worker. A Map task corresponds to one input
// file; a Reduce task corresponds to one reduce partition, gathering the map
// output of that partition across every map task.
struct Task {
    TaskKind kind=TaskKind::Map;
    std::size_t id=0;              // map: input index; reduce: partition index
    std::string job;
    std::size_t reducers=1;
    std::string workDir;
    std::string input;             // Map only
    std::string output;            // Reduce only
    std::vector<std::string> intermediates;  // Reduce only

    // Tab-separated single-line encoding used on the control-plane wire.
    std::string serialize() const;
    static Task deserialize(const std::string& text);
};

// Runs one task against the mapreduce library (the V3 worker bodies).
void executeTask(const Task& task);

}  // namespace mrapp
