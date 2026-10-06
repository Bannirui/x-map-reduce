#pragma once

#include"mapreduce/mapreduce.h"

#include<cstddef>
#include<functional>
#include<string>
#include<vector>

namespace mrapp {

enum class TaskKind { Map, Reduce };

// One unit of work handed to a worker. A Map task corresponds to one input
// file; a Reduce task corresponds to one reduce partition, gathering that
// partition's map output across every map task. In V4.2 the intermediate data
// travels over TCP, so a task carries no file paths.
struct Task {
    TaskKind kind=TaskKind::Map;
    std::size_t id=0;         // map: input index; reduce: partition index
    std::string job;
    std::size_t reducers=1;
    std::size_t maps=0;       // number of map tasks (a reducer fetches one per map)
    std::string input;        // Map only

    // Tab-separated single-line encoding used on the control-plane wire.
    std::string serialize() const;
    static Task deserialize(const std::string& text);
};

// Data-plane encoding: the same "key\tvalue\n" layout the old files used, just
// carried inside a message instead of a file.
std::string serializeKeyValues(const std::vector<KeyValue>& pairs);
std::vector<KeyValue> deserializeKeyValues(const std::string& blob);

// Map task: map the single input file and return one bucket per reducer.
std::vector<std::vector<KeyValue>> runMapTask(const Task& task);

// Reduce task: fetch each map task's partition through `fetch`, then
// shuffle+reduce into the final key-ordered pairs.
std::vector<KeyValue> runReduceTask(const Task& task,
    const std::function<std::vector<KeyValue>(std::size_t mapTask,std::size_t partition)>& fetch);

}  // namespace mrapp
