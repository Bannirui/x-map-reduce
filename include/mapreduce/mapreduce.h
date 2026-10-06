#pragma once

#include"mapper.h"
#include"reducer.h"

#include<cstddef>
#include<string>
#include<vector>
#include<map>

class MapReduce {
public:
    // workers == 0 means use std::thread::hardware_concurrency().
    explicit MapReduce(MapFunction mapper,ReduceFunction reducer,std::size_t workers=0);
    // In-process pipeline: Map -> Shuffle -> Reduce.
    void Run(const std::vector<std::string>& inputFiles,const std::string& outFile);
    // V3: Map only (used by map_worker processes).
    std::vector<KeyValue> Map(const std::vector<std::string>& inputFiles) const;
    // V3: Shuffle + Reduce over already-collected intermediate data.
    void ShuffleAndReduce(std::vector<KeyValue> intermediate,const std::string& outFile);
    // V4.2: Shuffle + Reduce over already-collected pairs, returning the result
    // instead of writing it (the data plane ships pairs over TCP, not files).
    std::vector<KeyValue> Reduce(std::vector<KeyValue> intermediate);
private:
    std::map<Key,std::vector<Value>> shufflePhase(std::vector<KeyValue> intermediate);
    std::size_t resolveWorkers(std::size_t tasks) const;
private:
    MapFunction mapper_;
    ReduceFunction reducer_;
    std::size_t workers_;
};

// Plain "key\tvalue\n" intermediate file format (V3).
void WriteKeyValues(const std::string& path,const std::vector<KeyValue>& pairs);
std::vector<KeyValue> ReadKeyValues(const std::string& path);

// Stable FNV-1a hash partition, key -> [0, reducers). Shared so map and reduce
// agree across processes (std::hash is not guaranteed stable).
std::size_t partitionOf(const Key& key,std::size_t reducers);
