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
    explicit MapReduce(MapFunction mapper, ReduceFunction reducer, std::size_t workers = 0);

    // In-process pipeline: Map -> Shuffle -> Reduce.
    void Run(const std::vector<std::string>& inputFiles, const std::string& outFile);

    // V3: Map only (used by map_worker processes).
    std::vector<KeyValue> Map(const std::vector<std::string>& inputFiles) const;

    /**
     * map函数的输入[key,value]
     * @param inputName key
     * @param content value
     * @return map函数的中间结果 [k1,v1]->[[k2,v2],[k3,v3],[k4,v3]...]
     */
    std::vector<KeyValue> MapData(const std::string& inputName, const std::string& content) const;

    // V3: Shuffle + Reduce over already-collected intermediate data.
    void ShuffleAndReduce(std::vector<KeyValue> intermediate, const std::string& outFile);

    // V4.2: Shuffle + Reduce over already-collected pairs, returning the result
    // instead of writing it (the data plane ships pairs over TCP, not files).
    std::vector<KeyValue> Reduce(std::vector<KeyValue> intermediate);

private:
    std::map<Key, std::vector<Value> > shufflePhase(std::vector<KeyValue> intermediate);

    std::size_t resolveWorkers(std::size_t tasks) const;

private:
    MapFunction mapper_;
    ReduceFunction reducer_;
    std::size_t workers_;
};

// Plain "key\tvalue\n" intermediate file format (V3).
void WriteKeyValues(const std::string& path, const std::vector<KeyValue>& pairs);

std::vector<KeyValue> ReadKeyValues(const std::string& path);

/**
 * map函数处理的中间结果是一系列的[key,value] 用key hash完对R分区
 * @param key hash(key)%R
 * @param reducers R 将来多少个reduce任务 他们要跟master要自己需要的k-[v1,v2,v3...] 而这些数据是map函数执行完后是放在worker的内存上的 经过shuffle依然是在map所在的worker本机上的
 */
std::size_t partitionOf(const Key& key, std::size_t reducers);