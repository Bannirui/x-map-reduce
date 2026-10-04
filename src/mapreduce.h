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
    void Run(const std::vector<std::string>& inputFiles,const std::string& outFile);
private:
    std::vector<KeyValue> mapPhase(const std::vector<std::string>& inputFiles);
    std::map<Key,std::vector<Value>> shufflePhase(std::vector<KeyValue> intermediate);
    void reducePhase(const std::map<Key,std::vector<Value>>& grouped,const std::string& outputFile);
    std::size_t resolveWorkers(std::size_t tasks) const;
private:
    MapFunction mapper_;
    ReduceFunction reducer_;
    std::size_t workers_;
};
