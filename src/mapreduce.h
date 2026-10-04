#pragma once

#include"mapper.h"
#include"reducer.h"

#include<string>
#include<vector>
#include<map>

class MapReduce {
public:
    MapReduce(MapFunction mapper,ReduceFunction reducer);
    void Run(const std::vector<std::string>& inputFiles,const std::string& outFile);
private:
    std::vector<KeyValue> mapPhase(const std::vector<std::string>& inputFiles);
    std::map<Key,std::vector<Value>> shufflePhase(std::vector<KeyValue> intermediate);
    void reducePhase(const std::map<Key,std::vector<Value>>& grouped,const std::string& outputFile);
private:
    MapFunction mapper_;
    ReduceFunction reducer_;
};
