#include"mapreduce.h"

#include<fstream>
#include<stdexcept>
#include<algorithm>
#include<map>


MapReduce::MapReduce(MapFunction mapper,ReduceFunction reducer)
    :mapper_(std::move(mapper)),reducer_(std::move(reducer))
{}

void MapReduce::Run(const std::vector<std::string>& inputFiles,const std::string& outFile){
    auto intermediate=this->mapPhase(inputFiles);
    auto grouped=this->shufflePhase(std::move(intermediate));
    this->reducePhase(grouped,outFile);
}

/**
 * input.txt:
 *   hello world
 *   hello mapreduce
 *
 * intermediate:
 *   hello 1
 *   world 1
 *   hello 1
 *   mapreduce 1
 */
std::vector<KeyValue> MapReduce::mapPhase(const std::vector<std::string>& inputFiles){
    std::vector<KeyValue> intermediate;
    for(const auto& filename:inputFiles){
        std::ifstream input(filename);
        if(!input){
            throw std::runtime_error("Failed to open input file: "+filename);
        }
        std::string line;
        while(std::getline(input,line)){
            auto pairs=this->mapper_(filename,line);
            intermediate.insert(intermediate.end(),pairs.begin(),pairs.end());
        }
    }
    return intermediate;
}

std::map<Key,std::vector<Value>> MapReduce::shufflePhase(std::vector<KeyValue> intermediate){
    /**
     * after sorting:
     *   hello 1
     *   hello 1
     *   world 1
     *   mapreduce 1
     */
    std::sort(intermediate.begin(),intermediate.end(),
        [](const KeyValue& a,const KeyValue& b){
            return a.first<b.first;
        }
    );
    /**
     * after group:
     *   hello [1, 1]
     *   world [1]
     *   mapreduce [1]
     */
    std::map<Key,std::vector<Value>> grouped;
    for(const auto& [k,v]:intermediate){
        grouped[k].push_back(v);
    }
    return grouped;
}

void MapReduce::reducePhase(const std::map<Key,std::vector<Value>>& grouped,const std::string& outputFile){
    std::ofstream output(outputFile);
    if(!output){
        throw std::runtime_error("Failed to open output file: "+outputFile);
    }
    for(const auto& [key,values]:grouped){
        auto results=this->reducer_(key,values);
        for(const auto& [retKey,retValue]:results){
            output<<retKey<<'\t'<<retValue<<'\n';
        }
    }
}
