#include"mapreduce.h"

#include<sstream>
#include<iostream>

int main() {
    auto mapper=[](const Key& key,const Value& line){
        std::vector<KeyValue> ret;
        std::istringstream stream(line);
        std::string word;
        while(stream>>word){
            ret.emplace_back(word,"1");
        }
        return ret;
    };

    auto reducer=[](const Key& key,const std::vector<Value>& values){
        int count=0;
        for(const auto& value:values){
            count+=std::stoi(value);
        }
        return std::vector<KeyValue>{
            {key,std::to_string(count)}
        };
    };

    MapReduce job(mapper,reducer);
    job.Run({"data/input.txt"},"output.txt");

    std::cout<<"MapReduce compledted"<<std::endl;

    return 0;
}
