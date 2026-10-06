#include"mapreduce/mapreduce.h"

#include<sstream>
#include<iostream>

int main() {
    auto mapper=[](const Key& fileName,const Value& lineContent)->std::vector<KeyValue>{
        return std::vector<KeyValue>({{"sum",lineContent}});
    };

    auto reducer=[](const Key& key,const std::vector<Value>& values)->std::vector<KeyValue>{
        int sum=0;
        for(const auto& value:values){
            sum+=std::stoi(value);
        }
        return std::vector<KeyValue>({{key,std::to_string(sum)}});
    };

    MapReduce job(mapper,reducer);
    job.Run({"asset/sum.txt"},"sum.txt");

    std::cout<<"Sum compledted"<<std::endl;

    return 0;
}
