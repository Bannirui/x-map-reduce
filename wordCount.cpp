#include"mapreduce.h"

#include<sstream>
#include<iostream>

int main() {
    auto mapper=[](const Key& fileName,const Value& lineContent)->std::vector<KeyValue>{
        std::vector<KeyValue> ret;
        std::istringstream stream(lineContent);
        std::string word;
        while(stream>>word){
            ret.emplace_back(word,"1");
        }
        return ret;
    };

    /**
     * before: hello [1,1]
     * after: hello 2
     */
    auto reducer=[](const Key& word,const std::vector<Value>& wordCnts)->std::vector<KeyValue>{
        int count=0;
        for(const auto& wordCnt:wordCnts){
            count+=std::stoi(wordCnt);
        }
        return std::vector<KeyValue>({{word,std::to_string(count)}});
    };

    MapReduce job(mapper,reducer);
    job.Run({"asset/wordCount.txt"},"wordCount.txt");

    std::cout<<"WordCount compledted"<<std::endl;

    return 0;
}
