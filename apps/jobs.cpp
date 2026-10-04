#include"jobs.h"

#include<sstream>
#include<string>
#include<vector>

namespace {

// --- word count ---
std::vector<KeyValue> wcMapper(const Key&,const Value& line){
    std::vector<KeyValue> pairs;
    std::istringstream stream(line);
    std::string word;
    while(stream>>word){
        pairs.emplace_back(word,"1");
    }
    return pairs;
}

std::vector<KeyValue> wcReducer(const Key& key,const std::vector<Value>& values){
    int total=0;
    for(const auto& value:values){
        total+=std::stoi(value);
    }
    return std::vector<KeyValue>{{key,std::to_string(total)}};
}
// --- word count ---

// --- calculate ---
std::vector<KeyValue> sumMapper(const Key&,const Value& line){
    return std::vector<KeyValue>{{"sum",line}};
}

std::vector<KeyValue> sumReducer(const Key& key,const std::vector<Value>& values){
    int total=0;
    for(const auto& value:values){
        total+=std::stoi(value);
    }
    return std::vector<KeyValue>{{key,std::to_string(total)}};
}
// --- calculate ---

const std::vector<Job> kJobs{
    {"word_count",wcMapper,wcReducer},
    {"sum",sumMapper,sumReducer},
};

}  // namespace

const Job* findJob(const std::string& name){
    for(const auto& job:kJobs){
        if(job.name==name){
            return &job;
        }
    }
    return nullptr;
}
