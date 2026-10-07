#include"mapreduce/plugin.h"

#include<sstream>
#include<string>
#include<vector>

namespace {

std::vector<KeyValue> wordCountMapper(const Key&,const Value& line){
    std::vector<KeyValue> pairs;
    std::istringstream stream(line);
    std::string word;
    while(stream>>word){
        pairs.emplace_back(word,"1");
    }
    return pairs;
}

std::vector<KeyValue> wordCountReducer(const Key& key,const std::vector<Value>& values){
    int total=0;
    for(const auto& value:values){
        total+=std::stoi(value);
    }
    return std::vector<KeyValue>{{key,std::to_string(total)}};
}

const XmrJob kJob{
    XMR_PLUGIN_ABI_VERSION,
    "word_count",
    wordCountMapper,
    wordCountReducer,
};

}  // namespace

extern "C" const XmrJob* xmr_get_job_v1(){
    return &kJob;
}
