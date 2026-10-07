#include"mapreduce/plugin.h"

#include<string>
#include<vector>

namespace {

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

const XmrJob kJob{
    XMR_PLUGIN_ABI_VERSION,
    "sum",
    sumMapper,
    sumReducer,
};

}  // namespace

extern "C" const XmrJob* xmr_get_job_v1(){
    return &kJob;
}
