#include"runtime/task.h"

#include"mapreduce/mapreduce.h"
#include"runtime/jobs.h"
#include"runtime/paths.h"

#include<iterator>
#include<stdexcept>
#include<utility>
#include<vector>

namespace mrapp {

namespace {

std::vector<std::string> splitTab(const std::string& text){
    std::vector<std::string> fields;
    std::size_t start=0;
    while(true){
        const std::size_t tab=text.find('\t',start);
        if(tab==std::string::npos){
            fields.push_back(text.substr(start));
            break;
        }
        fields.push_back(text.substr(start,tab-start));
        start=tab+1;
    }
    return fields;
}

}  // namespace

std::string Task::serialize() const{
    std::string out=(kind==TaskKind::Map)?"MAP":"REDUCE";
    out+="\t"+std::to_string(id);
    out+="\t"+job;
    out+="\t"+std::to_string(reducers);
    out+="\t"+workDir;
    if(kind==TaskKind::Map){
        out+="\t"+input;
    }else{
        out+="\t"+output;
        for(const auto& file:intermediates){
            out+="\t"+file;
        }
    }
    return out;
}

Task Task::deserialize(const std::string& text){
    const std::vector<std::string> fields=splitTab(text);
    if(fields.size()<6){
        throw std::runtime_error("malformed task message");
    }
    Task task;
    task.kind=(fields[0]=="MAP")?TaskKind::Map:TaskKind::Reduce;
    task.id=std::stoul(fields[1]);
    task.job=fields[2];
    task.reducers=std::stoul(fields[3]);
    task.workDir=fields[4];
    if(task.kind==TaskKind::Map){
        task.input=fields[5];
    }else{
        task.output=fields[5];
        for(std::size_t i=6;i<fields.size();++i){
            task.intermediates.push_back(fields[i]);
        }
    }
    return task;
}

void executeTask(const Task& task){
    const Job* job=findJob(task.job);
    if(job==nullptr){
        throw std::runtime_error("unknown job '"+task.job+"'");
    }
    if(task.kind==TaskKind::Map){
        // Same work as the V3.2 map_worker: map one file, partition by
        // fnv(key) % R, and write every partition file (empty ones included).
        MapReduce runner(job->mapper,job->reducer);
        auto pairs=runner.Map({task.input});

        std::vector<std::vector<KeyValue>> parts(task.reducers);
        for(auto& pair:pairs){
            parts[partitionOf(pair.first,task.reducers)].push_back(std::move(pair));
        }
        for(std::size_t r=0;r<task.reducers;++r){
            WriteKeyValues(mapPartPath(task.workDir,task.id,r).string(),parts[r]);
        }
    }else{
        // Same work as the V3.2 reduce_worker: concatenate this partition's
        // files and reduce them into a sorted part file.
        std::vector<KeyValue> intermediate;
        for(const auto& path:task.intermediates){
            auto pairs=ReadKeyValues(path);
            intermediate.insert(intermediate.end(),
                std::make_move_iterator(pairs.begin()),
                std::make_move_iterator(pairs.end()));
        }
        MapReduce runner(job->mapper,job->reducer);
        runner.ShuffleAndReduce(std::move(intermediate),task.output);
    }
}

}  // namespace mrapp
