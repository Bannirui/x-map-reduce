#include"runtime/task.h"

#include"runtime/jobs.h"

#include<iterator>
#include<stdexcept>
#include<utility>
#include<vector>

namespace mrapp {

namespace {

// Splits a task message into tab-separated fields. Task messages never carry a
// blob, so a plain split is safe here (the control messages that do carry a
// blob parse it with splitHead in the coordinator/worker instead).
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
    out+="\t"+std::to_string(maps);
    if(kind==TaskKind::Map){
        out+="\t"+input;
    }
    return out;
}

Task Task::deserialize(const std::string& text){
    const std::vector<std::string> fields=splitTab(text);
    if(fields.size()<5){
        throw std::runtime_error("malformed task message");
    }
    Task task;
    task.kind=(fields[0]=="MAP")?TaskKind::Map:TaskKind::Reduce;
    task.id=std::stoul(fields[1]);
    task.job=fields[2];
    task.reducers=std::stoul(fields[3]);
    task.maps=std::stoul(fields[4]);
    if(task.kind==TaskKind::Map){
        if(fields.size()<6){
            throw std::runtime_error("malformed map task");
        }
        task.input=fields[5];
    }
    return task;
}

std::string serializeKeyValues(const std::vector<KeyValue>& pairs){
    std::string blob;
    for(const auto& [key,value]:pairs){
        blob+=key;
        blob+='\t';
        blob+=value;
        blob+='\n';
    }
    return blob;
}

std::vector<KeyValue> deserializeKeyValues(const std::string& blob){
    std::vector<KeyValue> pairs;
    std::size_t start=0;
    while(start<blob.size()){
        const std::size_t newline=blob.find('\n',start);
        const std::size_t end=(newline==std::string::npos)?blob.size():newline;
        const std::string line=blob.substr(start,end-start);
        const std::size_t tab=line.find('\t');
        if(tab!=std::string::npos){
            pairs.emplace_back(line.substr(0,tab),line.substr(tab+1));
        }
        if(newline==std::string::npos){
            break;
        }
        start=newline+1;
    }
    return pairs;
}

std::vector<std::vector<KeyValue>> runMapTask(const Task& task){
    const Job* job=findJob(task.job);
    if(job==nullptr){
        throw std::runtime_error("unknown job '"+task.job+"'");
    }
    // Map one file, then bucket the pairs by fnv(key) % R.
    MapReduce runner(job->mapper,job->reducer);
    auto pairs=runner.Map({task.input});

    std::vector<std::vector<KeyValue>> parts(task.reducers);
    for(auto& pair:pairs){
        parts[partitionOf(pair.first,task.reducers)].push_back(std::move(pair));
    }
    return parts;
}

std::vector<KeyValue> runReduceTask(const Task& task,
    const std::function<std::vector<KeyValue>(std::size_t,std::size_t)>& fetch){
    const Job* job=findJob(task.job);
    if(job==nullptr){
        throw std::runtime_error("unknown job '"+task.job+"'");
    }
    std::vector<KeyValue> intermediate;
    for(std::size_t mapTask=0;mapTask<task.maps;++mapTask){
        auto pairs=fetch(mapTask,task.id);
        intermediate.insert(intermediate.end(),
            std::make_move_iterator(pairs.begin()),
            std::make_move_iterator(pairs.end()));
    }
    MapReduce runner(job->mapper,job->reducer);
    return runner.Reduce(std::move(intermediate));
}

}  // namespace mrapp
