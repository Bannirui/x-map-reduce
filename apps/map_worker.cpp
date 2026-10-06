#include"jobs.h"
#include"mapreduce/mapreduce.h"
#include"paths.h"

#include<iostream>
#include<string>
#include<utility>
#include<vector>

namespace {

void usage(const char* program){
    std::cerr<<"Usage: "<<program
             <<" --job <name> --task <i> --reducers <R> --output-dir <dir> <input>\n";
}

}  // namespace

/**
 * V3.2 Map worker process. Maps one input file and partitions the output with
 * fnv(key) % R, writing one file per reduce partition
 * (<dir>/map-<task>-part-<r>.txt). All R files are created, even if empty, so
 * the coordinator can hand each reducer its files unconditionally.
 */
int main(int argc,char** argv){
    std::string jobName;
    std::string outputDir;
    std::size_t task=0;
    std::size_t reducers=1;
    bool hasTask=false;
    std::vector<std::string> positional;

    for(int i=1;i<argc;++i){
        const std::string arg=argv[i];
        if(arg=="--job"&&i+1<argc){
            jobName=argv[++i];
        }else if(arg=="--task"&&i+1<argc){
            task=std::stoul(argv[++i]);
            hasTask=true;
        }else if(arg=="--reducers"&&i+1<argc){
            reducers=std::stoul(argv[++i]);
        }else if(arg=="--output-dir"&&i+1<argc){
            outputDir=argv[++i];
        }else{
            positional.emplace_back(arg);
        }
    }

    if(jobName.empty()||outputDir.empty()||positional.size()!=1
       ||!hasTask||reducers==0){
        usage(argv[0]);
        return 2;
    }

    const Job* job=findJob(jobName);
    if(job==nullptr){
        std::cerr<<"map_worker: unknown job '"<<jobName<<"'\n";
        return 2;
    }

    try{
        MapReduce jobRunner(job->mapper,job->reducer);
        auto pairs=jobRunner.Map({positional[0]});

        std::vector<std::vector<KeyValue>> parts(reducers);
        for(auto& pair:pairs){
            parts[partitionOf(pair.first,reducers)].push_back(std::move(pair));
        }

        const std::filesystem::path dir=outputDir;
        for(std::size_t r=0;r<reducers;++r){
            WriteKeyValues(mrapp::mapPartPath(dir,task,r).string(),parts[r]);
        }
    }catch(const std::exception& error){
        std::cerr<<"map_worker: "<<error.what()<<'\n';
        return 1;
    }
    return 0;
}
