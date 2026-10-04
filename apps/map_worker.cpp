#include"jobs.h"
#include"mapreduce.h"

#include<iostream>
#include<string>
#include<vector>

namespace {

void usage(const char* program){
    std::cerr<<"Usage: "<<program<<" --job <name> <input> <output>\n";
}

}  // namespace

/**
 * V3 Map worker process. argv-driven so the coordinator can launch it with
 * fork + exec: it maps one input file and writes the intermediate file.
 */
int main(int argc,char** argv){
    std::string jobName;
    std::vector<std::string> positional;
    for(int i=1;i<argc;++i){
        const std::string arg=argv[i];
        if(arg=="--job"&&i+1<argc){
            jobName=argv[++i];
        }else{
            positional.emplace_back(arg);
        }
    }

    if(jobName.empty()||positional.size()!=2){
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
        WriteKeyValues(positional[1],pairs);
    }catch(const std::exception& error){
        std::cerr<<"map_worker: "<<error.what()<<'\n';
        return 1;
    }
    return 0;
}
