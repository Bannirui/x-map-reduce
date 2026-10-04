#include"jobs.h"
#include"mapreduce.h"

#include<iostream>
#include<iterator>
#include<string>
#include<utility>
#include<vector>

namespace {

void usage(const char* program){
    std::cerr<<"Usage: "<<program<<" --job <name> --output <file> <intermediate...>\n";
}

}  // namespace

/**
 * V3.2 Reduce worker process. The map side already partitioned the data, so a
 * reducer just reduces the intermediate files it is given (its own partition
 * across every map task) into a sorted part file.
 */
int main(int argc,char** argv){
    std::string jobName;
    std::string outputFile;
    std::vector<std::string> intermediates;

    for(int i=1;i<argc;++i){
        const std::string arg=argv[i];
        if(arg=="--job"&&i+1<argc){
            jobName=argv[++i];
        }else if(arg=="--output"&&i+1<argc){
            outputFile=argv[++i];
        }else{
            intermediates.emplace_back(arg);
        }
    }

    if(jobName.empty()||outputFile.empty()||intermediates.empty()){
        usage(argv[0]);
        return 2;
    }

    const Job* job=findJob(jobName);
    if(job==nullptr){
        std::cerr<<"reduce_worker: unknown job '"<<jobName<<"'\n";
        return 2;
    }

    try{
        std::vector<KeyValue> intermediate;
        for(const auto& path:intermediates){
            auto pairs=ReadKeyValues(path);
            intermediate.insert(intermediate.end(),
                std::make_move_iterator(pairs.begin()),
                std::make_move_iterator(pairs.end()));
        }
        MapReduce jobRunner(job->mapper,job->reducer);
        jobRunner.ShuffleAndReduce(std::move(intermediate),outputFile);
    }catch(const std::exception& error){
        std::cerr<<"reduce_worker: "<<error.what()<<'\n';
        return 1;
    }
    return 0;
}
