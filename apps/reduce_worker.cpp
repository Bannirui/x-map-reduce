#include"jobs.h"
#include"mapreduce.h"

#include<iostream>
#include<string>
#include<utility>
#include<vector>

namespace {

void usage(const char* program){
    std::cerr<<"Usage: "<<program
             <<" --job <name> --index <r> --reducers <R> --output <file> <intermediate...>\n";
}

}  // namespace

/**
 * V3.1 Reduce worker process. Reads every intermediate file, keeps only the
 * keys that belong to partition `index` out of `reducers` (fnv(key) % R), and
 * reduces them into a sorted part file. V3.2 will make map workers pre-split
 * the intermediates so a reducer only reads its own files.
 */
int main(int argc,char** argv){
    std::string jobName;
    std::string outputFile;
    std::size_t index=0;
    std::size_t reducers=1;
    bool hasIndex=false;
    std::vector<std::string> intermediates;

    for(int i=1;i<argc;++i){
        const std::string arg=argv[i];
        if(arg=="--job"&&i+1<argc){
            jobName=argv[++i];
        }else if(arg=="--index"&&i+1<argc){
            index=std::stoul(argv[++i]);
            hasIndex=true;
        }else if(arg=="--reducers"&&i+1<argc){
            reducers=std::stoul(argv[++i]);
        }else if(arg=="--output"&&i+1<argc){
            outputFile=argv[++i];
        }else{
            intermediates.emplace_back(arg);
        }
    }

    if(jobName.empty()||outputFile.empty()||intermediates.empty()
       ||!hasIndex||reducers==0||index>=reducers){
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
            for(auto& pair:pairs){
                if(partitionOf(pair.first,reducers)==index){
                    intermediate.push_back(std::move(pair));
                }
            }
        }
        MapReduce jobRunner(job->mapper,job->reducer);
        jobRunner.ShuffleAndReduce(std::move(intermediate),outputFile);
    }catch(const std::exception& error){
        std::cerr<<"reduce_worker: "<<error.what()<<'\n';
        return 1;
    }
    return 0;
}
