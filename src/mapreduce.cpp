#include"mapreduce.h"

#include<algorithm>
#include<exception>
#include<fstream>
#include<iterator>
#include<map>
#include<stdexcept>
#include<thread>
#include<utility>


namespace {

/**
 * Runs fn(id) on `workers` threads (id in [0, workers)).
 * Workers == 1 runs inline.
 * Exceptions are captured per worker and rethrown on the calling thread after every worker has been joined.
 */
template<class Fn>
void runTasks(std::size_t workers,Fn fn){
    if(workers<=1){
        fn(0);
        return;
    }
    std::vector<std::thread> threads;
    std::vector<std::exception_ptr> errors(workers);
    threads.reserve(workers);
    for(std::size_t id=0;id<workers;++id){
        threads.emplace_back([&,id]{
            try{
                fn(id);
            }catch(...){
                errors[id]=std::current_exception();
            }
        });
    }
    for(auto& thread:threads){
        thread.join();
    }
    for(const auto& error:errors){
        if(error){
            std::rethrow_exception(error);
        }
    }
}

}  // namespace


MapReduce::MapReduce(MapFunction mapper,ReduceFunction reducer,std::size_t workers)
    :mapper_(std::move(mapper)),reducer_(std::move(reducer)),workers_(workers)
{}

void MapReduce::Run(const std::vector<std::string>& inputFiles,const std::string& outFile){
    auto intermediate=this->Map(inputFiles);
    this->ShuffleAndReduce(std::move(intermediate),outFile);
}

void MapReduce::ShuffleAndReduce(std::vector<KeyValue> intermediate,const std::string& outFile){
    auto grouped=this->shufflePhase(std::move(intermediate));
    this->reducePhase(grouped,outFile);
}

/**
 * one Map task per input file (the paper's M tasks). Tasks are distributed
 * round-robin over the worker threads. Each task writes into its own buffer so
 * no synchronization is needed; buffers are merged in file order afterwards.
 *
 * input files:
 *   a.txt        b.txt
 *      │            │
 *      └──── Map ───┘   (worker threads)
 *             │
 *   per-file intermediate data (in memory)
 */
std::vector<KeyValue> MapReduce::Map(const std::vector<std::string>& inputFiles) const{
    const std::size_t workers=this->resolveWorkers(inputFiles.size());
    std::vector<std::vector<KeyValue>> perFile(inputFiles.size());

    runTasks(workers,[&](std::size_t id){
        for(std::size_t taskId=id,taskSz=inputFiles.size();taskId<taskSz;taskId+=workers){
            const auto& filename=inputFiles[taskId];
            std::ifstream input(filename);
            if(!input){
                throw std::runtime_error("Failed to open input file: "+filename);
            }
            auto& intermediate=perFile[taskId];
            std::string line;
            while(std::getline(input,line)){
                auto pairs=this->mapper_(filename,line);
                intermediate.insert(intermediate.end(),
                    std::make_move_iterator(pairs.begin()),
                    std::make_move_iterator(pairs.end()));
            }
        }
    });

    std::vector<KeyValue> intermediate;
    for(auto& file:perFile){
        intermediate.insert(intermediate.end(),
            std::make_move_iterator(file.begin()),
            std::make_move_iterator(file.end()));
    }
    return intermediate;
}

std::map<Key,std::vector<Value>> MapReduce::shufflePhase(std::vector<KeyValue> intermediate){
    /**
     * after sorting:
     *   hello 1
     *   hello 1
     *   world 1
     *   mapreduce 1
     */
    std::sort(intermediate.begin(),intermediate.end(),
        [](const KeyValue& a,const KeyValue& b){
            return a.first<b.first;
        }
    );
    /**
     * after group:
     *   hello [1, 1]
     *   world [1]
     *   mapreduce [1]
     */
    std::map<Key,std::vector<Value>> grouped;
    for(const auto& [k,v]:intermediate){
        grouped[k].push_back(v);
    }
    return grouped;
}

void MapReduce::reducePhase(const std::map<Key,std::vector<Value>>& grouped,const std::string& outputFile){
    std::ofstream output(outputFile);
    if(!output){
        throw std::runtime_error("Failed to open output file: "+outputFile);
    }
    for(const auto& [key,values]:grouped){
        auto results=this->reducer_(key,values);
        for(const auto& [retKey,retValue]:results){
            output<<retKey<<'\t'<<retValue<<'\n';
        }
    }
}

std::size_t MapReduce::resolveWorkers(std::size_t tasks) const{
    if(tasks==0){
        return 1;
    }
    std::size_t workers=this->workers_!=0?this->workers_:std::thread::hardware_concurrency();
    if(workers==0){
        workers=1;
    }
    return std::min(workers,tasks);
}

void WriteKeyValues(const std::string& path,const std::vector<KeyValue>& pairs){
    std::ofstream output(path);
    if(!output){
        throw std::runtime_error("Failed to open intermediate file: "+path);
    }
    for(const auto& [key,value]:pairs){
        output<<key<<'\t'<<value<<'\n';
    }
    if(!output){
        throw std::runtime_error("Failed to write intermediate file: "+path);
    }
}

std::vector<KeyValue> ReadKeyValues(const std::string& path){
    std::ifstream input(path);
    if(!input){
        throw std::runtime_error("Failed to open intermediate file: "+path);
    }
    std::vector<KeyValue> pairs;
    std::string line;
    while(std::getline(input,line)){
        const auto tab=line.find('\t');
        if(tab==std::string::npos){
            continue;
        }
        pairs.emplace_back(line.substr(0,tab),line.substr(tab+1));
    }
    return pairs;
}
