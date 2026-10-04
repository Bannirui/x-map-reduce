#include"jobs.h"
#include"mapreduce.h"

#include<algorithm>
#include<cerrno>
#include<filesystem>
#include<iostream>
#include<iterator>
#include<stdexcept>
#include<string>
#include<vector>

#include<sys/wait.h>
#include<unistd.h>

namespace fs=std::filesystem;

namespace {

void usage(const char* program){
    std::cerr<<"Usage: "<<program
             <<" --job <name> [--reducers <R>] --work-dir <dir> --output <file> <input...>\n";
}

// Directory holding this executable, so map_worker / reduce_worker are found as
// siblings regardless of the current working directory.
fs::path executableDir(){
    std::error_code error;
    const auto self=fs::read_symlink("/proc/self/exe",error);
    if(!error&&!self.empty()){
        return self.parent_path();
    }
    return fs::current_path();
}

pid_t spawnProcess(const fs::path& program,const std::vector<std::string>& args){
    const pid_t pid=fork();
    if(pid<0){
        throw std::runtime_error("fork failed");
    }
    if(pid==0){
        std::vector<char*> argv;
        argv.reserve(args.size()+2);
        argv.push_back(const_cast<char*>(program.c_str()));
        for(const auto& arg:args){
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        argv.push_back(static_cast<char*>(nullptr));
        execv(program.c_str(),argv.data());
        // execv only returns on failure; _exit avoids flushing inherited stdio.
        _exit(127);
    }
    return pid;
}

void waitFor(const std::vector<pid_t>& children){
    for(const pid_t pid:children){
        int status=0;
        while(waitpid(pid,&status,0)==-1){
            if(errno!=EINTR){
                throw std::runtime_error("waitpid failed");
            }
        }
        if(!WIFEXITED(status)||WEXITSTATUS(status)!=0){
            throw std::runtime_error("worker exited abnormally");
        }
    }
}

}  // namespace

/**
 * V3.1 coordinator: forks one map_worker per input file, waits, then forks R
 * reduce_worker processes (each keeps fnv(key) % R == its index), waits, and
 * merges their sorted part files into one key-ordered output.
 *
 *   coordinator ──> map_worker 0..N ──> map-<i>.txt
 *                       (waitpid all)
 *   coordinator ──> reduce_worker 0..R-1 ──> part-<r>.txt
 *                       (waitpid all)
 *                       merge sorted parts -> output
 */
int main(int argc,char** argv){
    std::string jobName;
    std::string outputFile;
    fs::path workDir;
    std::size_t reducers=3;
    std::vector<std::string> inputs;

    for(int i=1;i<argc;++i){
        const std::string arg=argv[i];
        if(arg=="--job"&&i+1<argc){
            jobName=argv[++i];
        }else if(arg=="--reducers"&&i+1<argc){
            reducers=std::stoul(argv[++i]);
        }else if(arg=="--work-dir"&&i+1<argc){
            workDir=argv[++i];
        }else if(arg=="--output"&&i+1<argc){
            outputFile=argv[++i];
        }else{
            inputs.emplace_back(arg);
        }
    }

    if(jobName.empty()||outputFile.empty()||inputs.empty()||reducers==0){
        usage(argv[0]);
        return 2;
    }

    const Job* job=findJob(jobName);
    if(job==nullptr){
        std::cerr<<"coordinator: unknown job '"<<jobName<<"'\n";
        return 2;
    }

    bool ownsWorkDir=false;
    if(workDir.empty()){
        workDir=fs::temp_directory_path()/("x-map-reduce-"+std::to_string(::getpid()));
        ownsWorkDir=true;
    }

    try{
        fs::create_directories(workDir);
        const fs::path execDir=executableDir();

        // Map phase: one process per input file.
        std::vector<pid_t> mapChildren;
        std::vector<fs::path> intermediates;
        for(std::size_t i=0;i<inputs.size();++i){
            const fs::path intermediate=workDir/("map-"+std::to_string(i)+".txt");
            intermediates.push_back(intermediate);
            mapChildren.push_back(spawnProcess(execDir/"map_worker",
                {"--job",jobName,inputs[i],intermediate.string()}));
        }
        waitFor(mapChildren);

        // Reduce phase: R processes, each filtering its partition.
        std::vector<pid_t> reduceChildren;
        std::vector<fs::path> parts;
        for(std::size_t r=0;r<reducers;++r){
            const fs::path part=workDir/("part-"+std::to_string(r)+".txt");
            parts.push_back(part);
            std::vector<std::string> args{
                "--job",jobName,
                "--index",std::to_string(r),
                "--reducers",std::to_string(reducers),
                "--output",part.string(),
            };
            for(const auto& intermediate:intermediates){
                args.push_back(intermediate.string());
            }
            reduceChildren.push_back(spawnProcess(execDir/"reduce_worker",args));
        }
        waitFor(reduceChildren);

        // Merge the (already individually sorted) part files into one output.
        std::vector<KeyValue> merged;
        for(const auto& part:parts){
            auto pairs=ReadKeyValues(part.string());
            merged.insert(merged.end(),
                std::make_move_iterator(pairs.begin()),
                std::make_move_iterator(pairs.end()));
        }
        std::stable_sort(merged.begin(),merged.end(),
            [](const KeyValue& a,const KeyValue& b){
                return a.first<b.first;
            });
        WriteKeyValues(outputFile,merged);
    }catch(const std::exception& error){
        std::cerr<<"coordinator: "<<error.what()<<'\n';
        return 1;
    }

    if(ownsWorkDir){
        std::error_code error;
        fs::remove_all(workDir,error);
    }
    return 0;
}
