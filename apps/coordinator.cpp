#include"jobs.h"
#include"mapreduce.h"

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
             <<" --job <name> --work-dir <dir> --output <file> <input...>\n";
}

// Directory holding this executable, so map_worker is found as a sibling
// regardless of the current working directory.
fs::path executableDir(){
    std::error_code error;
    const auto self=fs::read_symlink("/proc/self/exe",error);
    if(!error&&!self.empty()){
        return self.parent_path();
    }
    return fs::current_path();
}

pid_t spawnMapWorker(const fs::path& worker,
                     const std::string& job,
                     const std::string& input,
                     const std::string& output){
    const pid_t pid=fork();
    if(pid<0){
        throw std::runtime_error("fork failed");
    }
    if(pid==0){
        execl(worker.c_str(),worker.c_str(),
              "--job",job.c_str(),input.c_str(),output.c_str(),
              static_cast<char*>(nullptr));
        // execl only returns on failure; _exit avoids flushing inherited stdio.
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
            throw std::runtime_error("map worker exited abnormally");
        }
    }
}

}  // namespace

/**
 * V3.0 coordinator: forks one map_worker process per input file, waits for all
 * of them, then runs Shuffle + Reduce in-process over the intermediate files.
 *
 *   coordinator ──fork/exec──> map_worker 0 ──> map-0.txt
 *               ──fork/exec──> map_worker 1 ──> map-1.txt
 *               ──fork/exec──> map_worker 2 ──> map-2.txt
 *                              (waitpid all)
 *                                  │
 *               Shuf fle + Reduce over map-*.txt
 * command:
 *   ./coordinator --job word_count \
 *                 --work-dir . \
 *                 --output wc.txt \
 *                 asset/wordCount1.txt asset/wordCount2.txt asset/wordCount3.txt 
 */
int main(int argc,char** argv){
    std::string jobName;
    std::string outputFile;
    fs::path workDir;
    std::vector<std::string> inputs;

    for(int i=1;i<argc;++i){
        const std::string arg=argv[i];
        if(arg=="--job"&&i+1<argc){
            jobName=argv[++i];
        }else if(arg=="--work-dir"&&i+1<argc){
            workDir=argv[++i];
        }else if(arg=="--output"&&i+1<argc){
            outputFile=argv[++i];
        }else{
            inputs.emplace_back(arg);
        }
    }

    if(jobName.empty()||outputFile.empty()||inputs.empty()){
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
        const fs::path worker=executableDir()/"map_worker";

        std::vector<pid_t> children;
        std::vector<fs::path> intermediates;
        for(std::size_t i=0;i<inputs.size();++i){
            const fs::path intermediate=workDir/("map-"+std::to_string(i)+".txt");
            intermediates.push_back(intermediate);
            children.push_back(spawnMapWorker(worker,jobName,inputs[i],intermediate.string()));
        }
        waitFor(children);

        std::vector<KeyValue> merged;
        for(const auto& intermediate:intermediates){
            auto pairs=ReadKeyValues(intermediate.string());
            merged.insert(merged.end(),
                std::make_move_iterator(pairs.begin()),
                std::make_move_iterator(pairs.end()));
        }

        MapReduce reduceRunner(job->mapper,job->reducer);
        reduceRunner.ShuffleAndReduce(std::move(merged),outputFile);
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
