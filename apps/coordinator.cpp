#include"net/net.h"
#include"runtime/jobs.h"
#include"runtime/paths.h"
#include"runtime/scheduler.h"
#include"runtime/task.h"

#include<algorithm>
#include<cerrno>
#include<cstdint>
#include<filesystem>
#include<iostream>
#include<iterator>
#include<poll.h>
#include<stdexcept>
#include<string>
#include<utility>
#include<vector>

#include<unistd.h>

#include<sys/socket.h>

namespace fs=std::filesystem;

namespace {

void usage(const char* program){
    std::cerr<<"Usage: "<<program
             <<" --job <name> [--reducers <R>] [--workers <N>] [--listen <host:port>]"
             <<" [--work-dir <dir>] --output <file> <input...>\n";
}

std::pair<std::string,std::uint16_t> parseEndpoint(const std::string& endpoint){
    const auto colon=endpoint.rfind(':');
    if(colon==std::string::npos){
        throw std::runtime_error("expected host:port in '"+endpoint+"'");
    }
    return {endpoint.substr(0,colon),
            static_cast<std::uint16_t>(std::stoul(endpoint.substr(colon+1)))};
}

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

mrapp::TaskKind kindOf(const std::string& token){
    if(token=="MAP"){
        return mrapp::TaskKind::Map;
    }
    if(token=="REDUCE"){
        return mrapp::TaskKind::Reduce;
    }
    throw std::runtime_error("bad task kind '"+token+"'");
}

std::vector<std::uint8_t> toBytes(const std::string& text){
    return std::vector<std::uint8_t>(text.begin(),text.end());
}

std::string toString(const std::vector<std::uint8_t>& bytes){
    return std::string(bytes.begin(),bytes.end());
}

struct Worker {
    xmr::net::Connection connection;
    bool idle=false;  // sent REQUEST, waiting for a task
};

}  // namespace

/**
 * V4.1 coordinator: a single-threaded control plane. It listens for persistent
 * workers, hands them map tasks then reduce tasks through an in-process
 * Scheduler, and merges the reduce part files into the final output. The
 * intermediate data still travels through the shared work dir; moving it to
 * TCP is V4.2.
 *
 *   worker ──connect──> coordinator
 *   worker ──REQUEST──> coordinator ──TASK──> worker ──DONE──> coordinator
 *          (map barrier) then (reduce barrier), then STOP + merge
 */
int main(int argc,char** argv){
    std::string jobName;
    std::string outputFile;
    std::string listen="127.0.0.1:0";
    fs::path workDir;
    std::size_t reducers=3;
    std::size_t expectedWorkers=1;
    std::vector<std::string> inputs;

    for(int i=1;i<argc;++i){
        const std::string arg=argv[i];
        if(arg=="--job"&&i+1<argc){
            jobName=argv[++i];
        }else if(arg=="--reducers"&&i+1<argc){
            reducers=std::stoul(argv[++i]);
        }else if(arg=="--workers"&&i+1<argc){
            expectedWorkers=std::stoul(argv[++i]);
        }else if(arg=="--listen"&&i+1<argc){
            listen=argv[++i];
        }else if(arg=="--work-dir"&&i+1<argc){
            workDir=argv[++i];
        }else if(arg=="--output"&&i+1<argc){
            outputFile=argv[++i];
        }else{
            inputs.emplace_back(arg);
        }
    }

    if(jobName.empty()||outputFile.empty()||inputs.empty()
       ||reducers==0||expectedWorkers==0){
        usage(argv[0]);
        return 2;
    }

    if(findJob(jobName)==nullptr){
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

        const auto [host,port]=parseEndpoint(listen);
        xmr::net::Listener listener(host,port);
        // Report the real port (--listen port 0 picks an ephemeral one) so the
        // caller knows where to point the workers.
        std::cout<<"LISTENING "<<host<<":"<<listener.port()<<std::endl;

        mrapp::Scheduler scheduler(jobName,inputs,reducers,workDir.string(),outputFile);
        std::vector<Worker> workers;

        auto handleMessage=[&](Worker& worker){
            const std::vector<std::string> fields=
                splitTab(toString(worker.connection.receive()));
            if(fields.empty()){
                throw std::runtime_error("empty control message");
            }
            if(fields[0]=="HELLO"){
                // Registration only; the worker asks for work next.
            }else if(fields[0]=="REQUEST"){
                worker.idle=true;
            }else if(fields[0]=="DONE"&&fields.size()>=3){
                scheduler.markDone(kindOf(fields[1]));
                worker.idle=false;
            }else if(fields[0]=="FAIL"&&fields.size()>=3){
                const std::string reason=fields.size()>=4?fields[3]:"unknown";
                scheduler.markFailed(kindOf(fields[1]),std::stoul(fields[2]),reason);
                worker.idle=false;
            }else{
                throw std::runtime_error("unexpected control message '"+fields[0]+"'");
            }
        };

        auto dispatch=[&]{
            // Hold work until the expected workers have all connected, so a
            // fast worker cannot finish the job before the others join.
            if(workers.size()<expectedWorkers){
                return;
            }
            for(auto& worker:workers){
                if(!worker.idle){
                    continue;
                }
                auto task=scheduler.takeTask();
                if(!task){
                    break;
                }
                worker.connection.send(toBytes("TASK\t"+task->serialize()));
                worker.idle=false;
            }
        };

        while(!scheduler.finished()&&!scheduler.failed()){
            std::vector<pollfd> fds;
            fds.push_back(pollfd{listener.fd(),POLLIN,0});
            for(const auto& worker:workers){
                fds.push_back(pollfd{worker.connection.fd(),POLLIN,0});
            }

            const int ready=::poll(fds.data(),static_cast<nfds_t>(fds.size()),-1);
            if(ready<0){
                if(errno==EINTR){
                    continue;
                }
                throw std::runtime_error("poll failed");
            }

            for(std::size_t index=1;index<fds.size();++index){
                if(fds[index].revents&(POLLIN|POLLHUP|POLLERR)){
                    handleMessage(workers[index-1]);
                }
            }

            if(fds[0].revents&POLLIN){
                workers.push_back(Worker{listener.accept(),false});
            }

            dispatch();
        }

        for(auto& worker:workers){
            try{
                worker.connection.send(toBytes("STOP"));
                // Half-close, then drain any in-flight REQUEST so closing the
                // socket does not reset the connection underneath the worker.
                ::shutdown(worker.connection.fd(),SHUT_WR);
                char buffer[256];
                while(::read(worker.connection.fd(),buffer,sizeof(buffer))>0){
                }
            }catch(const std::exception&){
                // Worker already disconnected; nothing to do at shutdown.
            }
        }

        if(scheduler.failed()){
            std::cerr<<"coordinator: "<<scheduler.error()<<'\n';
            return 1;
        }

        // Merge the (already individually sorted) part files into one output.
        std::vector<KeyValue> merged;
        for(std::size_t r=0;r<reducers;++r){
            auto pairs=ReadKeyValues(mrapp::reducePartPath(workDir,r).string());
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
