#include"net/net.h"
#include"runtime/jobs.h"
#include"runtime/scheduler.h"
#include"runtime/task.h"

#include<algorithm>
#include<cerrno>
#include<cstdint>
#include<fstream>
#include<iostream>
#include<iterator>
#include<poll.h>
#include<stdexcept>
#include<string>
#include<utility>
#include<vector>

#include<sys/socket.h>
#include<unistd.h>

namespace {

void usage(const char* program){
    std::cerr<<"Usage: "<<program
             <<" --job <name> [--reducers <R>] [--workers <N>] [--listen <host:port>]"
             <<" --output <file> <input...>\n";
}

std::pair<std::string,std::uint16_t> parseEndpoint(const std::string& endpoint){
    const auto colon=endpoint.rfind(':');
    if(colon==std::string::npos){
        throw std::runtime_error("expected host:port in '"+endpoint+"'");
    }
    return {endpoint.substr(0,colon),
            static_cast<std::uint16_t>(std::stoul(endpoint.substr(colon+1)))};
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

// Split off the first `fields` tab-separated fields; `rest` (which may itself
// contain tabs and newlines) is whatever follows. This is how the messages that
// carry a key/value blob are parsed without corrupting the blob.
std::vector<std::string> splitHead(const std::string& text,std::size_t fields,std::string& rest){
    std::vector<std::string> head;
    std::size_t start=0;
    for(std::size_t i=0;i<fields;++i){
        const std::size_t tab=text.find('\t',start);
        if(tab==std::string::npos){
            head.push_back(text.substr(start));
            rest.clear();
            return head;
        }
        head.push_back(text.substr(start,tab-start));
        start=tab+1;
    }
    rest=text.substr(start);
    return head;
}

struct Worker {
    xmr::net::Connection connection;
    bool idle=false;  // sent REQUEST, waiting for a task
};

}  // namespace

/**
 * V4.2 coordinator: a single-threaded control *and* data plane. It hands map
 * then reduce tasks to persistent workers, relays map output to the reducers
 * that fetch it, and merges the reduce results in memory into the final output.
 * No shared work dir / intermediate files are used.
 *
 *   map worker   --MAPOUT(task,part,blob)--> coordinator
 *   reduce worker --FETCH(task,part)-------> coordinator --DATA(blob)-->
 *   reduce worker --RESULT(part,blob)------> coordinator  (merged -> --output)
 */
int main(int argc,char** argv){
    std::string jobName;
    std::string outputFile;
    std::string listen="127.0.0.1:0";
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

    try{
        // V4.3: the coordinator owns the inputs (it is the submitter); each
        // worker fetches its split over TCP instead of reading a local path.
        std::vector<std::string> inputData;
        inputData.reserve(inputs.size());
        for(const auto& path:inputs){
            std::ifstream in(path,std::ios::binary);
            if(!in){
                throw std::runtime_error("failed to open input file: "+path);
            }
            inputData.emplace_back(
                std::istreambuf_iterator<char>(in),
                std::istreambuf_iterator<char>());
        }

        const auto [host,port]=parseEndpoint(listen);
        xmr::net::Listener listener(host,port);
        // Report the real port (--listen port 0 picks an ephemeral one) so the
        // caller knows where to point the workers.
        std::cout<<"LISTENING "<<host<<":"<<listener.port()<<std::endl;

        mrapp::Scheduler scheduler(jobName,inputs,reducers);
        std::vector<Worker> workers;

        // Shuffle state (data plane): mapOutput[mapTask][partition] and the
        // accumulated reduce results.
        std::vector<std::vector<std::string>> mapOutput(
            inputs.size(),std::vector<std::string>(reducers));
        std::vector<KeyValue> merged;

        auto handleMessage=[&](Worker& worker){
            const std::string message=toString(worker.connection.receive());
            std::string rest;
            const std::vector<std::string> head=splitHead(message,1,rest);
            if(head.empty()){
                throw std::runtime_error("empty control message");
            }
            const std::string& command=head[0];

            if(command=="HELLO"){
                // Registration only; the worker asks for work next.
            }else if(command=="REQUEST"){
                worker.idle=true;
            }else if(command=="DONE"){
                std::string tail;
                const auto fields=splitHead(rest,2,tail);
                scheduler.markDone(kindOf(fields[0]));
                worker.idle=false;
            }else if(command=="FAIL"){
                std::string tail;
                const auto fields=splitHead(rest,3,tail);
                scheduler.markFailed(kindOf(fields[0]),std::stoul(fields[1]),fields[2]);
                worker.idle=false;
            }else if(command=="MAPOUT"){
                std::string blob;
                const auto fields=splitHead(rest,2,blob);
                const std::size_t task=std::stoul(fields[0]);
                const std::size_t partition=std::stoul(fields[1]);
                if(task>=mapOutput.size()||partition>=reducers){
                    throw std::runtime_error("MAPOUT out of range");
                }
                mapOutput[task][partition]=std::move(blob);
            }else if(command=="FETCH"){
                std::string tail;
                const auto fields=splitHead(rest,2,tail);
                const std::size_t task=std::stoul(fields[0]);
                const std::size_t partition=std::stoul(fields[1]);
                if(task>=mapOutput.size()||partition>=reducers){
                    throw std::runtime_error("FETCH out of range");
                }
                worker.connection.send(toBytes("DATA\t"+mapOutput[task][partition]));
            }else if(command=="INPUT"){
                std::string tail;
                const auto fields=splitHead(rest,1,tail);
                const std::size_t id=std::stoul(fields[0]);
                if(id>=inputData.size()){
                    throw std::runtime_error("INPUT out of range");
                }
                worker.connection.send(toBytes("DATA\t"+inputData[id]));
            }else if(command=="RESULT"){
                std::string blob;
                const auto fields=splitHead(rest,1,blob);
                auto pairs=mrapp::deserializeKeyValues(blob);
                merged.insert(merged.end(),
                    std::make_move_iterator(pairs.begin()),
                    std::make_move_iterator(pairs.end()));
            }else{
                throw std::runtime_error("unexpected control message '"+command+"'");
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

        std::stable_sort(merged.begin(),merged.end(),
            [](const KeyValue& a,const KeyValue& b){
                return a.first<b.first;
            });
        WriteKeyValues(outputFile,merged);
    }catch(const std::exception& error){
        std::cerr<<"coordinator: "<<error.what()<<'\n';
        return 1;
    }

    return 0;
}
