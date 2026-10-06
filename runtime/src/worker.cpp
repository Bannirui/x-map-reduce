#include"net/net.h"
#include"runtime/task.h"

#include<cstdint>
#include<iostream>
#include<stdexcept>
#include<string>
#include<utility>
#include<vector>

namespace {

std::vector<std::uint8_t> toBytes(const std::string& text){
    return std::vector<std::uint8_t>(text.begin(),text.end());
}

std::string toString(const std::vector<std::uint8_t>& bytes){
    return std::string(bytes.begin(),bytes.end());
}

std::pair<std::string,std::string> parseEndpoint(const std::string& endpoint){
    const auto colon=endpoint.rfind(':');
    if(colon==std::string::npos){
        throw std::runtime_error("expected host:port in '"+endpoint+"'");
    }
    return {endpoint.substr(0,colon),endpoint.substr(colon+1)};
}

std::string kindToken(mrapp::TaskKind kind){
    return kind==mrapp::TaskKind::Map?"MAP":"REDUCE";
}

void usage(const char* program){
    std::cerr<<"Usage: "<<program<<" --coordinator <host:port>\n";
}

}  // namespace

/**
 * V4.2 persistent worker: connect to the coordinator, then repeatedly request a
 * task, run it, and report the result until told STOP. Map output and reduce
 * input/results travel over this same TCP connection (the data plane); no
 * shared filesystem is involved.
 *
 *   map    : TASK -> run -> MAPOUT (per partition) -> DONE
 *   reduce : TASK -> FETCH each map partition -> DATA -> RESULT -> DONE
 */
int main(int argc,char** argv){
    std::string coordinator;

    for(int i=1;i<argc;++i){
        const std::string arg=argv[i];
        if(arg=="--coordinator"&&i+1<argc){
            coordinator=argv[++i];
        }
    }

    if(coordinator.empty()){
        usage(argv[0]);
        return 2;
    }

    try{
        const auto [host,portText]=parseEndpoint(coordinator);
        const std::uint16_t port=static_cast<std::uint16_t>(std::stoul(portText));
        xmr::net::Connection connection=xmr::net::connectTo(host,port);
        connection.send(toBytes("HELLO"));

        while(true){
            connection.send(toBytes("REQUEST"));
            const std::string message=toString(connection.receive());
            if(message=="STOP"){
                break;
            }
            if(message.rfind("TASK\t",0)!=0){
                throw std::runtime_error("unexpected coordinator message");
            }

            const mrapp::Task task=mrapp::Task::deserialize(message.substr(5));
            try{
                if(task.kind==mrapp::TaskKind::Map){
                    const auto parts=mrapp::runMapTask(task);
                    for(std::size_t r=0;r<parts.size();++r){
                        connection.send(toBytes("MAPOUT\t"+std::to_string(task.id)+"\t"
                            +std::to_string(r)+"\t"+mrapp::serializeKeyValues(parts[r])));
                    }
                }else{
                    auto fetch=[&](std::size_t mapTask,std::size_t partition){
                        connection.send(toBytes("FETCH\t"+std::to_string(mapTask)
                            +"\t"+std::to_string(partition)));
                        const std::string data=toString(connection.receive());
                        if(data.rfind("DATA\t",0)!=0){
                            throw std::runtime_error("expected DATA reply from coordinator");
                        }
                        return mrapp::deserializeKeyValues(data.substr(5));
                    };
                    const auto result=mrapp::runReduceTask(task,fetch);
                    connection.send(toBytes("RESULT\t"+std::to_string(task.id)
                        +"\t"+mrapp::serializeKeyValues(result)));
                }
                connection.send(toBytes("DONE\t"+kindToken(task.kind)+"\t"+std::to_string(task.id)));
            }catch(const std::exception& error){
                connection.send(toBytes("FAIL\t"+kindToken(task.kind)+"\t"
                    +std::to_string(task.id)+"\t"+error.what()));
            }
        }
    }catch(const std::exception& error){
        std::cerr<<"worker: "<<error.what()<<'\n';
        return 1;
    }
    return 0;
}
