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
 * V4.1 persistent worker: connect to the coordinator, then repeatedly request a
 * task, run it, and report the result until the coordinator says STOP. Workers
 * are launched by the user and outlive a single task; the coordinator pushes
 * work to whichever worker has requested it (so workers never busy-wait).
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
                throw std::runtime_error("unexpected coordinator message '"+message+"'");
            }

            const mrapp::Task task=mrapp::Task::deserialize(message.substr(5));
            try{
                mrapp::executeTask(task);
                connection.send(toBytes(
                    "DONE\t"+kindToken(task.kind)+"\t"+std::to_string(task.id)));
            }catch(const std::exception& error){
                connection.send(toBytes(
                    "FAIL\t"+kindToken(task.kind)+"\t"
                    +std::to_string(task.id)+"\t"+error.what()));
            }
        }
    }catch(const std::exception& error){
        std::cerr<<"worker: "<<error.what()<<'\n';
        return 1;
    }
    return 0;
}
