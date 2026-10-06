#include"runtime/scheduler.h"

#include"runtime/paths.h"

#include<stdexcept>
#include<utility>

namespace mrapp {

Scheduler::Scheduler(std::string job,
                     std::vector<std::string> inputs,
                     std::size_t reducers,
                     std::string workDir,
                     std::string output)
    :job_(std::move(job)),
     inputs_(std::move(inputs)),
     reducers_(reducers),
     workDir_(std::move(workDir)),
     output_(std::move(output)){
    if(reducers_==0){
        throw std::runtime_error("reducers must be > 0");
    }
    if(inputs_.empty()){
        throw std::runtime_error("at least one input is required");
    }
}

std::optional<Task> Scheduler::takeTask(){
    if(phase_==Phase::Map&&nextMap_<inputs_.size()){
        Task task;
        task.kind=TaskKind::Map;
        task.id=nextMap_;
        task.job=job_;
        task.reducers=reducers_;
        task.workDir=workDir_;
        task.input=inputs_[nextMap_];
        ++nextMap_;
        return task;
    }
    if(phase_==Phase::Reduce&&nextReduce_<reducers_){
        Task task;
        task.kind=TaskKind::Reduce;
        task.id=nextReduce_;
        task.job=job_;
        task.reducers=reducers_;
        task.workDir=workDir_;
        task.output=reducePartPath(workDir_,nextReduce_).string();
        for(std::size_t i=0;i<inputs_.size();++i){
            task.intermediates.push_back(mapPartPath(workDir_,i,nextReduce_).string());
        }
        ++nextReduce_;
        return task;
    }
    return std::nullopt;
}

void Scheduler::markDone(TaskKind kind){
    if(kind==TaskKind::Map){
        ++mapDone_;
    }else{
        ++reduceDone_;
    }
    advanceIfPhaseComplete();
}

void Scheduler::markFailed(TaskKind kind,std::size_t id,std::string reason){
    phase_=Phase::Failed;
    error_=std::string(kind==TaskKind::Map?"map":"reduce")
           +" task "+std::to_string(id)+" failed: "+std::move(reason);
}

void Scheduler::advanceIfPhaseComplete(){
    if(phase_==Phase::Map&&mapDone_==inputs_.size()){
        phase_=Phase::Reduce;
    }
    if(phase_==Phase::Reduce&&reduceDone_==reducers_){
        phase_=Phase::Done;
    }
}

}  // namespace mrapp
