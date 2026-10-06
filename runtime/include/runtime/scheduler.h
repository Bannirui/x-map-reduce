#pragma once

#include"runtime/task.h"

#include<cstddef>
#include<optional>
#include<string>
#include<vector>

namespace mrapp {

enum class Phase { Map, Reduce, Done, Failed };

// Pure control-plane state machine: hands out the map tasks, waits for the map
// barrier, then hands out the reduce tasks. It holds no sockets and does no
// I/O, so it can be reasoned about (and tested) on its own.
class Scheduler {
public:
    Scheduler(std::string job,
              std::vector<std::string> inputs,
              std::size_t reducers);

    // Next task to dispatch, or nullopt when nothing is currently available:
    // either everything has been handed out (waiting on in-flight tasks) or the
    // job is finished/failed.
    std::optional<Task> takeTask();

    void markDone(TaskKind kind);
    void markFailed(TaskKind kind,std::size_t id,std::string reason);

    Phase phase() const{ return phase_; }
    bool finished() const{ return phase_==Phase::Done; }
    bool failed() const{ return phase_==Phase::Failed; }
    const std::string& error() const{ return error_; }

private:
    void advanceIfPhaseComplete();

    std::string job_;
    std::vector<std::string> inputs_;
    std::size_t reducers_;

    Phase phase_=Phase::Map;
    std::size_t nextMap_=0;
    std::size_t nextReduce_=0;
    std::size_t mapDone_=0;
    std::size_t reduceDone_=0;
    std::string error_;
};

}  // namespace mrapp
