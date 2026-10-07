#include"runtime/jobs.h"

#include<deque>
#include<stdexcept>
#include<string>
#include<utility>

namespace {
    // 把运行时Job动态库缓存起来
    std::deque<Job>& registry() {
        static std::deque<Job> jobs;
        return jobs;
    }
} // namespace

const Job* findJob(const std::string& name) {
    for (const auto& job : registry()) {
        if (job.name == name) {
            return &job;
        }
    }
    return nullptr;
}

void registerJob(Job job) {
    for (const auto& existing : registry()) {
        if (existing.name == job.name) {
            throw std::runtime_error("job '" + job.name + "' is already registered");
        }
    }
    registry().push_back(std::move(job));
}