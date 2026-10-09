#include"runtime/task.h"

#include"runtime/jobs.h"

#include<iterator>
#include<stdexcept>
#include<utility>
#include<vector>

namespace xmr {
    std::string serializeKeyValues(const std::vector<KeyValue>& pairs) {
        std::string blob;
        for (const auto& [key,value] : pairs) {
            blob += key;
            blob += '\t';
            blob += value;
            blob += '\n';
        }
        return blob;
    }

    std::vector<KeyValue> deserializeKeyValues(const std::string& blob) {
        std::vector<KeyValue> pairs;
        std::size_t start = 0;
        while (start < blob.size()) {
            const std::size_t newline = blob.find('\n', start);
            const std::size_t end = (newline == std::string::npos) ? blob.size() : newline;
            const std::string line = blob.substr(start, end - start);
            const std::size_t tab = line.find('\t');
            if (tab != std::string::npos) {
                pairs.emplace_back(line.substr(0, tab), line.substr(tab + 1));
            }
            if (newline == std::string::npos) {
                break;
            }
            start = newline + 1;
        }
        return pairs;
    }

    std::vector<std::vector<KeyValue> > runMapTask(const Task& task, const std::string& content,
                                                   const std::function<void(double)>& progress) {
        // 根据名称唯一所以找到job的so
        const Job* job = findJob(task.job);
        if (job == nullptr) {
            throw std::runtime_error("unknown job '" + task.job + "'");
        }
        MapReduce runner(job->mapper, job->reducer);
        // map函数执行的中间结果
        auto pairs = runner.MapData(task.input, content, progress);
        // map函数的中间结果按照R分区
        std::vector<std::vector<KeyValue> > parts(task.reducers);
        for (auto& pair : pairs) {
            parts[partitionOf(pair.first, task.reducers)].push_back(std::move(pair));
        }
        return parts;
    }

    std::vector<KeyValue> runReduceTask(const Task& task,
                                        const std::function<std::vector<KeyValue>(std::size_t, std::size_t)>& fetch) {
        const Job* job = findJob(task.job);
        if (job == nullptr) {
            throw std::runtime_error("unknown job '" + task.job + "'");
        }
        std::vector<KeyValue> intermediate;
        for (std::size_t mapTask = 0; mapTask < task.maps; ++mapTask) {
            auto pairs = fetch(mapTask, task.id);
            intermediate.insert(intermediate.end(),
                                std::make_move_iterator(pairs.begin()),
                                std::make_move_iterator(pairs.end()));
        }
        MapReduce runner(job->mapper, job->reducer);
        return runner.Reduce(std::move(intermediate));
    }
} // namespace xmr