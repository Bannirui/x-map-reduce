#pragma once

#include"mapreduce/mapreduce.h"

#include<cstddef>
#include<cstdint>
#include<functional>
#include<string>
#include<vector>

namespace xmr {
    enum class TaskKind { Map, Reduce };

    // 任务的抽象 可能是map任务 也可能是reduce任务
    struct Task {
        // 任务类型 是map任务还是reduce任务
        TaskKind kind = TaskKind::Map;
        /**
         * map任务 map id 用来索引输入数据的
         * reduce任务 reduce id 用来索引map输出的分区
         */
        std::size_t id = 0;
        /**
         * 任务的尝试序号
         * 同一(kind,id)的第几次执行 用来区分 同一任务的多次尝试 从而支持重发和挡住迟到结果。
         * 默认0的含义 0是个从未执行的哨兵 有效值从1开始
         */
        std::uint32_t attempt = 0;
        // 任务名称 唯一索引
        std::string job;
        // R map输出分区的依据
        std::size_t reducers = 1;
        // M 输入文件数决定多少个map任务 也就决定将来reduce要从多少个worker里面拉数据
        std::size_t maps = 0;
        // map任务才要关注 map任务的输入[key,value]的key
        std::string input;
    };

    // Data-plane encoding: the same "key\tvalue\n" layout the old files used, just
    // carried inside a message instead of a file.
    std::string serializeKeyValues(const std::vector<KeyValue>& pairs);

    std::vector<KeyValue> deserializeKeyValues(const std::string& blob);

    /**
     * @param task map任务
     * @param content map任务的输入[key,value]的value
     * @return map产出的中间结果 已经按照R分区好了 现在还放在worker的内存上 等着shuffle
     */
    std::vector<std::vector<KeyValue> > runMapTask(const Task& task, const std::string& content);

    // Reduce task: fetch each map task's partition through `fetch`, then
    // shuffle+reduce into the final key-ordered pairs.
    std::vector<KeyValue> runReduceTask(const Task& task,
                                        const std::function<std::vector<KeyValue>(
                                            std::size_t mapTask, std::size_t partition)>& fetch);
} // namespace xmr