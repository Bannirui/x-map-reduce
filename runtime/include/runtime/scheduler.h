#pragma once

#include"runtime/task.h"

#include<cstddef>
#include<optional>
#include<string>
#include<vector>

namespace xmr {
    // 任务的类型 要么是map任务 要么是reduce任务 从任务视角 要么在执行map 要么在执行reduce
    enum class Phase { Map, Reduce, Done, Failed };

    // master负责worker资源管理 任务调度 把任务调度抽象封装出来
    class Scheduler {
    public:
        /**
         * @param job 名称 唯一索引 能用名称找到对应的so动态库的函数
         * @param inputs 要处理的数据 一行一行的格式
         * @param reducers R map要分区的依据
         */
        Scheduler(std::string job, std::vector<std::string> inputs, std::size_t reducers);

        /**
         * 开放给master用 拿个任务派发给空闲worker
         * @return master拿到的任务 可能是空的
         *               什么时候是空的
         *               1 当前阶段都派发出去了 比如map阶段map任务都派发出去了 reduce阶段reduce任务都派发出去了
         *               2 整个任务执行成功了
         *               3 整个任务执行失败了
         *               4 处在reduce阶段和map阶段的屏障期间 reduce要等所有map执行结束
         */
        std::optional<Task> takeTask();

        void markDone(TaskKind kind);

        void markFailed(TaskKind kind, std::size_t id, std::string reason);

        Phase phase() const {
            return phase_;
        }

        bool finished() const {
            return phase_ == Phase::Done;
        }

        bool failed() const {
            return phase_ == Phase::Failed;
        }

        const std::string& error() const {
            return error_;
        }

    private:
        void advanceIfPhaseComplete();

        // 任务名称 唯一索引
        std::string job_;
        // M map任务的数量 由输入数据规模决定
        std::vector<std::string> inputs_;
        // R reduce任务的数量 由客户端指定 map输出分区的依据
        std::size_t reducers_;

        // 任务执行阶段 reduce任务和map任务有屏障 reduce要等map全部执行完
        Phase phase_ = Phase::Map;
        // M个map任务 也就是map的id=[0...M) 记录map任务的id编号
        std::size_t nextMap_ = 0;
        // R个reduce任务 也就是reduce的id=[0...R) 记录reduce任务的id编号
        std::size_t nextReduce_ = 0;
        std::size_t mapDone_ = 0;
        std::size_t reduceDone_ = 0;
        std::string error_;
    };
} // namespace xmr