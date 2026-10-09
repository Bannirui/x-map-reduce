#pragma once

#include"runtime/task.h"

#include<cstddef>
#include<cstdint>
#include<deque>
#include<optional>
#include<string>
#include<vector>

namespace xmr {
    // 任务的类型 要么是map任务 要么是reduce任务 从任务视角 要么在执行map 要么在执行reduce
    enum class Phase {
        Map,
        // map跟reduce之间屏障 必须执行完所有的map任务 才进入到reduce环节
        Reduce, Done, Failed
    };

    // master负责worker资源管理 任务调度 把任务调度抽象封装出来
    class Scheduler {
    public:
        /**
         * @param job 名称 唯一索引 能用名称找到对应的so动态库的函数
         * @param inputs 要处理的数据 一行一行的格式
         * @param reducers R map要分区的依据
         * @param maxAttempts 单个任务最多的尝试次数
         */
        Scheduler(std::string job, std::vector<std::string> inputs, std::size_t reducers, std::uint32_t maxAttempts = 4);

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

        /**
         * 给一个已经在跑的任务再发一个重复attempt(推测执行)
         * @return 新的attempt任务 超次数或任务已完成返回空
         */
        std::optional<Task> speculate(TaskKind kind, std::size_t id);

        /**
         * 放到任务 这个任务要重新派发
         */
        bool retry(const Task& task);

        /**
         * @param kind map任务还是reduce任务
         * @param id 任务id
         * @param attempt
         */
        bool markDone(TaskKind kind, std::size_t id, std::uint32_t attempt);

        bool invalidate(TaskKind kind, std::size_t id);

        void markFailed(TaskKind kind, std::size_t id, std::string reason);

        /**
         * @param kind map任务还是reduce任务
         * @param id 任务id
         * @return 任务正在被第几次尝试
         */
        std::uint32_t attemptOf(TaskKind kind, std::size_t id) const;

        /**
         * 当前处于什么阶段 执行map还是执行reduce
         */
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
        struct Entry {
            // 已经签发到的尝试号 每次派发(含推测)都自增
            std::uint32_t nextAttempt = 0;
            // 正在执行、还没结束的attempt 推测执行时同一任务可能有多个
            std::vector<std::uint32_t> inFlight;
            // 完成 不再参与派发
            bool done = false;
        };

        /**
         * 索引任务
         * @param kind map任务还是reduce任务
         * @param id 任务id
         */
        Entry& entryOf(TaskKind kind, std::size_t id);

        const Entry& entryOf(TaskKind kind, std::size_t id) const;

        Task makeTask(TaskKind kind, std::size_t id, std::uint32_t attempt) const;

        // 每每有任务执行完 都可能要推进任务管理器的状态
        void advanceIfPhaseComplete();

        // 任务名称 唯一索引
        std::string job_;
        // M map任务的数量 由输入数据规模决定
        std::vector<std::string> inputs_;
        // R reduce任务的数量 由客户端指定 map输出分区的依据
        std::size_t reducers_;
        // 单个任务最多尝试的次数
        std::uint32_t maxAttempts_;

        // 任务执行阶段 reduce任务和map任务有屏障 reduce要等map全部执行完
        Phase phase_ = Phase::Map;
        // M个map任务 也就是map的id=[0...M) 记录map任务的id编号
        std::deque<std::size_t> mapPending_;
        // R个reduce任务 也就是reduce的id=[0...R) 记录reduce任务的id编号
        std::deque<std::size_t> reducePending_;
        // map类型任务
        std::vector<Entry> mapEntries_;
        // reduce类型任务
        std::vector<Entry> reduceEntries_;
        // 多少个map任务被完成
        std::size_t mapDone_ = 0;
        // 多少个reduce任务被完成
        std::size_t reduceDone_ = 0;
        std::string error_;
    };
} // namespace xmr
