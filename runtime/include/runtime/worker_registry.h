#pragma once

#include"net/timer.h"
#include"runtime/task.h"

#include<chrono>
#include<cstddef>
#include<optional>
#include<string>
#include<unordered_map>
#include<vector>

namespace xmr {
    // worker节点状态
    enum class WorkerState {
        /**
         * worker已注册 但还没要任务
         * 既不是可用 也不是忙碌
         */
        Registered,
        // 活着且已请求任务 等派发
        Idle,
        // 活着且在执行任务
        Busy,
        /**
         * worker节点在心跳阈值内没给master发送心跳就会被master判定主观下线
         * Registered->Idle->Busy
         * 任一环节超时都会变成Lost
         */
        Lost,
    };

    // master负责worker资源管理+任务调度 把资源管理抽象出来
    class WorkerRegistry {
    public:
        using Clock = net::TimerQueue::Clock;
        using TimePoint = net::TimerQueue::TimePoint;

        /**
         * @param heartbeatTimeout worker心跳超时的阈值是多少
         */
        explicit WorkerRegistry(std::chrono::milliseconds heartbeatTimeout);

        /**
         * @param id worker的id
         * @param now
         */
        bool add(const std::string& id, TimePoint now);

        /**
         * 移除worker的注册
         * @param id worker id
         */
        bool remove(const std::string& id);

        /**
         * 刷新对worker的心跳看门狗
         * @param id worker
         */
        bool touch(const std::string& id, TimePoint now);

        /**
         * master把worker标记空闲
         * @param id worker
         */
        bool markIdle(const std::string& id);

        /**
         * master找到空闲的worker给它派个任务
         * @param task 什么任务
         */
        std::optional<std::string> assignNext(const Task& task);

        /**
         * worker给master汇报Done后 master把worker状态置到初始
         * 不是直接置到空闲 什么时候才叫真正的空闲 是worker跟master要任务没要到的时候才叫空闲
         * @param id worker
         */
        bool complete(const std::string& id);

        /**
         * @param now 看看now这个时间有没有看门狗任务到期了
         * @return 被master判定下线的worker
         */
        std::vector<std::string> pollExpired(TimePoint now);

        /**
         * 基于now最近的一个看门狗啥时候执行
         */
        int nextTimeoutMs(TimePoint now);

        /**
         * 看看worker有没有注册在master上
         * @param id worker
         */
        bool contains(const std::string& id) const;

        /**
         * 看看worker状态
         * @param id worker
         */
        WorkerState state(const std::string& id) const;

        /**
         * @param id worker
         * @return worker的任务是什么
         */
        std::optional<Task> taskOf(const std::string& id) const;

        /**
         * master上注册了多少个worker
         */
        std::size_t size() const;

        // 看看有没有在等任务的worker
        bool hasIdle() const;

    private:
        struct Entry {
            WorkerState state = WorkerState::Registered;
            TimePoint lastSeen;
            // master对worker心跳看门狗定时任务编号 定时任务编号0是哨兵无效值 有效值是从1开始的
            net::TimerQueue::TimerId watchdog = net::TimerQueue::kInvalidId;
            // worker处理的任务
            std::optional<Task> task;
        };

        /**
         * master刷新它给worker的心跳看门狗服务
         * 调用时机
         *   - woker注册到master后
         *   - master收到worker心跳后
         * @param id worker的id
         * @param entry worker
         * @param worker给master上报的心跳时间
         */
        void arm(const std::string& id, Entry& entry, TimePoint now);

        /**
         * master对worker心跳的看门狗服务到期后会调用这个回调
         * 把worker判定下线了
         * @param id worker
         */
        void onTimeout(const std::string& id);

        // master管理着注册进来的worker
        std::unordered_map<std::string, Entry> workers_;
        // 定时任务队列
        net::TimerQueue timers_;
        // master判定主观下线的worker
        std::vector<std::string> lost_;
        // worker的心跳超时阈值 超过这个时间master没有收到worker的心跳就判定crash了
        std::chrono::milliseconds heartbeatTimeout_;
    };
} // namespace xmr
