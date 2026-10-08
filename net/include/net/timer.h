#pragma once

#include<algorithm>
#include<chrono>
#include<cstdint>
#include<functional>
#include<stdexcept>
#include<vector>

namespace xmr::net {
    // 定时任务队列 一次性执行/周期性执行
    class TimerQueue {
    public:
        using Clock = std::chrono::steady_clock;
        using TimePoint = Clock::time_point;
        using Duration = Clock::duration;
        using TimerId = std::uint64_t;
        using Callback = std::function<void()>;

        /**
         * 在now+delay后执行
         */
        TimerId addAfter(Duration delay, Callback callback, TimePoint now = Clock::now());

        /**
         * 在now后每隔interval执行一次 周期性的
         */
        TimerId addInterval(Duration interval, Callback callback, TimePoint now = Clock::now());

        /**
         * 从任务队列中移除任务 惰性移除 仅仅是先做逻辑删除的处理 在队列里面找到它打上标识 没有实际从队列里面删除掉
         * @parm id 任务id
         */
        bool cancel(TimerId id);

        /**
         * @return 最近的一个定时任务还有多久
         *         -1表示任务队列是空的
         */
        int timeoutMs(TimePoint now = Clock::now());

        void fire(TimePoint now = Clock::now());

        bool empty() const {
            return heap_.empty();
        }

    private:
        struct Timer {
            // 任务编号
            TimerId id = 0;
            TimePoint deadline;
            Duration interval{};
            Callback callback;
            // 任务是要取消的 惰性处理 在任务到期的时候看一下它是不是要取消
            bool cancelled = false;
        };

        /**
         * 算法调整新入队元素的位置 让它符合堆的特性
         * 默认是大根堆 它定义的comparison函数的语义是第1个元素<第2个元素就返回true
         * 我现在需要它是小根堆特性所以comp要反过来 第1个元素>第2个元素就返回true
         */
        static bool comp(const Timer& parent, const Timer& child) {
            return parent.deadline > child.deadline;
        }

        /**
         * 定时任务入队
         * @param deadline 最近一次是什么时候需要调度
         * @param interval 区分一次性任务/周期任务
         *                 0-表示一次性任务
         *                 n-是周期性任务 每隔n执行一次
         */
        TimerId add(TimePoint deadline, Duration interval, Callback callback);

        /**
         * 在合适的时机看一下到期的任务是不是要取消
         */
        void dropCancelledTop();
        // 小根堆实现对任务的管理 排序的key是到期时间 这个里面包含了活跃任务和被要取消的任务 当定时任务到期了的时候 可能这个任务已经被系统逻辑取消了 这个时候看一下它的状态
        std::vector<Timer> heap_;
        // 正在被执行回调的任务 为什么要用这个玩意 防止任务被回调执行期间自己删除自己
        Timer* current_ = nullptr;
        // 任务id生成器
        TimerId nextId_ = 1;
    };
} // namespace xmr::net
