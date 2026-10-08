#include"net/timer.h"

namespace xmr::net {
    TimerQueue::TimerId TimerQueue::addAfter(Duration delay, Callback callback, TimePoint now) {
        return add(now + delay, Duration::zero(), std::move(callback));
    }

    TimerQueue::TimerId TimerQueue::addInterval(Duration interval, Callback callback, TimePoint now) {
        if (interval <= Duration::zero()) {
            throw std::invalid_argument("timer interval must be positive");
        }
        return add(now + interval, interval, std::move(callback));
    }

    bool TimerQueue::cancel(TimerId id) {
        if (live_.count(id) == 0 || cancelled_.count(id) != 0) {
            return false;
        }
        // 要出队的任务 缓存起来 惰性处理
        cancelled_.insert(id);
        return true;
    }

    int TimerQueue::timeoutMs(TimePoint now) {
        dropCancelledTop();
        if (heap_.empty()) {
            return -1;
        }
        const Duration delta = heap_.front().deadline - now;
        if (delta <= Duration::zero()) {
            return 0;
        }
        return static_cast<int>(std::chrono::ceil<std::chrono::milliseconds>(delta).count());
    }

    void TimerQueue::fire(TimePoint now) {
        while (true) {
            // 堆顶的任务可能是要取消的
            dropCancelledTop();
            if (heap_.empty() || heap_.front().deadline > now) {
                break;
            }
            // 拿到堆顶的任务
            const Timer timer = heap_.front();
            std::pop_heap(heap_.begin(), heap_.end(), comp);
            heap_.pop_back();
            live_.erase(timer.id);
            // 执行这个任务
            timer.callback();
            // 要确认它是不是周期性的定时任务 如果是周期任务就要重新入队了 再检查下它有没有在执行期间被逻辑删除了
            const bool selfCancelled = cancelled_.erase(timer.id) != 0;
            if (!selfCancelled && timer.interval > Duration::zero()) {
                Timer next = timer;
                next.deadline = now + timer.interval;
                heap_.push_back(next);
                std::push_heap(heap_.begin(), heap_.end(), comp);
                live_.insert(next.id);
            }
        }
    }

    TimerQueue::TimerId TimerQueue::add(TimePoint deadline, Duration interval, Callback callback) {
        const TimerId id = nextId_++;
        // push_heap算法要求先把入队元素放到末尾
        heap_.push_back(Timer{id, deadline, interval, std::move(callback)});
        // 保证小根堆的特性 到期时间
        std::push_heap(heap_.begin(), heap_.end(), comp);
        // 活跃任务
        live_.insert(id);
        return id;
    }

    void TimerQueue::dropCancelledTop() {
        while (!heap_.empty() && cancelled_.count(heap_.front().id) != 0) {
            live_.erase(heap_.front().id);
            cancelled_.erase(heap_.front().id);
            std::pop_heap(heap_.begin(), heap_.end(), comp);
            heap_.pop_back();
        }
    }
} // namespace xmr::net
