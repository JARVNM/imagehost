#ifndef _IH_THREAD_POOL_H_
#define _IH_THREAD_POOL_H_

#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <sys/time.h>

void getNow(timeval* tv);
int64_t getNowMs();

#define TNOW getNow()
#define TNOWMS getNowMs()

class ThreadPool {

protected:
    struct TaskFunc {
        TaskFunc(uint64_t expireTime) : _expireTime(expireTime) {}

        std::function<void()> _func;
        int64_t _expireTime = 0;
    };
    typedef std::shared_ptr<struct TaskFunc> TaskFuncPtr;

public:

    ThreadPool();
    virtual ~ThreadPool();
    bool Init(size_t num);
    size_t GetThreadNum();
    size_t GetJobNum();
    void Stop();
    bool Start();

    template <class F, class... Args>
    auto Exec(F &&f, Args &&... args) -> std::future<decltype(f(args...))> {
        return Exec(0, f, args...);
    }

    template <class F, class... Args>
    auto Exec(int64_t timeoutMs, F &&f, Args &&...args)
        -> std::future<decltype(f(args...))> {

        int64_t expireTime = (timeoutMs == 0 ? 0 : TNOWMS + timeoutMs);

        using RetType = decltype(f(args...));
        auto task = std::make_shared<std::packaged_task<RetType>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );

        TaskFuncPtr fPtr = std::make_shared<TaskFunc>(expireTime);
        fPtr->_func = [task](){(*task)();}
        tasks_.push(fPtr);
        condition_.notify_one();
        return task->get_future();
    }

    bool WaitForAllDone(int millsecond = -1);
protected:

    bool Get(TaskFuncPtr& task);

    bool IsTerminate() {return terminate_;}

    void Run();

protected:
    
    std::queue<struct TaskFunc> tasks_;

    std::vector<std::thread*> threads_;
    std::mutex mutex_;
    std::condition_variable condition_;
    size_t thread_num_;
    bool terminate_;
    std::atomic<int> atomic_{0}; 
};

#endif