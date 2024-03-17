#include "ih_thread_pool.h"

ThreadPool::ThreadPool() : thread_num_(1), terminate_(false) {}

ThreadPool::~ThreadPool() {

    Stop();
}

bool ThreadPool::Init(size_t num) {
    

    std::unique_lock<std::mutex> lock(mutex_);

    if(!threads_.empty())
        return false;

    thread_num_ = num;
    return true;
}

void ThreadPool::Stop() {

    {
        std::unique_lock<std::mutex> lock(mutex_);
        terminate_ = true;
        condition_.notify_all();
    }
    

    for(int i = 0; i < threads_.size(); i++) {

        if(threads_[i]->joinable())
            threads_[i]->join();

        delete threads_[i];
        threads_[i] = NULL;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    threads_.clear();
}

size_t ThreadPool::GetThreadNum() {

    std::unique_lock<std::mutex> lock(mutex_);
    return threads_.size();
}

size_t ThreadPool::GetJobNum() {

    std::unique_lock<std::mutex> lock(mutex_);
    return tasks_.size();
}

bool ThreadPool::Start() {

    std::unique_lock<std::mutex> lock(mutex_);
    if(!threads_.empty())
        return false;

    for(size_t i = 0; i < thread_num_; i++) {
        threads_.push_back(new std::thread(&ThreadPool::Run, this));
    }

    return true;
}

void ThreadPool::Run() {

    while(!IsTerminate) {
        TaskFuncPtr task;
        bool ok = Get(task);
        if(ok) {
            ++atomic_;
            try{
                if(task->_expireTime != 0 && task->_expireTime < TNOWMS) {
                    //超时任务处理
                }
                else {
                    task->_func();
                }
            }
            catch(...) {
            }

            --atomic_;

            std::unique_lock<std::mutex> lock(mutex_);
            if(atomic_ == 0 && tasks_.empty())
                condition_.notify_all();
        }
    }
}

bool ThreadPool::WaitForAllDone(int millsecond) {

    std::unique_lock<std::mutex> lock(mutex_);
    if(tasks_.empty())
        return true;

    if(millsecond < 0) {
        condition_.wait(lock, [this]{return tasks_.empty();});
        return true;
    }
    else {
        return condition_.wait_for(lock, std::chrono::milliseconds(millsecond),
            [this]{return tasks_.empty();});
    }
}

bool ThreadPool::Get(TaskFuncPtr& task) {

    std::unique_lock<std::mutex> lock(mutex_);

    if(tasks_.empty()) {
        condition_.wait(lock, [this]{
            return terminate_ || !tasks_.empty();
        });
    }

    if(terminate_)
        return false;

    if(!tasks_.empty()) {
        task = std::move(tasks_.front());
        tasks_.pop();
        return true;
    }

    return false;
}

int gettimeofday(struct timeval& tv) {
    return ::gettimeofday(&tv, 0);
}

void getNow(timeval* tv) {

    gettimeofday(*tv);
}

int64_t getNowMs() {

    struct timeval tv;
    getNow(&tv);

    return tv.tv_sec * (int64_t)1000 + tv.tv_usec / 1000;
}