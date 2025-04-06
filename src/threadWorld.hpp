#ifndef THREAD_WORLD_HPP
#define THREAD_WORLD_HPP

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <memory>
#include <chrono>
#include <iostream>
#include "handler.hpp"

using namespace std::chrono_literals;

class ThreadWorld {
public:
    enum class AllocationMode { Static, Dynamic };

    ThreadWorld(size_t initialThreadsPerStage, AllocationMode mode) :
        mode_(mode),
        stopped_(false),
        buffers_(3), // Input + 2 intermediate buffers
        initialThreads_(initialThreadsPerStage) {
        
        handlers_.resize(3); // Validation, Date, Revenue
        workers_.resize(3);  // One vector per stage
        
        for (size_t stage = 0; stage < handlers_.size(); ++stage) {
            addWorkerThreads(stage, initialThreads_);
        }
    }

    ~ThreadWorld() { 
        stop(); 
    }

    void stop() {
        stopped_.store(true);
        
        // Stop all buffers first
        for (auto& buf : buffers_) {
            buf.stop();
        }
        
        // Then join all threads
        for (auto& stageThreads : workers_) {
            for (auto& thread : stageThreads) {
                if (thread.joinable()) {
                    thread.join();
                }
            }
            stageThreads.clear();
        }
    }

    void addHandler(size_t stage, std::unique_ptr<BaseHandler> handler) {
        if (stage < handlers_.size()) {
            std::lock_guard<std::mutex> lock(mtx_);
            handlers_[stage] = std::move(handler);
        }
    }

    void addData(const Reservation& data) {
        if (stopped_) return;
        
        buffers_[0].add(data);
        if (mode_ == AllocationMode::Dynamic) {
            checkDynamicAllocation(0);
        }
    }

    bool isProcessingComplete() const {
        std::unique_lock<std::mutex> lock(mtx_);
        
        // Check if all buffers are empty
        bool buffersEmpty = true;
        for (const auto& buf : buffers_) {
            if (!buf.isEmpty()) {
                buffersEmpty = false;
                break;
            }
        }
        
        // If no workers and buffers empty, we're done
        bool done = (active_workers_ == 0) && buffersEmpty;
        
        if (!done) {
            std::cout << "Completion check - Workers: " << active_workers_.load() 
                     << " | Buffers: ";
            for (size_t i = 0; i < buffers_.size(); ++i) {
                std::cout << "B" << i << "=" << buffers_[i].size() << " ";
            }
            std::cout << "\n";
        }
        
        return done;
    }

    std::vector<size_t> getThreadCounts() const {
        std::vector<size_t> counts;
        std::lock_guard<std::mutex> lock(mtx_);
        for (const auto& stageThreads : workers_) {
            counts.push_back(stageThreads.size());
        }
        return counts;
    }

    void printBufferStatus() const {
        std::cout << "Buffer Status:\n";
        for (size_t i = 0; i < buffers_.size(); ++i) {
            std::cout << "  Buffer " << i << ": " << buffers_[i].size() << " items\n";
        }
    }

private:
    class ThreadSafeBuffer {
    public:
        void add(const Reservation& item) {
            std::unique_lock<std::mutex> lock(mtx_);
            queue_.push(item);
            lock.unlock();
            cv_.notify_one();
        }

        bool get(Reservation& item) {
            std::unique_lock<std::mutex> lock(mtx_);
            if (queue_.empty() && !stopped_) {
                cv_.wait_for(lock, 100ms, [this] { 
                    return !queue_.empty() || stopped_; 
                });
            }
            
            if (stopped_ || queue_.empty()) return false;
            
            item = queue_.front();
            queue_.pop();
            return true;
        }

        bool isEmpty() const {
            std::lock_guard<std::mutex> lock(mtx_);
            return queue_.empty();
        }

        size_t size() const {
            std::lock_guard<std::mutex> lock(mtx_);
            return queue_.size();
        }

        void notifyAll() { 
            cv_.notify_all(); 
        }

        void stop() {
            stopped_ = true;
            notifyAll();
        }

    private:
        std::queue<Reservation> queue_;
        mutable std::mutex mtx_;
        std::condition_variable cv_;
        std::atomic<bool> stopped_{false};
    };

    void workerFunction(size_t stage) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            active_workers_++;
            std::cout << "Worker started for stage " << stage 
                     << " (Total active: " << active_workers_.load() << ")\n";
        }

        auto lastActivity = std::chrono::high_resolution_clock::now();
        const auto timeout = 500ms;

        while (!stopped_) {
            Reservation res;
            bool gotItem = buffers_[stage].get(res);
            
            if (!gotItem) {
                if (stopped_) break;
                
                // Check if we should exit due to inactivity
                std::unique_lock<std::mutex> lock(mtx_);
                if (buffers_[stage].isEmpty() && 
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::high_resolution_clock::now() - lastActivity) > timeout) {
                    break;
                }
                continue;
            }
            
            lastActivity = std::chrono::high_resolution_clock::now();

            if (handlers_[stage]) {
                handlers_[stage]->process(res);
            }

            if (stage + 1 < buffers_.size() && !stopped_) {
                buffers_[stage + 1].add(res);
                if (mode_ == AllocationMode::Dynamic) {
                    checkDynamicAllocation(stage + 1);
                }
            }
        }

        {
            std::lock_guard<std::mutex> lock(mtx_);
            active_workers_--;
            std::cout << "Worker exiting stage " << stage 
                     << " (Remaining: " << active_workers_.load() << ")\n";
        }
    }

    bool checkAllBuffersEmpty() const {
        for (const auto& buf : buffers_) {
            if (!buf.isEmpty()) return false;
        }
        return true;
    }

    void addWorkerThreads(size_t stage, size_t count) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (workers_.size() <= stage) workers_.resize(stage + 1);
        
        for (size_t i = 0; i < count; ++i) {
            workers_[stage].emplace_back(&ThreadWorld::workerFunction, this, stage);
        }
    }

    void checkDynamicAllocation(size_t stage) {
        std::lock_guard<std::mutex> lock(mtx_);
        const size_t max_threads = 8;
        const size_t queue_threshold = 50;

        // Check if we need more threads for this stage
        if (buffers_[stage].size() > queue_threshold && 
            workers_[stage].size() < max_threads) {
            workers_[stage].emplace_back(&ThreadWorld::workerFunction, this, stage);
            std::cout << "Added worker for stage " << stage 
                     << " (Total: " << workers_[stage].size() << ")\n";
        }
    }

    AllocationMode mode_;
    std::atomic<bool> stopped_;
    std::vector<ThreadSafeBuffer> buffers_;
    std::vector<std::unique_ptr<BaseHandler>> handlers_;
    std::vector<std::vector<std::thread>> workers_;
    mutable std::mutex mtx_;
    std::atomic<int> active_workers_{0};
    const size_t initialThreads_;
};
#endif // THREAD_WORLD_HPP