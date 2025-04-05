// thread_world.hpp
#ifndef THREAD_WORLD_HPP
#define THREAD_WORLD_HPP

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "handler.hpp"

class ThreadWorld {
public:
    ThreadWorld(size_t initialThreads) : 
        buffers(3), // Input buffer + 2 intermediate buffers
        workers(initialThreads),
        dynamicAllocation(false),
        processingComplete(false) {
        
        for (size_t i = 0; i < workers.size(); ++i) {
            workers[i] = std::thread(&ThreadWorld::workerFunction, this, i);
        }
    }

    ~ThreadWorld() {
        stop();
    }

    void setDynamicAllocation(bool enable) {
        dynamicAllocation = enable;
    }

    void start() {
        processingComplete = false;
    }

    void stop() {
        for (auto& handler : handlers) {
            handler->stop();
        }
        
        for (auto& worker : workers) {
            if (worker.joinable()) worker.join();
        }
    }

    bool isProcessingComplete() const {
        return processingComplete;
    }

    void addDataToBuffer(size_t idx, const Reservation& data) {
        if (idx < buffers.size()) {
            buffers[idx].add(data);
            if (dynamicAllocation && buffers[idx].isFull()) {
                addWorkerThread();
            }
        }
    }

private:
    template<typename T>
    class Buffer {
    public:
        void add(const T& item) {
            std::lock_guard<std::mutex> lock(mtx);
            queue.push(item);
            cv.notify_one();
        }

        T get() {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [this]{ return !queue.empty(); });
            T item = queue.front();
            queue.pop();
            return item;
        }

        bool isEmpty() const {
            std::lock_guard<std::mutex> lock(mtx);
            return queue.empty();
        }

        bool isFull() const {
            std::lock_guard<std::mutex> lock(mtx);
            return queue.size() >= capacity;
        }

    private:
        std::queue<T> queue;
        mutable std::mutex mtx;
        std::condition_variable cv;
        const size_t capacity = 100;
    };

    std::vector<Buffer<Reservation>> buffers;
    std::vector<std::thread> workers;
    std::vector<std::unique_ptr<BaseHandler>> handlers;
    bool dynamicAllocation;
    bool processingComplete;
    mutable std::mutex mtx;

    void workerFunction(size_t id) {
        while (true) {
            // Get data from input buffer
            Reservation res = buffers[0].get();
            
            // Process through all handlers
            for (auto& handler : handlers) {
                handler->process(res);
            }
            
            // Check if processing is complete
            std::lock_guard<std::mutex> lock(mtx);
            if (buffers[0].isEmpty() && !processingComplete) {
                processingComplete = true;
                break;
            }
        }
    }

    void addWorkerThread() {
        std::lock_guard<std::mutex> lock(mtx);
        workers.emplace_back(&ThreadWorld::workerFunction, this, workers.size());
    }
};

#endif // THREAD_WORLD_HPP