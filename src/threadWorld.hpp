#ifndef THREAD_WORLD_HPP
#define THREAD_WORLD_HPP

#include <vector>
#include <thread>
#include <functional>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <iostream>

class ThreadWorld {
public:
    // Constructor to initialize with a number of threads
    ThreadWorld(size_t initialThreads) : running(false) {
        // Initialize buffers based on your processing pipeline
        buffers.resize(3); // Example for three buffers
        
        // Start the worker threads
        for (size_t i = 0; i < initialThreads; ++i) {
            workers.push_back(std::thread(&ThreadWorld::workerThreadFunction, this, i));
        }
    }

    // Destructor to join all threads
    ~ThreadWorld() {
        stop();
    }

    // Method to add a function to be executed by the threads
    void addTask(std::function<void()> task) {
        std::lock_guard<std::mutex> lock(taskMutex);
        std::thread(task).detach();
    }

    // Method to start the thread world
    void start() {
        running = true;
        // Optionally, create threads that process data
        // Each worker thread will process data from buffers
    }

    // Method to stop the thread world
    void stop() {
        running = false;
        // Wait for all threads to finish
        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    // Method to add data to a buffer
    template<typename T>
    void addDataToBuffer(size_t bufferIndex, T&& data) {
        buffers[bufferIndex].add(std::forward<T>(data));
    }

    // Method to consume data from a buffer
    template<typename T>
    T consumeDataFromBuffer(size_t bufferIndex) {
        return buffers[bufferIndex].consume();
    }

    // Helper method to manage thread allocations dynamically
    void manageThreads() {
        // Dynamic thread management logic based on buffer state
        for (size_t i = 0; i < buffers.size(); ++i) {
            // Example logic for adding/removing threads dynamically
            if (buffers[i].isFull()) {
                // If buffer is full, allocate a new thread for consuming data
            }
            else if (buffers[i].isEmpty()) {
                // If buffer is empty, stop a thread if necessary
            }
        }
    }

private:
    // Buffer class to store data
    template<typename T>
    class Buffer {
    public:
        // Method to add data to the buffer
        void add(T&& data) {
            std::lock_guard<std::mutex> lock(mtx);
            dataQueue.push(std::forward<T>(data));
            cv.notify_one();
        }

        // Method to consume data from the buffer
        T consume() {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [this] { return !dataQueue.empty(); });
            T data = dataQueue.front();
            dataQueue.pop();
            return data;
        }

        // Check if the buffer is empty
        bool isEmpty() const {
            std::lock_guard<std::mutex> lock(mtx);
            return dataQueue.empty();
        }

        // Check if the buffer is full (arbitrary limit, you can adjust this)
        bool isFull() const {
            std::lock_guard<std::mutex> lock(mtx);
            return dataQueue.size() >= maxSize;
        }

    private:
        std::queue<T> dataQueue;
        mutable std::mutex mtx;
        std::condition_variable cv;
        const size_t maxSize = 100; // Arbitrary max size, adjust as needed
    };

    // Buffer storage for input and output data
    std::vector<Buffer<std::string>> buffers;

    // Vector of worker threads
    std::vector<std::thread> workers;

    // Flag to control the running state of the threads
    bool running;
    
    // Mutex and condition variable to manage thread synchronization
    std::mutex taskMutex;
    std::condition_variable taskCV;

    // Method to be executed by each worker thread
    void workerThreadFunction(size_t threadIndex) {
        while (running) {
            // Example worker logic
            for (size_t i = 0; i < buffers.size(); ++i) {
                if (!buffers[i].isEmpty()) {
                    auto data = consumeDataFromBuffer<std::string>(i);
                    // Process the data (task) here
                    std::cout << "Worker " << threadIndex << " processed data: " << data << std::endl;
                }
            }
        }
    }
};

#endif // THREAD_WORLD_HPP