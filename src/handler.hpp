#ifndef HANDLER_HPP
#define HANDLER_HPP

#include <iostream>
#include <queue>
#include <thread>
#include <mutex>
#include <vector>
#include <condition_variable>
#include "dataframe.hpp" 

template <typename T>
class Handler {
protected:
    std::queue<T> inputQueue;
    std::vector<std::queue<T>> outputQueues;
    std::mutex inputMutex;
    std::vector<std::mutex> outputMutexes;
    std::condition_variable cv;
    bool isRunning = true;
    std::vector<Reservation>& reservations; 

public:
    virtual void run() = 0;

    void addInputData(const T& data) {
        std::lock_guard<std::mutex> lock(inputMutex);
        inputQueue.push(data);
        cv.notify_one();
    }

    // Função para parar a thread
    void stop() {
        isRunning = false;
        cv.notify_all(); // Notifica todas as threads em espera
    }

    Handler(size_t numOutputQueues, std::vector<"""df""">& res) 
        : reservations(res), outputQueues(numOutputQueues), outputMutexes(numOutputQueues) {}

    virtual ~Handler() {}

    bool getOutputData(size_t queueIndex, T& data) {
        if (queueIndex >= outputQueues.size()) return false;
        
        std::lock_guard<std::mutex> lock(outputMutexes[queueIndex]);
        if (outputQueues[queueIndex].empty()) return false;
        
        data = outputQueues[queueIndex].front();
        outputQueues[queueIndex].pop();
        return true;
    }
};

#endif // HANDLER_HPP