#include <iostream>
#include <queue>
#include <thread>
#include <mutex>
#include <vector>
#include <functional>
#include "dataframe.hpp"

template <typename T>
class Handler {
public:
    queue<"""estrutura do df"""> inputQueue;
    std::vector<std::queue<"""estrutura do df"""> outputQueues;
    
    // mutex para as filas de entrada e saída
    std::mutex inputMutex;
    std::vector<std::mutex> outputMutexes;

    bool isRunning = true;

    // metodo das sublasses
    virtual void run() = 0;

    // add dados na fila de entrada
    void addInputData(int data) {
        std::lock_guard<std::mutex> lock(inputMutex);
        inputQueue.push(data);
        cv.notify_one();
    }

    // função pra bloquear a thread quando for conveniente
    void stop() {
        isRunning = false;
        cv.notify_one();
    }

    // construtor que inicializa o número de filas de saída
    Handler(size_t numOutputQueues) {
        outputQueues.resize(numOutputQueues);
        outputMutexes.resize(numOutputQueues);
    }

    virtual ~Handler(){}
};