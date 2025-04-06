#pragma once

#include <mutex>
#include <queue>
#include <condition_variable>
#include <stdexcept>
#include <thread>
#include <utility> 

template <typename U, typename V>
class Queue {
public:
    Queue(int capacity) : capacity_(capacity) {}

    ~Queue() {
        // destruidor com mutex (para garantir segurança ao limpar a fila)
        std::lock_guard<std::mutex> lock(mutex_);
        while (!queue_.empty()) {
            queue_.pop();
        }
    }

    // adicionar na fila
    void enQueue(std::pair<U, V> item) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return queue_.size() < capacity_; });  // espera até ter espaço na fila

        queue_.push(item);
        cv_.notify_one();  // notifica uma thread que pode consumir
    }

    // remover e retornar o item seguinte da fila
    std::pair<U, V> deQueue() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !queue_.empty(); });  // espera até ter item para remover

        std::pair<U, V> item = queue_.front();
        queue_.pop();

        cv_.notify_one();  // notifica uma thread que pode adicionar
        return item;
    }

private:
    std::mutex mutex_;                   // mutex para controlar o acesso à fila
    std::condition_variable cv_;         // variável de condição para bloquear e acordar threads
    int capacity_;                       // capacidade máxima da fila
    std::queue<std::pair<U, V>> queue_;  // fila interna que armazena os itens
};
