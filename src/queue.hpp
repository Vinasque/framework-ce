#pragma once

#include <mutex>
#include <queue>
#include <condition_variable>
#include <stdexcept>

/**
 * Implementa uma fila thread-safe com capacidade limitada.
 * 
 * Esta classe gerencia uma fila com um controle de concorrência, permitindo
 * que múltiplos threads possam adicionar e remover itens da fila de forma segura.
 * 
 * O tamanho da fila é limitado e o comportamento de bloqueio é implementado
 * para garantir que os threads esperem quando a fila estiver cheia ou vazia.
 */
template <typename U, typename V>
class Queue {
public:
    /**
     * @brief Construtor da fila.
     * 
     * @param capacity A capacidade máxima da fila.
     */
    Queue(int capacity) : capacity_(capacity) {}

    ~Queue() {
        // Destruidor com proteção de mutex para garantir segurança ao limpar a fila
        std::lock_guard<std::mutex> lock(mutex_);
        while (!queue_.empty()) {
            queue_.pop();
        }
    }

    /**
     * @brief Adiciona um item à fila.
     *
     * Bloqueia até que haja espaço suficiente na fila.
     */
    void enqueue(std::pair<U, V> item) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return queue_.size() < capacity_; });  // Espera até ter espaço na fila

        queue_.push(item);
        cv_.notify_one();  // Notifica um thread que pode consumir
    }

    /**
     * @brief Remove e retorna o item da frente da fila.
     * 
     * Bloqueia até que haja um item na fila.
     */
    std::pair<U, V> dequeue() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !queue_.empty(); });  // Espera até que haja um item para remover

        std::pair<U, V> item = queue_.front();
        queue_.pop();

        cv_.notify_one();  // Notifica um thread que pode adicionar
        return item;
    }

private:
    std::mutex mutex_;                   // Mutex para controlar o acesso à fila
    std::condition_variable cv_;         // Variável de condição para bloquear e acordar threads
    int capacity_;                       // Capacidade máxima da fila
    std::queue<std::pair<U, V>> queue_;  // Fila interna que armazena os itens
};
