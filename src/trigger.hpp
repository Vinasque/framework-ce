#pragma once

#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include "queue.hpp"
#include <mutex>
#include <condition_variable>

/**
 * @brief Classe base para todos os tipos de gatilhos (Triggers).
 * 
 * Ela empurra um par de strings (chave e valor) para a fila de saída,
 * simulando a chegada de um novo dado ou comando.
 */
class Trigger {
public:
    /**
     * @param outQueue - Fila para onde o trigger enviará os sinais
     * @param first - Identificador da origem ou tipo do dado (ex: "orders")
     * @param second - Caminho do dado ou parâmetro extra (ex: "orders.json")
     */
    Trigger(Queue<std::string, std::string>& outQueue, std::string first, std::string second)
        : outQueue(outQueue), first(first), second(second) {}

    virtual ~Trigger() {
        running = false;
        join();  // Aguarda a finalização da thread
    }

    // Inicia o trigger em uma nova thread
    void start() {
        trigger_thread = std::thread(&Trigger::run, this);
    }

    // Aguarda a thread encerrar
    void join() {
        if (trigger_thread.joinable()) {
            trigger_thread.join();
        }
    }

    // Função que deve ser implementada pelas subclasses (define o comportamento do trigger)
    virtual void run() = 0;

    // Empurra um par (tipo, dado) na fila
    void addToQueue() {
        outQueue.enQueue(std::make_pair(first, second));
    }

protected:
    Queue<std::string, std::string>& outQueue;
    bool running = true;
    std::string first;
    std::string second;

private:
    std::thread trigger_thread;
};


/**
 * @brief Trigger que envia um evento a cada N milissegundos.
 * 
 * Ideal para simular chegada contínua de dados, tipo streaming.
 */
class TimeTrigger : public Trigger {
public:
    TimeTrigger(Queue<std::string, std::string>& outQueue, int intervalMs,
                std::string first, std::string second)
        : Trigger(outQueue, first, second), interval(intervalMs) {}

    void run() override {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
            addToQueue();  // Envia o sinal para a fila
        }
    }

private:
    int interval = 1000;  // Intervalo de envio em ms
};


/**
 * @brief Trigger que dispara apenas uma vez —  para eventos sob demanda.
 */
class RequestTrigger : public Trigger {
public:
    RequestTrigger(Queue<std::string, std::string>& outQueue,
                   std::string first, std::string second)
        : Trigger(outQueue, first, second) {}

    void run() override {
        addToQueue();  // Dispara uma única vez
    }
};

