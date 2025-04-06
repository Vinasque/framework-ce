#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include "../src/queue.hpp"
#include "../src/trigger.hpp"
#include "../src/dataframe.hpp"
#include "../src/series.hpp"

// Simula um consumidor que escuta a fila e imprime os dados recebidos
void queueListener(Queue<std::string, std::string>& queue, int maxReads) {
    for (int i = 0; i < maxReads; ++i) {
        std::pair<std::string, std::string> item = queue.deQueue();
        std::cout << "[QueueListener] Received: " << item.first << ", " << item.second << std::endl;
    }
}

// Função de teste principal
void testQAT() {
    std::cout << "=== Testando Queue + Triggers ===" << std::endl;

    // Criação da fila compartilhada entre triggers e consumidor
    Queue<std::string, std::string> queue(10);

    // Criação de triggers
    TimeTrigger t1(queue, 1000, "orders", "orders.json");  // a cada 1s
    RequestTrigger r1(queue, "orders", "orders.json");     // uma vez só

    // Iniciar o time trigger e request trigger
    t1.start();
    r1.start();

    // Consumidor que simula leitura de 5 elementos da fila
    std::thread listenerThread(queueListener, std::ref(queue), 50);

    // Espera terminar a leitura
    listenerThread.join();

    std::cout << "=== Fim do teste ===\n" << std::endl;

    // Encerrar triggers (TimeTrigger ainda rodando)
    t1.~TimeTrigger();  // chama destrutor para parar thread
}

int main() {
    testQAT();
    return 0;
}
