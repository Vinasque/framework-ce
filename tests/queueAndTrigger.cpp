#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

#include "../src/queue.hpp"
#include "../src/trigger.hpp"
#include "../src/handler.hpp"

// Simula uma reserva com dados dummy
Reservation generateDummyReservation() {
    return {
        "AAA-123",             // flight_id
        "12B",                 // seat
        "12345",               // user_id
        "João Gabriel",        // customer_name
        "confirmed",           // status
        "credit_card",         // payment_method
        "2024-04-06T10:30:00", // reservation_time
        499.90                 // amount
    };
}

// Lê da fila de triggers e envia para o primeiro handler
void dispatchFromQueueToHandler(Queue<std::string, std::string>& q, BaseHandler* handler, int max_dispatches) {
    for (int i = 0; i < max_dispatches; ++i) {
        auto item = q.deQueue();
        std::cout << "[Dispatcher] Triggered: " << item.first << ", " << item.second << std::endl;

        Reservation r = generateDummyReservation();
        handler->addReservation(r);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

// Função principal de teste
void testQAT() {
    std::cout << "=== Início do Teste com Trigger + Queue + Handlers ===" << std::endl;

    // Criar fila de entrada para trigger
    Queue<std::string, std::string> triggerQueue(10);

    // Trigger periódico a cada 1s
    TimeTrigger t(triggerQueue, 1000, "orders", "orders.json");

    // Criar handlers
    ValidationHandler validationHandler;
    DateHandler dateHandler;
    RevenueHandler revenueHandler;

    // Inicia cada handler em sua thread
    std::thread t1([&]() { validationHandler.run(); });
    std::thread t2([&]() { dateHandler.run(); });
    std::thread t3([&]() { revenueHandler.run(); });

    // Inicia trigger
    t.start();

    // Dispatcher pega da trigger e joga no primeiro handler
    std::thread dispatcher(dispatchFromQueueToHandler, std::ref(triggerQueue), &validationHandler, 5);

    // Encadeia: Validation ➝ Date ➝ Revenue
    std::thread handoff1([&]() {
        while (validationHandler.isRunning()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            Reservation r;
            if (!validationHandler.tryPop(r)) continue;
            dateHandler.addReservation(r);
        }
    });

    std::thread handoff2([&]() {
        while (dateHandler.isRunning()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            Reservation r;
            if (!dateHandler.tryPop(r)) continue;
            revenueHandler.addReservation(r);
        }
    });

    // Aguarda final do dispatch e mais 3 segundos para processar
    dispatcher.join();
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Para os handlers
    validationHandler.stop();
    dateHandler.stop();
    revenueHandler.stop();

    t1.join();
    t2.join();
    t3.join();
    handoff1.join();
    handoff2.join();

    // Imprime resultado
    double total = revenueHandler.getTotalRevenue();
    std::cout << "\n[FINAL] Receita total processada: R$ " << total << std::endl;
    std::cout << "=== Fim do teste ===\n";
}

int main() {
    testQAT();
    return 0;
}
