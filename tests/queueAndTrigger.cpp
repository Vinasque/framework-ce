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
    Reservation res = {
        "AAA-123",             // flight_id
        "12B",                 // seat
        "12345",               // user_id
        "João Gabriel",        // customer_name
        "confirmed",           // status
        "credit_card",         // payment_method
        "2024-04-06T10:30:00", // reservation_time
        499.90                 // price
    };

    std::cout << "[generateDummyReservation] Gerado: " << res.customer_name 
              << ", R$" << res.price << ", status: " << res.status << std::endl;
    return res;
}

// Lê da fila de triggers e envia para o primeiro handler
void dispatchFromQueueToHandler(Queue<std::string, std::string>& q, BaseHandler* handler, int max_dispatches) {
    for (int i = 0; i < max_dispatches; ++i) {
        auto item = q.deQueue();
        std::cout << "[Dispatcher] Triggered: " << item.first << ", " << item.second << std::endl;

        Reservation r = generateDummyReservation();
        handler->addReservation(r);
        std::cout << "[Dispatcher] Adicionada reserva para validação.\n";

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

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
    std::thread t1([&]() {
        std::cout << "[Thread] validationHandler iniciado\n";
        validationHandler.run();
        std::cout << "[Thread] validationHandler finalizado\n";
    });

    std::thread t2([&]() {
        std::cout << "[Thread] dateHandler iniciado\n";
        dateHandler.run();
        std::cout << "[Thread] dateHandler finalizado\n";
    });

    std::thread t3([&]() {
        std::cout << "[Thread] revenueHandler iniciado\n";
        revenueHandler.run();
        std::cout << "[Thread] revenueHandler finalizado\n";
    });

    t.start();
    std::thread dispatcher(dispatchFromQueueToHandler, std::ref(triggerQueue), &validationHandler, 5);

    std::thread handoff1([&]() {
        while (validationHandler.isRunning()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            Reservation r;
            if (!validationHandler.tryPop(r)) continue;
            std::cout << "[handoff1] Reserva passada de Validation ➝ DateHandler: "
                      << r.customer_name << " | " << r.status << std::endl;
            dateHandler.addReservation(r);
        }
        std::cout << "[handoff1] Encerrado\n";
    });

    std::thread handoff2([&]() {
        while (dateHandler.isRunning()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            Reservation r;
            if (!dateHandler.tryPop(r)) continue;
            std::cout << "[handoff2] Reserva passada de Date ➝ RevenueHandler: "
                      << r.customer_name << " | " << r.status 
                      << " | Data: " << r.reservation_time 
                      << " | Valor: " << r.price << std::endl;
            revenueHandler.addReservation(r);
        }
        std::cout << "[handoff2] Encerrado\n";
    });

    dispatcher.join();
    std::cout << "[Main] Dispatcher finalizado\n";

    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Parar handlers
    validationHandler.stop();
    dateHandler.stop();
    revenueHandler.stop();

    t1.join();
    t2.join();
    t3.join();
    handoff1.join();
    handoff2.join();

    double total = revenueHandler.getTotalRevenue();
    std::cout << "\n[FINAL] Receita total processada: R$ " << total << std::endl;
    std::cout << "=== Fim do teste ===\n";
}

int main() {
    testQAT();
    return 0;
}
