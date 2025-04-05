#ifndef DATE_TRANSFORM_HANDLER_HPP
#define DATE_TRANSFORM_HANDLER_HPP

#include "handler.hpp"
#include <string>

class DateTransformHandler : public Handler<int> {
public:
    DateTransformHandler(size_t numOutputQueues, std::vector<"""df""">& res) 
        : Handler<int>(numOutputQueues, res) {}
    
    void run() override {
        while (isRunning) {
            std::unique_lock<std::mutex> lock(inputMutex);
            cv.wait(lock, [this]() { return !inputQueue.empty() || !isRunning; });

            if (!isRunning) break;

            // Pega o índice da reserva da fila de entrada
            int reservation_index = inputQueue.front();
            inputQueue.pop();
            lock.unlock();

            // Processamento: transforma a data para mostrar apenas o dia
            processDate(reservation_index);

            // Envia para a fila de saída (pode ser usado para próximo processamento)
            if (!outputQueues.empty()) {
                std::lock_guard<std::mutex> outLock(outputMutexes[0]);
                outputQueues[0].push(reservation_index);
            }
        }
    }

private:
    // Função auxiliar para processar a data
    void processDate(int reservation_index) {
        std::string& original_date = reservations[reservation_index].reservation_time;
        size_t t_pos = original_date.find('T');
        if (t_pos != std::string::npos) {
            original_date = original_date.substr(0, t_pos);
        }
    }
};

#endif // DATE_TRANSFORM_HANDLER_HPP