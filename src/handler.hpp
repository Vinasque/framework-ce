// handler.hpp
#ifndef HANDLER_HPP
#define HANDLER_HPP

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <string>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <cmath>
#include "dataframe.hpp"  // Agora estamos usando DataFrame

// BaseHandler agora lida com DataFrame
class BaseHandler {
protected:
    std::queue<DataFrame<std::string>> inputQueue;  // Fila de DataFrame
    std::mutex inputMutex;
    std::condition_variable cv;
    bool running = true;

public:
    // Modificado para aceitar e retornar um DataFrame
    virtual DataFrame<std::string> process(DataFrame<std::string>& df) = 0;  

    void stop() {
        running = false;
        cv.notify_all();
    }

    virtual ~BaseHandler() {}

    // Função para rodar a execução dos handlers
    void run() {
        while (running) {
            std::unique_lock<std::mutex> lock(inputMutex);
            cv.wait(lock, [this] { return !inputQueue.empty() || !running; });

            if (!running) break;

            DataFrame<std::string> df = inputQueue.front();
            inputQueue.pop();
            lock.unlock();

            process(df);  // Processo do DataFrame
        }
    }

    // verificar status da thread
    bool isRunning() const {
        return running;
    }

    // inserir na fila
    void addDataFrame(DataFrame<std::string>& df) {
        std::lock_guard<std::mutex> lock(inputMutex);
        inputQueue.push(df);
        cv.notify_one();
    }
};

// Handler de validação (exemplo de process)
class ValidationHandler : public BaseHandler {
    public:
        DataFrame<std::string> process(DataFrame<std::string>& df) override {
            // Aqui, você pode processar o df como necessário. Por exemplo:
            for (int i = 0; i < df.numRows(); ++i) {
                if (df.getValue("flight_id", i).empty()) {
                    df.deleteLine(i);  // Deleta a linha se flight_id for vazio
                }
            }
            return df;  // Retorna o DataFrame modificado
        }
    };
    
class DateHandler : public BaseHandler {
public:
    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        for (int i = 0; i < df.numRows(); ++i) {
            std::string datetime = df.getValue("reservation_time", i);
            if (datetime.length() >= 10) {
                std::string formattedDate = datetime.substr(0, 10);  // Formata a data
                df.updateValue("reservation_time", i, formattedDate);  // Atualiza a célula no DataFrame
            }
        }
        return df;  // Retorna o DataFrame modificado
    }
};

// Handler de cálculo de receita com groupby por data
class RevenueHandler : public BaseHandler {
public:
    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        // First filter only confirmed reservations
        DataFrame<std::string> confirmed = df.filter("status", "==", std::string("confirmed"));
        // Then group by date and sum prices
        return confirmed.groupby("reservation_time", "price");
    }
};

class CardRevenueHandler : public BaseHandler {
public:
    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        // First filter only confirmed reservations
        DataFrame<std::string> confirmed = df.filter("status", "==", std::string("confirmed"));
        // Then group by payment method and sum prices
        return confirmed.groupby("payment_method", "price");
    }
};
#endif // HANDLER_HPP