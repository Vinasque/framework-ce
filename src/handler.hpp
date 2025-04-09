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
#include "dataframe.hpp" 

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
private:
    double totalRevenue = 0.0;
    mutable std::mutex revenueMutex;

public:
    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        // Agrupar pela coluna "reservation_time" (data) e somar os valores da coluna "price"
        DataFrame<std::string> groupedDf = df.groupby("reservation_time", "price");

        // Calcular a receita total, somando os valores da coluna "price" para o status "confirmed"
        for (int i = 0; i < df.numRows(); ++i) {
            std::string status = df.getValue("status", i);
            if (status == "confirmed") {
                totalRevenue += std::stod(df.getValue("price", i));
            }
        }

        return groupedDf;  // Retorna o DataFrame agrupado
    }

    double getTotalRevenue() const {
        std::lock_guard<std::mutex> lock(revenueMutex);
        return totalRevenue;
    }

    void resetRevenue() {
        std::lock_guard<std::mutex> lock(revenueMutex);
        totalRevenue = 0.0;
    }
};

// Handler de cálculo de receita com groupby por data
class CardRevenueHandler : public BaseHandler {
private:
    mutable std::mutex revenueMutex;

public:
    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        // Agrupar pela coluna "reservation_time" (data) e somar os valores da coluna "price"
        DataFrame<std::string> groupedDf = df.groupby("payment_method", "price");
        return groupedDf;  // Retorna o DataFrame agrupado
    }
};
#endif // HANDLER_HPP