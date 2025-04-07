#include <iostream>
#include <vector>
#include <chrono>
#include "dataframe.hpp"
#include "extractor.hpp"
#include "handler.hpp"
#include "database.h"
#include "loader.hpp"
#include "ThreadPool.hpp"

using Clock = std::chrono::high_resolution_clock;

void testSequentialPipeline() {
    auto start = Clock::now();

    DataBase db("earning2.db");
    db.createTable("faturamento", "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo", "(payment_method TEXT PRIMARY KEY, price REAL)");

    Extractor extractor;
    DataFrame<std::string> df = extractor.extractFromJson("../generator/orders.json");

    ValidationHandler validationHandler;
    DateHandler dateHandler;
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;

    DataFrame<std::string> df2 = validationHandler.process(df);
    DataFrame<std::string> df3 = dateHandler.process(df2);
    DataFrame<std::string> df4 = revenueHandler.process(df3);
    DataFrame<std::string> df5 = cardHandler.process(df3);

    Loader loader(db);
    loader.loadData("faturamento", df4, {"reservation_time", "price"});
    loader.loadData("faturamentoMetodo", df5, {"payment_method", "price"});

    auto end = Clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "\n⏱️ Tempo (sequencial): " << duration.count() << " ms\n";
}

void testParallelPipeline(int numThreads) {
    auto start = Clock::now();

    ThreadPool pool(numThreads);

    Extractor extractor;
    auto futureDf = pool.addTask(&Extractor::extractFromJson, &extractor, "../generator/orders.json");
    DataFrame<std::string> df = futureDf.get();

    ValidationHandler validationHandler;
    DateHandler dateHandler;
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;

    auto f1 = pool.addTask(&ValidationHandler::process, &validationHandler, df);
    auto f2 = pool.addTask(&DateHandler::process, &dateHandler, df);
    DataFrame<std::string> dfValid = f1.get();
    DataFrame<std::string> dfDated = f2.get();

    auto f3 = pool.addTask(&RevenueHandler::process, &revenueHandler, dfDated);
    auto f4 = pool.addTask(&CardRevenueHandler::process, &cardHandler, dfDated);
    DataFrame<std::string> dfRevenue = f3.get();
    DataFrame<std::string> dfCards = f4.get();

    DataBase db("earning3.db");
    db.createTable("faturamento", "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo", "(payment_method TEXT PRIMARY KEY, price REAL)");

    Loader loader(db);
    pool.addTask(&Loader::loadData, &loader, "faturamento", dfRevenue, std::vector<std::string>{"reservation_time", "price"});
    pool.addTask(&Loader::loadData, &loader, "faturamentoMetodo", dfCards, std::vector<std::string>{"payment_method", "price"});

    // Dá um tempo para que as tarefas de loading terminem (alternativa ao join manual)
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    auto end = Clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "\n⏱️ Tempo (paralelo com " << numThreads << " threads): " << duration.count() << " ms\n";
}

void testParallelPipeline2(int numThreads) {
    auto start = Clock::now();

    ThreadPool pool(numThreads);

    Extractor extractor;
    auto futureDf = pool.addTask(&Extractor::extractFromJson, &extractor, "../generator/orders.json");
    DataFrame<std::string> df = futureDf.get();

    ValidationHandler validationHandler;
    DateHandler dateHandler;
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;

    auto f1 = pool.addTask(&ValidationHandler::process, &validationHandler, df);
    auto f2 = pool.addTask(&DateHandler::process, &dateHandler, df);
    DataFrame<std::string> dfValid = f1.get();
    DataFrame<std::string> dfDated = f2.get();

    auto f3 = pool.addTask(&RevenueHandler::process, &revenueHandler, dfDated);
    auto f4 = pool.addTask(&CardRevenueHandler::process, &cardHandler, dfDated);
    DataFrame<std::string> dfRevenue = f3.get();
    DataFrame<std::string> dfCards = f4.get();

    DataBase db("earning4.db");
    db.createTable("faturamento", "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo", "(payment_method TEXT PRIMARY KEY, price REAL)");

    Loader loader(db);
    pool.addTask(&Loader::loadData, &loader, "faturamento", dfRevenue, std::vector<std::string>{"reservation_time", "price"});
    pool.addTask(&Loader::loadData, &loader, "faturamentoMetodo", dfCards, std::vector<std::string>{"payment_method", "price"});

    // Dá um tempo para que as tarefas de loading terminem (alternativa ao join manual)
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    auto end = Clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "\n⏱️ Tempo (paralelo com " << numThreads << " threads): " << duration.count() << " ms\n";
}

int main() {
    std::cout << "\n=== Benchmark: Versão SEQUENCIAL ===" << std::endl;
    testSequentialPipeline();

    std::cout << "\n=== Benchmark: Versão PARALELA (6 threads) ===" << std::endl;
    testParallelPipeline(20);

    std::cout << "\n=== Benchmark: Versão PARALELA (10 threads) ===" << std::endl;
    testParallelPipeline2(10);

    return 0;
}
