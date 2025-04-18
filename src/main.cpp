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

    DataBase db("../databases/earning2.db");
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
    loader.loadData("faturamento", df4, {"reservation_time", "price"}, false);
    loader.loadData("faturamentoMetodo", df5, {"payment_method", "price"}, false);

    auto end = Clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "\n⏱️ Tempo (sequencial): " << duration.count() << " ms\n";
}

void testParallelPipeline(int numThreads) {
    auto start = std::chrono::high_resolution_clock::now();

    // Criação da thread pool
    ThreadPool pool(numThreads);

    // Criação do extractor e carregamento do DataFrame
    Extractor extractor;
    auto futureDf = pool.addTask(&Extractor::extractFromJson, &extractor, "../generator/orders.json");
    DataFrame<std::string> df = futureDf.get();

    // Criando handlers para processamento dos dados
    ValidationHandler validationHandler;
    DateHandler dateHandler;
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;

    // Processando o DataFrame com os handlers em paralelo
    auto f1 = pool.addTask(&ValidationHandler::process, &validationHandler, df);
    auto f2 = pool.addTask(&DateHandler::process, &dateHandler, df);
    DataFrame<std::string> dfValid = f1.get();
    DataFrame<std::string> dfDated = f2.get();

    // Continuando com o processamento
    auto f3 = pool.addTask(&RevenueHandler::process, &revenueHandler, dfDated);
    auto f4 = pool.addTask(&CardRevenueHandler::process, &cardHandler, dfDated);
    DataFrame<std::string> dfRevenue = f3.get();
    DataFrame<std::string> dfCards = f4.get();

    // Criando e conectando ao banco de dados
    DataBase db("earning.db");
    db.createTable("faturamento", "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo", "(payment_method TEXT PRIMARY KEY, price REAL)");

    // Carregando os dados processados no banco
    Loader loader(db);
    auto f5 = pool.addTask(&Loader::loadData, &loader, "faturamento", dfRevenue, std::vector<std::string>{"reservation_time", "price"}, false);
    auto f6 = pool.addTask(&Loader::loadData, &loader, "faturamentoMetodo", dfCards, std::vector<std::string>{"payment_method", "price"}, false);

    // Aguardando as inserções terminarem
    f5.get();
    f6.get();

    // Calculando o tempo total do processamento
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "\n⏱️ Tempo (paralelo com " << numThreads << " threads): " << duration.count() << " ms\n";
}

int main() {
    std::cout << "\n=== Benchmark: Versão SEQUENCIAL ===" << std::endl;
    testSequentialPipeline();

    std::cout << "\n=== Benchmark: Versão PARALELA (8 threads) ===" << std::endl;
    testParallelPipeline(8);

    return 0;
}