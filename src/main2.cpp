#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <random>
#include "dataframe.hpp"
#include "extractor.hpp"
#include "trigger.hpp"
#include "handler.hpp"
#include "database.h"
#include "loader.hpp"
#include "ThreadPool.hpp"
#include "queue.hpp"

using Clock = std::chrono::high_resolution_clock;

// Print functions remain unchanged
void printTableHeader() {
    std::cout << "\n------------------------------------------------------------------------------------------------------------------------------------------------------" << std::endl;
    std::cout << "| Arquivo        | Seq. Process | Seq. Load | Par. (4) Process | Par. (4) Load | Par. (8) Process | Par. (8) Load | Par. (12) Process | Par. (12) Load |" << std::endl;
    std::cout << "--------------------------------------------------------------------------------------------------------------------------------------------------------" << std::endl;
}

void printTableRow(const std::string& nomeArquivo, 
        const long& seqProc, const long& seqLoad,
        const long& par4Proc, const long& par4Load,
        const long& par8Proc, const long& par8Load,
        const long& par12Proc, const long& par12Load) {
    std::cout << "| " << std::setw(14) << std::left << nomeArquivo
    << " | " << std::setw(12) << std::right << seqProc
    << " | " << std::setw(9) << std::right << seqLoad
    << " | " << std::setw(16) << std::right << par4Proc
    << " | " << std::setw(13) << std::right << par4Load
    << " | " << std::setw(16) << std::right << par8Proc
    << " | " << std::setw(13) << std::right << par8Load
    << " | " << std::setw(16) << std::right << par12Proc
    << " | " << std::setw(13) << std::right << par12Load
    << " |" << std::endl;
    std::cout << "-----------------------------------------------------------------------------------------------------------------" << std::endl;
}

// Modified TestResults to track incremental runs
struct TestResults {
    struct RunStats {
        long processingTime;
        long loadTime;
        size_t linesProcessed;
    };
    
    std::vector<RunStats> sequentialRuns;
    std::vector<RunStats> parallel4Runs;
    std::vector<RunStats> parallel8Runs;
    std::vector<RunStats> parallel12Runs;
};

// Modified sequential pipeline to work with data chunks
void processSequentialChunk(DataBase& db, const std::string& nomeArquivo, 
                          DataFrame<std::string>& df,
                          TestResults::RunStats& stats) {
    auto start = Clock::now();

    // Create tables if they don't exist
    db.createTable("faturamento" + nomeArquivo, "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo" + nomeArquivo, "(payment_method TEXT PRIMARY KEY, price REAL)");

    // Processing handlers
    ValidationHandler validationHandler;
    StatusFilterHandler statusFilterHandler("confirmed");
    DateHandler dateHandler;
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;

    // Process data
    auto startProcessing = Clock::now();
    DataFrame<std::string> df2 = validationHandler.process(df);
    DataFrame<std::string> df3 = statusFilterHandler.process(df2);
    DataFrame<std::string> df4 = dateHandler.process(df3);
    DataFrame<std::string> df5 = revenueHandler.process(df4);
    DataFrame<std::string> df6 = cardHandler.process(df4);
    auto endProcessing = Clock::now();

    // Load data
    Loader loader(db);
    auto startLoad = Clock::now();
    loader.loadData("faturamento" + nomeArquivo, df5, {"reservation_time", "price"}, false);
    loader.loadData("faturamentoMetodo" + nomeArquivo, df6, {"payment_method", "price"}, false);
    auto endLoad = Clock::now();

    // Update stats
    stats.processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(endProcessing - startProcessing).count();
    stats.loadTime = std::chrono::duration_cast<std::chrono::milliseconds>(endLoad - startLoad).count();
    stats.linesProcessed = df.numRows();
}

// Modified parallel pipeline to work with data chunks
void processParallelChunk(int numThreads, DataBase& db, const std::string& nomeArquivo,
                        DataFrame<std::string>& df,
                        TestResults::RunStats& stats) {
    auto start = Clock::now();
    ThreadPool pool(numThreads);

    // Create queues
    Queue<int, DataFrame<std::string>> partitionQueue(numThreads);
    Queue<int, DataFrame<std::string>> revenueQueue(numThreads);
    Queue<int, DataFrame<std::string>> cardQueue(numThreads);

    // Partition the DataFrame
    size_t chunk_size = df.numRows() / numThreads;
    for (int i = 0; i < numThreads; ++i) {
        size_t start_idx = i * chunk_size;
        size_t end_idx = (i == numThreads - 1) ? df.numRows() : start_idx + chunk_size;
        partitionQueue.enQueue({i, df.extractLines(start_idx, end_idx)});
    }

    // Create handlers
    ValidationHandler validationHandler;
    StatusFilterHandler statusFilterHandler("confirmed");
    DateHandler dateHandler;
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;

    // Process data
    auto startProcessing = Clock::now();
    std::vector<std::future<void>> futures;

    for (int i = 0; i < numThreads; ++i) {
        futures.push_back(pool.addTask([&, i] {
            auto [index, chunk] = partitionQueue.deQueue();
            auto validChunk = validationHandler.process(chunk);
            auto filteredChunk = statusFilterHandler.process(validChunk);
            auto datedChunk = dateHandler.process(filteredChunk);
            
            auto revenueChunk = revenueHandler.process(datedChunk);
            auto cardChunk = cardHandler.process(datedChunk);

            pool.addTask([&, index, revenueChunk] {
                revenueQueue.enQueue({index, revenueChunk});
            });

            pool.addTask([&, index, cardChunk] {
                cardQueue.enQueue({index, cardChunk});
            });
        }));
    }

    for (auto& fut : futures) {
        fut.get();
    }
    auto endProcessing = Clock::now();

    // Create tables
    db.createTable("faturamento" + nomeArquivo + "_" + std::to_string(numThreads), 
                  "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo" + nomeArquivo + "_" + std::to_string(numThreads),
                  "(payment_method TEXT PRIMARY KEY, price REAL)");

    // Load data
    Loader loader(db);
    auto startLoad = Clock::now();
    for (int i = 0; i < numThreads; ++i) {
        auto [index, revenueChunk] = revenueQueue.deQueue();
        auto [indexCard, cardChunk] = cardQueue.deQueue();

        loader.loadData("faturamento" + nomeArquivo + "_" + std::to_string(numThreads), 
                      revenueChunk, {"reservation_time", "price"}, true);
        loader.loadData("faturamentoMetodo" + nomeArquivo + "_" + std::to_string(numThreads),
                      cardChunk, {"payment_method", "price"}, true);
    }
    auto endLoad = Clock::now();

    // Update stats
    stats.processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(endProcessing - startProcessing).count();
    stats.loadTime = std::chrono::duration_cast<std::chrono::milliseconds>(endLoad - startLoad).count();
    stats.linesProcessed = df.numRows();
}

// Modified Test function with new trigger system
void Test() {
    Extractor extractor;
    DataBase db("../databases/DB_Teste.db");
    const std::string file_path = "../generator/ordersCemMil.json";
    TestResults results;

    // Initialize triggers with simple void() callbacks
    auto timer_trigger = std::make_shared<TimerTrigger>(5000);
    auto request_trigger = std::make_shared<RequestTrigger>();

    // Set up processing functions
    auto processSequential = [&]() {
        DataFrame<std::string> df = extractor.extractFromJson(file_path);
        if (df.numRows() == 0) return;
        
        TestResults::RunStats stats;
        processSequentialChunk(db, "ordersCemMil", df, stats);
        results.sequentialRuns.push_back(stats);
    };

    auto processParallel = [&]() {
        DataFrame<std::string> df = extractor.extractFromJson(file_path);
        if (df.numRows() == 0) return;
        
        TestResults::RunStats stats;
        processParallelChunk(8, db, "ordersCemMil", df, stats);
        results.parallel8Runs.push_back(stats);
    };

    // Assign callbacks
    timer_trigger->setCallback(processSequential);
    request_trigger->setCallback(processParallel);

    // Start triggers
    printTableHeader();
    timer_trigger->start();
    request_trigger->start();

    // Simulate random requests
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(1, 5);

    for (int i = 0; i < 3; ++i) {
        std::this_thread::sleep_for(std::chrono::seconds(dist(gen)));
        request_trigger->trigger();
    }

    // Run for 30 seconds
    std::this_thread::sleep_for(std::chrono::seconds(30));
    timer_trigger->stop();
    request_trigger->stop();

    // Calculate and print aggregated results
    auto calculateAverages = [](const std::vector<TestResults::RunStats>& runs) {
        long totalProc = 0, totalLoad = 0;
        size_t totalLines = 0;
        for (const auto& run : runs) {
            totalProc += run.processingTime;
            totalLoad += run.loadTime;
            totalLines += run.linesProcessed;
        }
        return std::make_tuple(
            runs.empty() ? 0 : totalProc / runs.size(),
            runs.empty() ? 0 : totalLoad / runs.size(),
            totalLines
        );
    };

    auto [avgSeqProc, avgSeqLoad, totalSeqLines] = calculateAverages(results.sequentialRuns);
    auto [avgPar8Proc, avgPar8Load, totalPar8Lines] = calculateAverages(results.parallel8Runs);

    printTableRow("ordersCemMil", 
                avgSeqProc, avgSeqLoad,
                0, 0, // Placeholder for parallel4
                avgPar8Proc, avgPar8Load,
                0, 0); // Placeholder for parallel12

    std::cout << "\nTotal lines processed - Sequential: " << totalSeqLines 
              << " | Parallel8: " << totalPar8Lines << "\n";
}

int main() {
    Test();
    return 0;
}