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

struct TestResults {
    struct RunStats {
        long processingTime;
        long loadTime;
        size_t linesProcessed;
        std::string triggerType;
        Clock::time_point startTime;
        Clock::time_point endTime;
    };
    
    std::vector<RunStats> allRuns;
};

void printExecutionSummary(const TestResults::RunStats& stats) {
    auto totalTime = std::chrono::duration_cast<std::chrono::milliseconds>(stats.endTime - stats.startTime).count();
    std::cout << "\n=== " << stats.triggerType << " Trigger Execution ==="
              << "\nLines processed: " << stats.linesProcessed
              << "\nProcessing time: " << stats.processingTime << " ms"
              << "\nLoad time: " << stats.loadTime << " ms"
              << "\nTotal time: " << totalTime << " ms\n";
}

void processSequentialChunk(DataBase& db, const std::string& nomeArquivo, 
                          DataFrame<std::string>& df,
                          TestResults::RunStats& stats) {
    stats.startTime = Clock::now();

    db.createTable("faturamento" + nomeArquivo, "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo" + nomeArquivo, "(payment_method TEXT PRIMARY KEY, price REAL)");

    ValidationHandler validationHandler;
    StatusFilterHandler statusFilterHandler("confirmed");
    DateHandler dateHandler;
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;

    auto startProcessing = Clock::now();
    DataFrame<std::string> df2 = validationHandler.process(df);
    DataFrame<std::string> df3 = statusFilterHandler.process(df2);
    DataFrame<std::string> df4 = dateHandler.process(df3);
    DataFrame<std::string> df5 = revenueHandler.process(df4);
    DataFrame<std::string> df6 = cardHandler.process(df4);
    auto endProcessing = Clock::now();

    Loader loader(db);
    auto startLoad = Clock::now();
    loader.loadData("faturamento" + nomeArquivo, df5, {"reservation_time", "price"}, false);
    loader.loadData("faturamentoMetodo" + nomeArquivo, df6, {"payment_method", "price"}, false);
    auto endLoad = Clock::now();

    stats.processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(endProcessing - startProcessing).count();
    stats.loadTime = std::chrono::duration_cast<std::chrono::milliseconds>(endLoad - startLoad).count();
    stats.linesProcessed = df.numRows();
    stats.endTime = Clock::now();
}

void processParallelChunk(int numThreads, DataBase& db, const std::string& nomeArquivo,
                        DataFrame<std::string>& df,
                        TestResults::RunStats& stats) {
    stats.startTime = Clock::now();
    ThreadPool pool(numThreads);

    Queue<int, DataFrame<std::string>> partitionQueue(numThreads);
    Queue<int, DataFrame<std::string>> revenueQueue(numThreads);
    Queue<int, DataFrame<std::string>> cardQueue(numThreads);

    size_t chunk_size = df.numRows() / numThreads;
    for (int i = 0; i < numThreads; ++i) {
        size_t start_idx = i * chunk_size;
        size_t end_idx = (i == numThreads - 1) ? df.numRows() : start_idx + chunk_size;
        partitionQueue.enQueue({i, df.extractLines(start_idx, end_idx)});
    }

    ValidationHandler validationHandler;
    StatusFilterHandler statusFilterHandler("confirmed");
    DateHandler dateHandler;
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;

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

    db.createTable("faturamento" + nomeArquivo + "_" + std::to_string(numThreads), 
                  "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo" + nomeArquivo + "_" + std::to_string(numThreads),
                  "(payment_method TEXT PRIMARY KEY, price REAL)");

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

    stats.processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(endProcessing - startProcessing).count();
    stats.loadTime = std::chrono::duration_cast<std::chrono::milliseconds>(endLoad - startLoad).count();
    stats.linesProcessed = df.numRows();
    stats.endTime = Clock::now();
}

void Test() {
    Extractor extractor;
    DataBase db("../databases/DB_Teste.db");
    const std::string file_path = "../generator/ordersCemMil.json";
    TestResults results;

    auto timer_trigger = std::make_shared<TimerTrigger>(5000);
    auto request_trigger = std::make_shared<RequestTrigger>();

    auto processSequential = [&]() {
        DataFrame<std::string> df = extractor.extractFromJson(file_path);
        if (df.numRows() == 0) return;
        
        TestResults::RunStats stats;
        stats.triggerType = "Timer";
        processSequentialChunk(db, "ordersCemMil", df, stats);
        results.allRuns.push_back(stats);
        printExecutionSummary(stats);
    };

    auto processParallel = [&]() {
        DataFrame<std::string> df = extractor.extractFromJson(file_path);
        if (df.numRows() == 0) return;
        
        TestResults::RunStats stats;
        stats.triggerType = "Request";
        processParallelChunk(8, db, "ordersCemMil", df, stats);
        results.allRuns.push_back(stats);
        printExecutionSummary(stats);
    };

    timer_trigger->setCallback(processSequential);
    request_trigger->setCallback(processParallel);

    printTableHeader();
    timer_trigger->start();
    request_trigger->start();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(1, 5);

    for (int i = 0; i < 3; ++i) {
        std::this_thread::sleep_for(std::chrono::seconds(dist(gen)));
        request_trigger->trigger();
    }

    std::this_thread::sleep_for(std::chrono::seconds(30));
    timer_trigger->stop();
    request_trigger->stop();

    // Final summary
    auto calculateAverages = [](const std::vector<TestResults::RunStats>& runs, const std::string& type) {
        long totalProc = 0, totalLoad = 0, totalTime = 0;
        size_t totalLines = 0, count = 0;
        
        for (const auto& run : runs) {
            if (run.triggerType == type) {
                totalProc += run.processingTime;
                totalLoad += run.loadTime;
                totalTime += std::chrono::duration_cast<std::chrono::milliseconds>(run.endTime - run.startTime).count();
                totalLines += run.linesProcessed;
                count++;
            }
        }
        
        return std::make_tuple(
            count == 0 ? 0 : totalProc / count,
            count == 0 ? 0 : totalLoad / count,
            totalLines,
            count == 0 ? 0 : totalTime / count
        );
    };

    auto [avgSeqProc, avgSeqLoad, totalSeqLines, avgSeqTime] = calculateAverages(results.allRuns, "Timer");
    auto [avgParProc, avgParLoad, totalParLines, avgParTime] = calculateAverages(results.allRuns, "Request");

    std::cout << "\n=== FINAL SUMMARY ===";
    std::cout << "\nSequential (Timer Trigger):";
    std::cout << "\n  Average Processing Time: " << avgSeqProc << " ms";
    std::cout << "\n  Average Load Time: " << avgSeqLoad << " ms";
    std::cout << "\n  Average Total Time: " << avgSeqTime << " ms";
    std::cout << "\n  Total Lines Processed: " << totalSeqLines;

    std::cout << "\n\nParallel (Request Trigger):";
    std::cout << "\n  Average Processing Time: " << avgParProc << " ms";
    std::cout << "\n  Average Load Time: " << avgParLoad << " ms";
    std::cout << "\n  Average Total Time: " << avgParTime << " ms";
    std::cout << "\n  Total Lines Processed: " << totalParLines << "\n";

    printTableRow("ordersCemMil", 
                avgSeqProc, avgSeqLoad,
                0, 0,
                avgParProc, avgParLoad,
                0, 0);
}

int main() {
    Test();
    return 0;
}