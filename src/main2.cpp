#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <random>
#include <mutex>
#include "dataframe.hpp"
#include "extractor.hpp"
#include "trigger.hpp"
#include "handler.hpp"
#include "database.h"
#include "loader.hpp"
#include "ThreadPool.hpp"
#include "queue.hpp"

using Clock = std::chrono::high_resolution_clock;

// Global mutex for thread-safe printing
std::mutex table_mutex;

void printTableHeader()
{
    std::lock_guard<std::mutex> lock(table_mutex);
    std::cout << "\n----------------------------------------------------------------------------------------------------------------------------------------------------------" << std::endl;
    std::cout << "| Trigger   | Lines | Seq. Process | Seq. Load | Par. (4) Process | Par. (4) Load | Par. (8) Process | Par. (8) Load | Par. (12) Process | Par. (12) Load |" << std::endl;
    std::cout << "------------------------------------------------------------------------------------------------------------------------------------------------------------" << std::endl;
}

void printTableRow(const std::string &triggerType, size_t linesProcessed,
                   const long &seqProc, const long &seqLoad,
                   const long &par4Proc, const long &par4Load,
                   const long &par8Proc, const long &par8Load,
                   const long &par12Proc, const long &par12Load)
{
    std::lock_guard<std::mutex> lock(table_mutex);
    std::cout << "| " << std::setw(9) << std::left << triggerType
              << " | " << std::setw(5) << std::right << linesProcessed
              << " | " << std::setw(12) << std::right << seqProc
              << " | " << std::setw(9) << std::right << seqLoad
              << " | " << std::setw(16) << std::right << par4Proc
              << " | " << std::setw(13) << std::right << par4Load
              << " | " << std::setw(16) << std::right << par8Proc
              << " | " << std::setw(13) << std::right << par8Load
              << " | " << std::setw(16) << std::right << par12Proc
              << " | " << std::setw(13) << std::right << par12Load
              << " |" << std::endl;
    std::cout << "------------------------------------------------------------------------------------------------------------------------------------------------------------" << std::endl;
}

struct TestResults
{
    struct RunStats
    {
        // Processing times
        long sequentialProcessingTime = 0;
        long sequentialLoadTime = 0;
        long parallel4ProcessingTime = 0;
        long parallel4LoadTime = 0;
        long parallel8ProcessingTime = 0;
        long parallel8LoadTime = 0;
        long parallel12ProcessingTime = 0;
        long parallel12LoadTime = 0;

        // Metadata
        size_t linesProcessed = 0;
        std::string triggerType;
        std::chrono::time_point<Clock> startTime;
        std::chrono::time_point<Clock> endTime;
    };

    std::vector<RunStats> allRuns;
};

void processSequentialChunk(DataBase &db, const std::string &nomeArquivo,
    DataFrame<std::string> &df,
    TestResults::RunStats &stats)
{
    stats.startTime = Clock::now();

    // Load supporting data
    Extractor extractor;
    auto users_df = std::make_shared<const DataFrame<std::string>>(
        extractor.extractFromCsv("../generator/users.csv")
    );
    auto flight_seats_df = std::make_shared<const DataFrame<std::string>>(
        extractor.extractFromCsv("../generator/flights_seats.csv")
    );
    auto flights_df = std::make_shared<const DataFrame<std::string>>(
        extractor.extractFromCsv("../generator/flights.csv")
    );

    // Criar os mapas uma vez
    std::unordered_map<std::string, std::string> userIdToCountry;
    for (int i = 0; i < users_df->numRows(); ++i) {
        userIdToCountry[users_df->getValue("user_id", i)] = users_df->getValue("country", i);
    }

    std::unordered_map<std::string, std::string> seatKeyToClass;
    for (int i = 0; i < flight_seats_df->numRows(); ++i) {
        std::string key = flight_seats_df->getValue("flight_id", i) + "_" + flight_seats_df->getValue("seat", i);
        seatKeyToClass[key] = flight_seats_df->getValue("seat_class", i);
    }

    // Create tables
    db.createTable("faturamento_" + nomeArquivo, "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo_" + nomeArquivo, "(payment_method TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoPaisUsuario_" + nomeArquivo, "(user_country TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoTipoAssento_" + nomeArquivo, "(seat_type TEXT PRIMARY KEY, price REAL)");
    db.createTable("flight_stats_" + nomeArquivo, "(flight_number TEXT PRIMARY KEY, reservation_count INTEGER)");
    db.createTable("destination_stats_" + nomeArquivo, "(most_common_destination TEXT PRIMARY KEY, reservation_count INTEGER)");

    // Processing
    auto startProcessing = Clock::now();
    ValidationHandler validationHandler;
    StatusFilterHandler statusFilterHandler("confirmed");
    UsersCountryRevenue userHandler(userIdToCountry);
    SeatTypeRevenue seatHandler(seatKeyToClass);
    DateHandler dateHandler;
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;
    FlightInfoEnricherHandler flightEnricher(*flights_df);
    DestinationCounterHandler destinationCounter;

    DataFrame<std::string> processed = validationHandler.process(df);
    processed = statusFilterHandler.process(processed);

    // Run new handlers
    auto flightResults = flightEnricher.processMulti({processed});
    DataFrame<std::string> enrichedDf = flightResults[0];
    DataFrame<std::string> flightStats = flightResults[1];
    
    DataFrame<std::string> destinationStats = destinationCounter.process(enrichedDf);
    
    DataFrame<std::string> porPais = userHandler.process(enrichedDf);
    DataFrame<std::string> porClasse = seatHandler.process(enrichedDf);

    processed = dateHandler.process(enrichedDf);
    DataFrame<std::string> revenue = revenueHandler.process(processed);
    DataFrame<std::string> cards = cardHandler.process(processed);
    auto endProcessing = Clock::now();

    // Loading
    Loader loader(db);
    auto startLoad = Clock::now();
    loader.loadData("faturamento_" + nomeArquivo, revenue, {"reservation_time", "price"}, false);
    loader.loadData("faturamentoMetodo_" + nomeArquivo, cards, {"payment_method", "price"}, false);
    loader.loadData("faturamentoPaisUsuario_" + nomeArquivo, porPais, {"user_country", "price"}, false);
    loader.loadData("faturamentoTipoAssento_" + nomeArquivo, porClasse, {"seat_type", "price"}, false);
    loader.loadData("flight_stats_" + nomeArquivo, flightStats, {"flight_number", "reservation_count"}, false);
    loader.loadData("destination_stats_" + nomeArquivo, destinationStats, {"most_common_destination", "reservation_count"}, false);
    auto endLoad = Clock::now();

    // Update stats
    stats.sequentialProcessingTime = std::chrono::duration_cast<std::chrono::milliseconds>(endProcessing - startProcessing).count();
    stats.sequentialLoadTime = std::chrono::duration_cast<std::chrono::milliseconds>(endLoad - startLoad).count();
    stats.linesProcessed = df.numRows();
    stats.endTime = Clock::now();
}

void processParallelChunk(int numThreads, DataBase &db, const std::string &nomeArquivo,
    DataFrame<std::string> &df,
    TestResults::RunStats &stats)
{
    auto start = Clock::now();

    Extractor extractor;
    auto users_df = std::make_shared<const DataFrame<std::string>>(
        extractor.extractFromCsv("../generator/users.csv")
    );
    auto flight_seats_df = std::make_shared<const DataFrame<std::string>>(
        extractor.extractFromCsv("../generator/flights_seats.csv")
    );
    auto flights_df = std::make_shared<const DataFrame<std::string>>(
        extractor.extractFromCsv("../generator/flights.csv")
    );

    std::string tableSuffix = "_" + nomeArquivo + "_" + std::to_string(numThreads);
    db.createTable("faturamento" + tableSuffix, "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo" + tableSuffix, "(payment_method TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoPaisUsuario" + tableSuffix, "(user_country TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoTipoAssento" + tableSuffix, "(seat_type TEXT PRIMARY KEY, price REAL)");
    db.createTable("flight_stats" + tableSuffix, "(flight_number TEXT PRIMARY KEY, reservation_count INTEGER)");
    db.createTable("destination_stats" + tableSuffix, "(most_common_destination TEXT PRIMARY KEY, reservation_count INTEGER)");

    ThreadPool pool(numThreads);
    Queue<int, DataFrame<std::string>> partitionQueue(numThreads);
    Queue<int, DataFrame<std::string>> processedQueue(numThreads);
    Queue<int, DataFrame<std::string>> userCountryQueue(numThreads);
    Queue<int, DataFrame<std::string>> seatTypeQueue(numThreads);
    Queue<int, DataFrame<std::string>> flightStatsQueue(numThreads);
    Queue<int, DataFrame<std::string>> destinationStatsQueue(numThreads);

    std::unordered_map<std::string, std::string> userIdToCountry;
    for (int i = 0; i < users_df->numRows(); ++i)
        userIdToCountry[users_df->getValue("user_id", i)] = users_df->getValue("country", i);

    std::unordered_map<std::string, std::string> seatKeyToClass;
    for (int i = 0; i < flight_seats_df->numRows(); ++i)
        seatKeyToClass[flight_seats_df->getValue("flight_id", i) + "_" + flight_seats_df->getValue("seat", i)] =
            flight_seats_df->getValue("seat_class", i);

    // Create shared handlers
    auto sharedFlightEnricher = std::make_shared<FlightInfoEnricherHandler>(*flights_df);
    auto sharedDestinationCounter = std::make_shared<DestinationCounterHandler>();

    // Partition the data
    size_t chunk_size = df.numRows() / numThreads;
    for (int i = 0; i < numThreads; ++i)
    {
        size_t start_idx = i * chunk_size;
        size_t end_idx = (i == numThreads - 1) ? df.numRows() : start_idx + chunk_size;
        partitionQueue.enQueue({i, df.extractLines(start_idx, end_idx)});
    }

    // Processing phase
    auto startProcessing = Clock::now();
    std::vector<std::future<void>> processingFutures;

    ValidationHandler validationHandler;
    StatusFilterHandler statusFilterHandler("confirmed");
    DateHandler dateHandler;
    
    // Instâncias únicas thread-safe
    auto sharedUserHandler = std::make_shared<UsersCountryRevenue>(userIdToCountry);
    auto sharedSeatHandler = std::make_shared<SeatTypeRevenue>(seatKeyToClass);

    for (int i = 0; i < numThreads; ++i)
    {
        processingFutures.push_back(pool.addTask([&, i, sharedUserHandler, sharedSeatHandler, sharedFlightEnricher, sharedDestinationCounter]()
        {
            auto [idx, chunk] = partitionQueue.deQueue();
            auto processed = validationHandler.process(chunk);
            processed = statusFilterHandler.process(processed);

            // Process flight enrichment
            auto flightResults = sharedFlightEnricher->processMulti({processed});
            DataFrame<std::string> enrichedDf = flightResults[0];
            DataFrame<std::string> flightStats = flightResults[1];
            
            // Process destination stats
            DataFrame<std::string> destinationStats = sharedDestinationCounter->process(enrichedDf);
            
            // Process other handlers
            DataFrame<std::string> countryRevenue = sharedUserHandler->process(enrichedDf);
            DataFrame<std::string> seatRevenue = sharedSeatHandler->process(enrichedDf);

            userCountryQueue.enQueue({idx, countryRevenue});
            seatTypeQueue.enQueue({idx, seatRevenue});
            flightStatsQueue.enQueue({idx, flightStats});
            destinationStatsQueue.enQueue({idx, destinationStats});

            processed = dateHandler.process(enrichedDf);
            processedQueue.enQueue({idx, processed});
        }));
    }

    for (auto &fut : processingFutures)
        fut.get();
    auto endProcessing = Clock::now();

    // Aggregate all processed data
    DataFrame<std::string> allProcessed;
    for (int i = 0; i < numThreads; ++i)
    {
        auto [idx, processed] = processedQueue.deQueue();
        allProcessed = (i == 0) ? processed : allProcessed.concat(processed);
    }

    // Aggregate user country data
    DataFrame<std::string> allUserCountry;
    for (int i = 0; i < numThreads; ++i)
    {
        auto [idx, countryDf] = userCountryQueue.deQueue();
        allUserCountry = (i == 0) ? countryDf : allUserCountry.concat(countryDf);
    }

    // Aggregate seat type data
    DataFrame<std::string> allSeatType;
    for (int i = 0; i < numThreads; ++i)
    {
        auto [idx, seatDf] = seatTypeQueue.deQueue();
        allSeatType = (i == 0) ? seatDf : allSeatType.concat(seatDf);
    }

    // Aggregate flight stats
    DataFrame<std::string> allFlightStats;
    for (int i = 0; i < numThreads; ++i)
    {
        auto [idx, flightDf] = flightStatsQueue.deQueue();
        allFlightStats = (i == 0) ? flightDf : allFlightStats.concat(flightDf);
    }

    // Aggregate destination stats
    DataFrame<std::string> allDestinationStats;
    for (int i = 0; i < numThreads; ++i)
    {
        auto [idx, destDf] = destinationStatsQueue.deQueue();
        allDestinationStats = (i == 0) ? destDf : allDestinationStats.concat(destDf);
    }

    // Final aggregation phase
    auto startAggregation = Clock::now();
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;

    DataFrame<std::string> revenue = revenueHandler.process(allProcessed);
    DataFrame<std::string> cards = cardHandler.process(allProcessed);

    auto aggregatedRevenue = revenue.groupby("reservation_time", "price");
    auto aggregatedCards = cards.groupby("payment_method", "price");
    auto aggregatedFlightStats = allFlightStats.groupby("flight_number", "reservation_count");
    auto aggregatedDestinationStats = allDestinationStats.groupby("most_common_destination", "reservation_count");
    auto aggregatedUserCountry = allUserCountry.groupby("user_country", "price");
    auto aggregatedSeatType = allSeatType.groupby("seat_type", "price");
    auto endAggregation = Clock::now();

    // Load all data into DB
    Loader loader(db);
    auto startLoad = Clock::now();
    loader.loadData("faturamento" + tableSuffix, aggregatedRevenue, {"reservation_time", "price"}, false);
    loader.loadData("faturamentoMetodo" + tableSuffix, aggregatedCards, {"payment_method", "price"}, false);
    loader.loadData("faturamentoPaisUsuario" + tableSuffix, aggregatedUserCountry, {"user_country", "price"}, false);
    loader.loadData("faturamentoTipoAssento" + tableSuffix, aggregatedSeatType, {"seat_type", "price"}, false);
    loader.loadData("flight_stats" + tableSuffix, aggregatedFlightStats, {"flight_number", "reservation_count"}, false);
    loader.loadData("destination_stats" + tableSuffix, aggregatedDestinationStats, {"most_common_destination", "reservation_count"}, false);
    auto endLoad = Clock::now();

    long aggregationTime = std::chrono::duration_cast<std::chrono::milliseconds>(endAggregation - startAggregation).count();
    long processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(endProcessing - startProcessing).count();

    switch (numThreads)
    {
        case 4:
            stats.parallel4ProcessingTime = processingTime + aggregationTime;
            stats.parallel4LoadTime = std::chrono::duration_cast<std::chrono::milliseconds>(endLoad - startLoad).count();
            break;
        case 8:
            stats.parallel8ProcessingTime = processingTime + aggregationTime;
            stats.parallel8LoadTime = std::chrono::duration_cast<std::chrono::milliseconds>(endLoad - startLoad).count();
            break;
        case 12:
            stats.parallel12ProcessingTime = processingTime + aggregationTime;
            stats.parallel12LoadTime = std::chrono::duration_cast<std::chrono::milliseconds>(endLoad - startLoad).count();
            break;
    }
}

void Test()
{
    Extractor extractor;
    DataBase db("../databases/DB_Teste.db");
    const std::string file_path = "../generator/ordersCemMil.json";
    TestResults results;

    printTableHeader();

    auto processFullPipeline = [&](const std::string &triggerType, DataFrame<std::string> df)
    {
        if (df.numRows() == 0)
        {
            extractor.resetFilePosition(file_path);
            return;
        }

        TestResults::RunStats stats;
        stats.triggerType = triggerType;
        stats.linesProcessed = df.numRows();

        // Run all pipeline variants
        processSequentialChunk(db, "ordersCemMil", df, stats);
        processParallelChunk(4, db, "ordersCemMil", df, stats);
        processParallelChunk(8, db, "ordersCemMil", df, stats);
        processParallelChunk(12, db, "ordersCemMil", df, stats);

        results.allRuns.push_back(stats);

        // Print to table with trigger type and line count
        printTableRow(stats.triggerType,
                      stats.linesProcessed,
                      stats.sequentialProcessingTime,
                      stats.sequentialLoadTime,
                      stats.parallel4ProcessingTime,
                      stats.parallel4LoadTime,
                      stats.parallel8ProcessingTime,
                      stats.parallel8LoadTime,
                      stats.parallel12ProcessingTime,
                      stats.parallel12LoadTime);
    };

    // TimerTrigger - processes random chunks (7000-21000 lines) every 5 seconds
    auto timer_trigger = std::make_shared<TimerTrigger>(15000);
    timer_trigger->setCallback([&]()
                               {
        DataFrame<std::string> df = extractor.extractRandomChunk(file_path, 7000, 21000);
        processFullPipeline("Timer", df); });

    // RequestTrigger - processes fixed 15000-line chunks
    auto request_trigger = std::make_shared<RequestTrigger>();
    request_trigger->setCallback([&]()
                                 {
        DataFrame<std::string> df = extractor.extractChunk(file_path, 15000);
        processFullPipeline("Request", df); });

    // Start triggers
    timer_trigger->start();
    request_trigger->start();

    // Simulate random requests
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(2, 5);

    for (int i = 0; i < 3; ++i)
    {
        std::this_thread::sleep_for(std::chrono::seconds(dist(gen)));
        request_trigger->trigger();
    }

    // Run for 30 seconds
    std::this_thread::sleep_for(std::chrono::seconds(30));
    timer_trigger->stop();
    request_trigger->stop();

    // Final summary
    std::cout << "\n=== FINAL SUMMARY ===\n";
    std::cout << "Total executions: " << results.allRuns.size() << "\n";

    // Calculate totals instead of averages
    long totalSeqProc = 0, totalSeqLoad = 0;
    long totalPar4Proc = 0, totalPar4Load = 0;
    long totalPar8Proc = 0, totalPar8Load = 0;
    long totalPar12Proc = 0, totalPar12Load = 0;
    size_t totalLines = 0;

    for (const auto &run : results.allRuns)
    {
        totalSeqProc += run.sequentialProcessingTime;
        totalSeqLoad += run.sequentialLoadTime;
        totalPar4Proc += run.parallel4ProcessingTime;
        totalPar4Load += run.parallel4LoadTime;
        totalPar8Proc += run.parallel8ProcessingTime;
        totalPar8Load += run.parallel8LoadTime;
        totalPar12Proc += run.parallel12ProcessingTime;
        totalPar12Load += run.parallel12LoadTime;
        totalLines += run.linesProcessed;
    }

    std::cout << "\nTotal Times (ms):\n";
    std::cout << "Sequential: Processing=" << totalSeqProc << " | Loading=" << totalSeqLoad << "\n";
    std::cout << "Parallel 4: Processing=" << totalPar4Proc << " | Loading=" << totalPar4Load << "\n";
    std::cout << "Parallel 8: Processing=" << totalPar8Proc << " | Loading=" << totalPar8Load << "\n";
    std::cout << "Parallel 12: Processing=" << totalPar12Proc << " | Loading=" << totalPar12Load << "\n";
    std::cout << "Total lines processed: " << totalLines << "\n";
}

int main()
{
    Test();
    
    return 0;
}