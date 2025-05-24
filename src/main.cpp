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
#include "threadPool.hpp"
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

unsigned int getOptimalThreadCount() {
    unsigned int available_threads = std::thread::hardware_concurrency();
    
    // Se não conseguir detectar, usa um valor padrão seguro (4 threads)
    if(available_threads == 0) {
        std::cerr << "Não foi possível detectar o número de núcleos. Usando 4 threads como padrão." << std::endl;
        return 4;
    }
    
    std::cout << "Número de threads disponíveis: " << available_threads << std::endl;
    return available_threads;
}

void processParallelChunk(int numThreads, DataBase &db, const std::string &nomeArquivo,
    DataFrame<std::string> &df,
    TestResults::RunStats &stats,
    bool bfirstTime)
{
    auto start = Clock::now();
    std::string tableSuffix = "_" + nomeArquivo + "_" + std::to_string(numThreads);

    std::shared_ptr<const DataFrame<std::string>> users_df;
    std::shared_ptr<const DataFrame<std::string>> flight_seats_df;
    std::shared_ptr<const DataFrame<std::string>> flights_df;
    std::unordered_map<std::string, std::string> userIdToCountry;
    std::unordered_map<std::string, std::string> seatKeyToClass;
    std::vector<DataFrame<std::string>> dfMeanPrices;

    Extractor extractor;
    users_df = std::make_shared<const DataFrame<std::string>>(
        extractor.extractFromCsv("../generator/users.csv")
    );
    flight_seats_df = std::make_shared<const DataFrame<std::string>>(
        extractor.extractFromCsv("../generator/flights_seats.csv")
    );
    flights_df = std::make_shared<const DataFrame<std::string>>(
        extractor.extractFromCsv("../generator/flights.csv")
    );

    if (bfirstTime)
    {
        db.createTable("faturamento" + tableSuffix, "(reservation_time TEXT PRIMARY KEY, price REAL)");
        db.createTable("faturamentoMetodo" + tableSuffix, "(payment_method TEXT PRIMARY KEY, price REAL)");
        db.createTable("faturamentoPaisUsuario" + tableSuffix, "(user_country TEXT PRIMARY KEY, price REAL)");
        db.createTable("faturamentoTipoAssento" + tableSuffix, "(seat_type TEXT PRIMARY KEY, price REAL)");
        db.createTable("flight_stats" + tableSuffix, "(flight_number TEXT PRIMARY KEY, reservation_count INTEGER)");
        db.createTable("destination_stats" + tableSuffix, "(destination TEXT PRIMARY KEY, reservation_count INTEGER)");
        db.createTable("precoMedioPorDestino" + tableSuffix, "(destination TEXT PRIMARY KEY, mean_avg_price REAL)");
        db.createTable("precoMedioPorAirline" + tableSuffix, "(airline TEXT PRIMARY KEY, mean_avg_price REAL)");

        MeanPricePerDestination_AirlineHandler MeanPriceHandler;
        dfMeanPrices = MeanPriceHandler.processMultiShared({flight_seats_df, flights_df});
        dfMeanPrices[0].renameColumn("to", "destination");
    }

    for (int i = 0; i < users_df->numRows(); ++i)
            userIdToCountry[users_df->getValue("user_id", i)] = users_df->getValue("country", i);
    
    for (int i = 0; i < flight_seats_df->numRows(); ++i)
        seatKeyToClass[flight_seats_df->getValue("flight_id", i) + "_" + flight_seats_df->getValue("seat", i)] =
            flight_seats_df->getValue("seat_class", i);

    // Create shared handlers
    auto sharedFlightEnricher = std::make_shared<FlightInfoEnricherHandler>(*flights_df);
    auto sharedDestinationCounter = std::make_shared<DestinationCounterHandler>();

    ThreadPool pool(numThreads);
    Queue<int, DataFrame<std::string>> partitionQueue(numThreads);
    Queue<int, DataFrame<std::string>> processedQueue(numThreads);
    Queue<int, DataFrame<std::string>> userCountryQueue(numThreads);
    Queue<int, DataFrame<std::string>> seatTypeQueue(numThreads);
    Queue<int, DataFrame<std::string>> flightStatsQueue(numThreads);
    Queue<int, DataFrame<std::string>> destinationStatsQueue(numThreads);

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
    auto aggregatedDestinationStats = allDestinationStats.groupby("destination", "reservation_count");
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
    loader.loadData("destination_stats" + tableSuffix, aggregatedDestinationStats, {"destination", "reservation_count"}, false);
    if (bfirstTime)
    {
        loader.loadData("precoMedioPorDestino" + tableSuffix, dfMeanPrices[0], {"destination", "mean_avg_price"}, false);
        loader.loadData("precoMedioPorAirline" + tableSuffix, dfMeanPrices[1], {"airline", "mean_avg_price"}, false);
    }
    auto endLoad = Clock::now();

    long aggregationTime = std::chrono::duration_cast<std::chrono::milliseconds>(endAggregation - startAggregation).count();
    long processingTime = std::chrono::duration_cast<std::chrono::milliseconds>(endProcessing - startProcessing).count();

    switch (numThreads)
    {   
        case 1:
            stats.sequentialProcessingTime = processingTime + aggregationTime;
            stats.sequentialLoadTime = std::chrono::duration_cast<std::chrono::milliseconds>(endLoad - startLoad).count();
            break;
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
    DataBase db("../databases/Database.db");
    const std::string file_path = "../generator/orders.json";
    TestResults results;

    DataBase dbMock("../databases/MockSQL.db");
    Loader loaderMock(dbMock);
    std::string createQuery = "(flight_id TEXT, seat TEXT, user_id BIGINT, customer_name TEXT, status TEXT, payment_method TEXT, reservation_time DATE, price FLOAT, timestamp INTEGER)";
    dbMock.createTable("MockData", createQuery);

    bool bFirstTime = true;

    printTableHeader();

    auto processFullPipeline = [&](const std::string &triggerType, DataFrame<std::string> df)
    {
        if (df.numRows() == 0)
        {
            extractor.resetFilePosition(file_path);
            // DEBUG: DataFrame is empty. Skipping processing.
            return;
        }

        long total_latency = 0;
        long valid_timestamp_count = 0;
        auto now = std::chrono::system_clock::now();
        auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
        long current_time = now_ms.time_since_epoch().count();

        for (int i = 0; i < df.numRows(); ++i) {
            std::string timestamp_str = df.getValue("timestamp", i);
            // DEBUG: Timestamp string: [" << timestamp_str << "]
            try {
                long event_time = std::stol(timestamp_str);
                if (event_time != 0) {
                    total_latency += (current_time - event_time);
                    valid_timestamp_count++;
                } else {
                    // DEBUG: Skipping timestamp '0' for latency calculation.
                }
            } catch (const std::invalid_argument& e) {
                // ERROR: Invalid timestamp string for std::stol: [" << timestamp_str << "] - " << e.what()
            } catch (const std::out_of_range& e) {
                // ERROR: Timestamp string out of range for long: [" << timestamp_str << "] - " << e.what()
            }
        }

        if (valid_timestamp_count > 0) {
            long avg_latency = total_latency / valid_timestamp_count;
            std::cout << "Average latency for " << triggerType << ": " << avg_latency << "ms\n";
        } else {
            std::cout << "No valid timestamps found for latency calculation in " << triggerType << " trigger." << std::endl;
        }


        TestResults::RunStats stats;
        stats.triggerType = triggerType;
        stats.linesProcessed = df.numRows();

        processParallelChunk(1, db, "orders", df, stats, bFirstTime);
        processParallelChunk(4, db, "orders", df, stats, bFirstTime);
        processParallelChunk(8, db, "orders", df, stats, bFirstTime);
        processParallelChunk(12, db, "orders", df, stats, bFirstTime);

        if (bFirstTime) { bFirstTime = false; }

        results.allRuns.push_back(stats);

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

    std::vector<std::string> columns = {
        "flight_id", "seat", "user_id", "customer_name",
        "status", "payment_method", "reservation_time", "price", "timestamp"
    };

    auto SQLiteMockTrigger = std::make_shared<TimerTrigger>(1000);
    SQLiteMockTrigger->setCallback([&](){
        DataFrame<std::string> df = extractor.extractRandomChunk(file_path, columns, 5000, 15000);
        loaderMock.loadData("MockData", df, {"flight_id", "seat", "user_id", "customer_name", "status", "payment_method", "reservation_time", "price", "timestamp"}, true);
    });

    auto timer_trigger = std::make_shared<TimerTrigger>(10000);
    timer_trigger->setCallback([&](){
        std::string tableName = "MockData";
        std::string dbFilePath = "../databases/MockSQL.db";

        DataFrame<std::string> df = extractor.extractFromSqlite(dbFilePath, tableName);

        if (df.numRows() > 0) {
            processFullPipeline("Timer", df);
        } else {
            std::cout << "Nenhum dado encontrado para processamento!" << std::endl;
        }
    });

    SQLiteMockTrigger->start();
    timer_trigger->start();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(2, 5);

    std::this_thread::sleep_for(std::chrono::seconds(60));
    SQLiteMockTrigger->stop();
    timer_trigger->stop();

    std::cout << "\n=== FINAL SUMMARY ===\n";
    std::cout << "Total executions: " << results.allRuns.size() << "\n";

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
    // Test();
    std::cout << getOptimalThreadCount();

    return 0;
}