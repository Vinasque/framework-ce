#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <iomanip>
#include <chrono>
#include "series.hpp"
#include "dataframe.hpp"
#include "extractor.hpp"
#include "handler.hpp"
#include "threadWorld.hpp"
#include "loader.hpp"

using namespace std::chrono;

// Function to test Series class
void testSeries() {
    std::cout << "Testing Series" << std::endl;

    // Creating an integer series
    Series<int> s1({1, 2, 3, 4, 5});

    // Adding an element to the series
    s1.addElement(6);
    s1.print();

    // Creating a second series and adding to the first
    Series<int> s2({6, 7, 8, 9, 10, 11});
    Series<int> s3 = s1.addSeries(s2);
    s3.print();

    // Testing index access
    std::cout << "Element at position 3 of s1: " << s1[3] << std::endl;  // Expected: 4
}

// Function to test DataFrame with real data
void testDataFrameWithRealData() {
    std::cout << "\nTesting DataFrame with real data from orders.json" << std::endl;

    try {
        Extractor extractor;
        DataFrame<std::string> df = extractor.extractFromJson("../generator/orders.json");

        // Verify required columns exist
        auto columns = df.getColumns();
        std::vector<std::string> requiredColumns = {
            "flight_id", "seat", "user_id", "customer_name",
            "status", "payment_method", "reservation_time", "price"
        };

        for (const auto& col : requiredColumns) {
            try {
                // Test access to each column
                auto& test = df[col];
            } catch (const std::invalid_argument& e) {
                std::cerr << "Error: Missing required column '" << col << "' in input data" << std::endl;
                return;
            }
        }

        // Print the entire DataFrame
        std::cout << "\nFull DataFrame:" << std::endl;
        df.print();

        // Print first 5 rows (if available)
        std::cout << "\nFirst 5 rows:" << std::endl;
        auto shape = df.getShape();
        int num_rows = shape.first;
        int num_cols = shape.second;
        
        for (int i = 0; i < std::min(5, num_rows); ++i) {
            for (const auto& col : columns) {
                std::cout << std::setw(20) << df[col][i];
            }
            std::cout << std::endl;
        }

        // Show DataFrame shape
        std::cout << "\nDataFrame shape: " << num_rows << " rows, " << num_cols << " columns" << std::endl;

        // Show column names
        std::cout << "\nColumn names:" << std::endl;
        for (const auto& col : columns) {
            std::cout << col << std::endl;
        }

        // Example: Count reservations by status
        std::cout << "\nReservation status counts:" << std::endl;
        std::map<std::string, int> statusCounts;
        Series<std::string> statusSeries = df["status"];
        for (int i = 0; i < statusSeries.size(); ++i) {
            statusCounts[statusSeries[i]]++;
        }
        for (const auto& [status, count] : statusCounts) {
            std::cout << status << ": " << count << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in testDataFrameWithRealData: " << e.what() << std::endl;
    }
}

// Função para testar ValidationHandler
void testValidationHandler() {
    std::cout << "\nTesting ValidationHandler" << std::endl;
    
    ValidationHandler validator;
    
    // Caso de teste 1: Reserva válida
    Reservation validRes;
    validRes.flight_id = "FL123";
    validRes.seat = "A1";
    validRes.user_id = "user456";
    validRes.customer_name = "João Silva";
    validRes.status = "confirmed";
    validRes.payment_method = "credit_card";
    validRes.reservation_time = "2023-10-01T10:00:00";
    validRes.price = 299.99;
    
    validator.process(validRes);
    std::cout << "Valid reservation - Status: " << validRes.status 
              << " (should remain 'confirmed')" << std::endl;
    
    // Caso de teste 2: Valor inválido
    Reservation invalidpriceRes = validRes;
    invalidpriceRes.price = -100.0;
    validator.process(invalidpriceRes);
    std::cout << "Invalid price - Status: " << invalidpriceRes.status 
              << " (should be 'invalid_price')" << std::endl;
    
    // Caso de teste 3: Campos obrigatórios faltando
    Reservation missingFieldsRes;
    missingFieldsRes.price = 100.0;
    validator.process(missingFieldsRes);
    std::cout << "Missing fields - Status: " << missingFieldsRes.status 
              << " (should be unchanged, handler returns early)" << std::endl;
}

// Função para testar DateHandler
void testDateHandler() {
    std::cout << "\nTesting DateHandler" << std::endl;
    
    DateHandler dateProcessor;
    
    // Caso de teste 1: Data completa
    Reservation fullDateRes;
    fullDateRes.reservation_time = "2023-10-01T10:00:00";
    dateProcessor.process(fullDateRes);
    std::cout << "Full timestamp: " << fullDateRes.reservation_time 
              << " (should be '2023-10-01')" << std::endl;
    
    // Caso de teste 2: Data curta
    Reservation shortDateRes;
    shortDateRes.reservation_time = "2023-10-01";
    dateProcessor.process(shortDateRes);
    std::cout << "Short date: " << shortDateRes.reservation_time 
              << " (should remain unchanged)" << std::endl;
    
    // Caso de teste 3: String vazia
    Reservation emptyDateRes;
    emptyDateRes.reservation_time = "";
    dateProcessor.process(emptyDateRes);
    std::cout << "Empty date: " << emptyDateRes.reservation_time 
              << " (should remain empty)" << std::endl;
}

// Função para testar RevenueHandler
void testRevenueHandler() {
    std::cout << "\nTesting RevenueHandler" << std::endl;
    
    RevenueHandler revenueCalculator;
    
    // Caso de teste 1: Reserva confirmada
    Reservation confirmedRes;
    confirmedRes.status = "confirmed";
    confirmedRes.price = 100.0;
    revenueCalculator.process(confirmedRes);
    std::cout << "After confirmed reservation ($100): " 
              << revenueCalculator.getTotalRevenue() 
              << " (should be 100)" << std::endl;
    
    // Caso de teste 2: Reserva não confirmada
    Reservation unconfirmedRes;
    unconfirmedRes.status = "canceled";
    unconfirmedRes.price = 50.0;
    revenueCalculator.process(unconfirmedRes);
    std::cout << "After unconfirmed reservation ($50): " 
              << revenueCalculator.getTotalRevenue() 
              << " (should remain 100)" << std::endl;
    
    // Caso de teste 3: Reset
    revenueCalculator.resetRevenue();
    std::cout << "After reset: " << revenueCalculator.getTotalRevenue() 
              << " (should be 0)" << std::endl;
    
    // Caso de teste 4: Múltiplas threads
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&revenueCalculator]() {
            Reservation res;
            res.status = "confirmed";
            res.price = 10.0;
            revenueCalculator.process(res);
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    std::cout << "After 10 thread-safe additions ($10 each): " 
              << revenueCalculator.getTotalRevenue() 
              << " (should be 100)" << std::endl;
}

class Pipeline {
public:
    Pipeline() : threadWorld(4), loader("revenue.db") {}

    struct RunResult {
        long extraction_ms = 0;
        long processing_ms = 0;
        long loading_ms = 0;
        size_t threads_used = 0;
        long total_ms = 0;
    };

    RunResult run(bool dynamic_allocation) {
        RunResult timing;
        auto total_start = high_resolution_clock::now();
        
        // 1. Data Extraction
        auto extract_start = high_resolution_clock::now();
        Extractor extractor;
        DataFrame<std::string> df = extractor.extractFromJson("../generator/orders.json");
        timing.extraction_ms = duration_cast<milliseconds>(high_resolution_clock::now() - extract_start).count();

        // 2. Data Processing
        auto process_start = high_resolution_clock::now();
        
        // Add handlers to pipeline
        threadWorld.addHandler(std::make_unique<ValidationHandler>());
        threadWorld.addHandler(std::make_unique<DateHandler>());
        threadWorld.addHandler(std::make_unique<RevenueHandler>());
        
        // Configure and start processing
        threadWorld.setDynamicAllocation(dynamic_allocation);
        threadWorld.start();

        // Feed data into pipeline
        auto shape = df.getShape();
        for (int i = 0; i < shape.first; ++i) {
            Reservation res;
            res.flight_id = df["flight_id"][i];
            res.seat = df["seat"][i];
            res.user_id = df["user_id"][i];
            res.customer_name = df["customer_name"][i];
            res.status = df["status"][i];
            res.payment_method = df["payment_method"][i];
            res.reservation_time = df["reservation_time"][i];
            
            try {
                res.price = std::stod(df["price"][i]);
            } catch (...) {
                res.price = 0.0;
            }
            
            threadWorld.addDataToBuffer(0, res);
        }

        // Wait for completion
        while (!threadWorld.isProcessingComplete()) {
            std::this_thread::sleep_for(milliseconds(100));
        }
        timing.processing_ms = duration_cast<milliseconds>(high_resolution_clock::now() - process_start).count();

        // 3. Data Loading
        auto load_start = high_resolution_clock::now();
        loader.loadOrders(df);
        timing.loading_ms = duration_cast<milliseconds>(high_resolution_clock::now() - load_start).count();

        // Calculate totals
        timing.total_ms = duration_cast<milliseconds>(high_resolution_clock::now() - total_start).count();
        timing.threads_used = threadWorld.getThreadCount();

        return timing;
    }

private:
    ThreadWorld threadWorld;
    Loader loader;
};

int main() {
    try {
        // Run component tests
        testSeries();
        testDataFrameWithRealData();
        testValidationHandler();
        testDateHandler();
        testRevenueHandler();

        // Run pipeline performance tests
        const int runs = 3;
        Pipeline pipeline;
        
        std::vector<Pipeline::RunResult> staticResults;
        std::vector<Pipeline::RunResult> dynamicResults;
        
        std::cout << "\nTesting without dynamic thread allocation...\n";
        for (int i = 0; i < runs; ++i) {
            std::cout << "Run " << i+1 << "...";
            staticResults.push_back(pipeline.run(false));
            std::cout << " done\n";
        }
        
        std::cout << "\nTesting with dynamic thread allocation...\n";
        for (int i = 0; i < runs; ++i) {
            std::cout << "Run " << i+1 << "...";
            dynamicResults.push_back(pipeline.run(true));
            std::cout << " done\n";
        }
        
        // Print results table
        std::cout << "\n\nPERFORMANCE RESULTS (milliseconds)\n";
        std::cout << "+----------------------+-----------+-----------+-----------+-----------+----------+\n";
        std::cout << "| Configuration        | Extraction| Processing| Loading   | Total     | Threads  |\n";
        std::cout << "+----------------------+-----------+-----------+-----------+-----------+----------+\n";
        
        // Static allocation results
        for (size_t i = 0; i < staticResults.size(); ++i) {
            const auto& r = staticResults[i];
            printf("| Static Run %-10zu | %9ld | %9ld | %9ld | %9ld | %8zu |\n", 
                    i+1, r.extraction_ms, r.processing_ms, r.loading_ms, r.total_ms, r.threads_used);
        }
        
        // Dynamic allocation results
        for (size_t i = 0; i < dynamicResults.size(); ++i) {
            const auto& r = dynamicResults[i];
            printf("| Dynamic Run %-9zu | %9ld | %9ld | %9ld | %9ld | %8zu |\n", 
                    i+1, r.extraction_ms, r.processing_ms, r.loading_ms, r.total_ms, r.threads_used);
        }
        
        // Calculate averages
        auto calc_avg = [](const auto& results, auto member) {
            long sum = 0;
            for (const auto& r : results) sum += r.*member;
            return sum / results.size();
        };
        
        Pipeline::RunResult static_avg, dynamic_avg;
        static_avg.extraction_ms = calc_avg(staticResults, &Pipeline::RunResult::extraction_ms);
        static_avg.processing_ms = calc_avg(staticResults, &Pipeline::RunResult::processing_ms);
        static_avg.loading_ms = calc_avg(staticResults, &Pipeline::RunResult::loading_ms);
        static_avg.total_ms = calc_avg(staticResults, &Pipeline::RunResult::total_ms);
        
        dynamic_avg.extraction_ms = calc_avg(dynamicResults, &Pipeline::RunResult::extraction_ms);
        dynamic_avg.processing_ms = calc_avg(dynamicResults, &Pipeline::RunResult::processing_ms);
        dynamic_avg.loading_ms = calc_avg(dynamicResults, &Pipeline::RunResult::loading_ms);
        dynamic_avg.total_ms = calc_avg(dynamicResults, &Pipeline::RunResult::total_ms);
        
        // Print averages
        std::cout << "+----------------------+-----------+-----------+-----------+-----------+----------+\n";
        printf("| Static Avg           | %9ld | %9ld | %9ld | %9ld | %8s |\n",
                static_avg.extraction_ms, static_avg.processing_ms, 
                static_avg.loading_ms, static_avg.total_ms, "N/A");
        printf("| Dynamic Avg          | %9ld | %9ld | %9ld | %9ld | %8s |\n",
                dynamic_avg.extraction_ms, dynamic_avg.processing_ms,
                dynamic_avg.loading_ms, dynamic_avg.total_ms, "N/A");
        std::cout << "+----------------------+-----------+-----------+-----------+-----------+----------+\n";
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}