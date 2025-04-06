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
    Pipeline() : threadWorld(12) {} // Start with 4 threads

    void run(bool dynamic_allocation) {
        try {
            auto start = high_resolution_clock::now();
            
            Extractor extractor;
            DataFrame<std::string> df = extractor.extractFromJson("../generator/orders.json");

            // Verify we have the price column
            try {
                auto& priceCol = df["price"];
            } catch (const std::invalid_argument&) {
                std::cerr << "Error: Input data is missing the 'price' column" << std::endl;
                return;
            }

            // Create handlers
            ValidationHandler validationHandler;
            DateHandler dateHandler;
            RevenueHandler revenueHandler;
            
            // Configure dynamic allocation
            threadWorld.setDynamicAllocation(dynamic_allocation);
            threadWorld.start();
            
            // Process each reservation
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
                    std::cerr << "Warning: Invalid price format for reservation " << i 
                              << ", defaulting to 0.0" << std::endl;
                    res.price = 0.0;
                }
                
                threadWorld.addDataToBuffer(0, res);
            }
            
            // Wait for completion
            while (!threadWorld.isProcessingComplete()) {
                std::this_thread::sleep_for(milliseconds(100));
            }
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<milliseconds>(end - start);
            
            std::cout << "Processing time (" 
                      << (dynamic_allocation ? "with" : "without") 
                      << " dynamic allocation): " 
                      << duration.count() << "ms" << std::endl;
            
        } catch (const std::exception& e) {
            std::cerr << "Pipeline error: " << e.what() << std::endl;
        }
    }

private:
    ThreadWorld threadWorld;
};

int main() {
    try {
        // Run tests
        testSeries();
        testDataFrameWithRealData();
        
        // New handler tests
        testValidationHandler();
        testDateHandler();
        testRevenueHandler();
        
        // Run pipeline
        Pipeline pipeline;
        
        std::cout << "\nRunning without dynamic thread allocation..." << std::endl;
        pipeline.run(false);
        
        std::cout << "\nRunning with dynamic thread allocation..." << std::endl;
        pipeline.run(true);
        
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}