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
            "status", "payment_method", "reservation_time", "amount"
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

class Pipeline {
public:
    Pipeline() : threadWorld(12) {} // Start with 4 threads

    void run(bool dynamic_allocation) {
        try {
            auto start = high_resolution_clock::now();
            
            Extractor extractor;
            DataFrame<std::string> df = extractor.extractFromJson("../generator/orders.json");

            // Verify we have the amount column
            try {
                auto& amountCol = df["amount"];
            } catch (const std::invalid_argument&) {
                std::cerr << "Error: Input data is missing the 'amount' column" << std::endl;
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
                    res.amount = std::stod(df["amount"][i]);
                } catch (...) {
                    std::cerr << "Warning: Invalid amount format for reservation " << i 
                              << ", defaulting to 0.0" << std::endl;
                    res.amount = 0.0;
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