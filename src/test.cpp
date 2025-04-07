#include <iostream>
#include <chrono>
#include "dataframe.hpp"
#include "extractor.hpp"
#include "handler.hpp"
#include "database.h"
#include "loader.hpp"
#include "threadWorld.hpp"

void sequentialProcessing() {
    auto start = std::chrono::high_resolution_clock::now();
    
    try {
        DataBase db("earning.db");
        db.createTable("faturamento", "(reservation_time TEXT PRIMARY KEY, price REAL)");
        db.createTable("faturamentoMetodo", "(payment_method TEXT PRIMARY KEY, price REAL)");

        Extractor extractor;
        DataFrame<std::string> df = extractor.extractFromJson("../generator/orders.json");

        ValidationHandler validationHandler;
        DateHandler dateHandler;
        RevenueHandler revenueHandler;
        CardRevenueHandler dataRevenueHandler;

        // Process pipeline
        DataFrame<std::string> validData = validationHandler.process(df);
        DataFrame<std::string> datedData = dateHandler.process(validData);
        DataFrame<std::string> revenueData = revenueHandler.process(datedData);
        DataFrame<std::string> cardData = dataRevenueHandler.process(datedData);

        // Load results
        std::vector<std::string> faturamentoColumns = {"reservation_time", "price"};
        std::vector<std::string> faturamentoMetodoColumns = {"payment_method", "price"};

        Loader loader(db);
        loader.loadData("faturamento", revenueData, faturamentoColumns);
        loader.loadData("faturamentoMetodo", cardData, faturamentoMetodoColumns);

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Sequential processing time: " << duration.count() << "ms\n";
}

void threadedProcessing() {
    auto start = std::chrono::high_resolution_clock::now();
    
    try {
        // Initialize tables (main thread only)
        {
            DataBase db("earning.db");
            db.createTable("faturamento", "(reservation_time TEXT PRIMARY KEY, price REAL)");
            db.createTable("faturamentoMetodo", "(payment_method TEXT PRIMARY KEY, price REAL)");
        }

        // Extract data
        Extractor extractor;
        DataFrame<std::string> df = extractor.extractFromJson("../generator/orders.json");

        // Print columns for debug
        std::cout << "Available columns (" << df.numRows() << " rows): ";
        for (const auto& col : df.getColumns()) {
            std::cout << col << " ";
        }
        std::cout << "\n";

        // Create pipeline
        ThreadWorld pipeline(1, 4); // More conservative thread limits
        
        // Add stages with explicit column requirements
        pipeline.addStage(
            [](DataFrame<std::string>& df) {
                ValidationHandler handler;
                auto result = handler.process(df);
                std::cout << "Validation kept " << result.numRows() << " rows\n";
                return result;
            },
            {"status", "reservation_time", "price"}, // Only actually required columns
            2
        );
        
        pipeline.addStage(
            [](DataFrame<std::string>& df) {
                DateHandler handler;
                return handler.process(df);
            },
            {"reservation_time", "price"},
            2
        );

        // Start processing
        std::cout << "Starting pipeline...\n";
        pipeline.start(df);
        pipeline.waitForCompletion();

        // Get results
        auto results = pipeline.getFinalResults();
        
        // Load results (main thread only)
        DataBase db("earning.db");
        Loader loader(db);
        loader.loadData("faturamento", results.revenue, {"reservation_time", "price"});
        loader.loadData("faturamentoMetodo", results.card, {"payment_method", "price"});

    } catch (const std::exception& e) {
        std::cerr << "FATAL ERROR: " << e.what() << std::endl;
        return;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Threaded processing completed in " << duration.count() << "ms\n";
}

int main() {
    std::cout << "Running sequential processing...\n";
    sequentialProcessing();
    
    std::cout << "\nRunning threaded processing...\n";
    threadedProcessing();
    
    return 0;
}