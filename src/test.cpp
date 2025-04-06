#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include "series.hpp"
#include "dataframe.hpp"
#include "extractor.hpp"
#include "handler.hpp"
#include "threadWorld.hpp"

using namespace std;
using namespace std::chrono;

void printDataFrameHead(DataFrame<string>& df, int rows = 5) {
    auto columns = df.getColumns();
    auto shape = df.getShape();
    
    cout << "\nDataFrame Preview (" << min(rows, shape.first) << " of " << shape.first << " rows):\n";
    cout << "+-----+";
    for (const auto& col : columns) {
        cout << setw(20) << setfill('-') << "+";
    }
    cout << endl;
    
    // Print header
    cout << "|     |";
    for (const auto& col : columns) {
        cout << setw(20) << setfill(' ') << left << col.substr(0, 19) << "|";
    }
    cout << endl;
    
    // Print rows
    for (int i = 0; i < min(rows, shape.first); ++i) {
        cout << "| " << setw(3) << right << i << " |";
        for (const auto& col : columns) {
            cout << setw(20) << left << df[col][i].substr(0, 19) << "|";
        }
        cout << endl;
    }
    
    cout << "+-----+";
    for (size_t i = 0; i < columns.size(); ++i) {
        cout << setw(20) << setfill('-') << "+";
    }
    cout << "\n" << endl;
}

class PipelineTester {
public:
    struct TestResult {
        string config;
        long extraction_ms;
        long processing_ms;
        long total_ms;
        vector<size_t> thread_counts;
        size_t data_size;
    };

    void runComparisonTests() {
        vector<TestResult> results;
        const int runs = 3;
        const size_t dataSize = 1000;

        // Static allocation tests
        for (int i = 0; i < runs; ++i) {
            ThreadWorld world(2, ThreadWorld::AllocationMode::Static);
            results.push_back(runTest(world, "Static " + to_string(i+1), dataSize));
        }

        // Dynamic allocation tests
        for (int i = 0; i < runs; ++i) {
            ThreadWorld world(1, ThreadWorld::AllocationMode::Dynamic);
            results.push_back(runTest(world, "Dynamic " + to_string(i+1), dataSize));
        }

        printResults(results);
    }

private:
    TestResult runTest(ThreadWorld& world, const string& config, size_t dataSize) {
        TestResult result;
        result.config = config;
        auto total_start = high_resolution_clock::now();

        try {
            // 1. Data Extraction
            auto extract_start = high_resolution_clock::now();
            auto df = generateTestData(dataSize);
            result.extraction_ms = duration_cast<milliseconds>(high_resolution_clock::now() - extract_start).count();
            
            // Print DataFrame info
            result.data_size = df.getShape().first;
            cout << "\nExtracted DataFrame with " << result.data_size << " rows\n";
            printDataFrameHead(df);

            // 2. Processing
            auto process_start = high_resolution_clock::now();
            setupHandlers(world);
            feedData(world, df);
            waitForCompletion(world);
            result.processing_ms = duration_cast<milliseconds>(high_resolution_clock::now() - process_start).count();
            result.thread_counts = world.getThreadCounts();
        } catch (const exception& e) {
            cerr << "Error in " << config << ": " << e.what() << endl;
            world.stop();
            throw;
        }

        result.total_ms = duration_cast<milliseconds>(high_resolution_clock::now() - total_start).count();
        return result;
    }

    DataFrame<string> generateTestData(size_t size) {
        Extractor extractor;
        return extractor.extractFromJson("../generator/orders.json");
    }

    void setupHandlers(ThreadWorld& world) {
        world.addHandler(0, make_unique<ValidationHandler>());
        world.addHandler(1, make_unique<DateHandler>());
        world.addHandler(2, make_unique<RevenueHandler>());
    }

    void feedData(ThreadWorld& world, DataFrame<string>& df) {
        // Get column references first
        Series<string>& flight_id = df["flight_id"];
        Series<string>& seat = df["seat"];
        Series<string>& user_id = df["user_id"];
        Series<string>& customer_name = df["customer_name"];
        Series<string>& status = df["status"];
        Series<string>& payment_method = df["payment_method"];
        Series<string>& reservation_time = df["reservation_time"];
        Series<string>& price = df["price"];

        for (size_t i = 0; i < df.getShape().first; ++i) {
            Reservation res;
            res.flight_id = flight_id[i];
            res.seat = seat[i];
            res.user_id = user_id[i];
            res.customer_name = customer_name[i];
            res.status = status[i];
            res.payment_method = payment_method[i];
            res.reservation_time = reservation_time[i];
            
            try {
                res.price = stod(price[i]);
            } catch (...) {
                res.price = 0.0;
            }
            
            world.addData(res);
        }
    }

    void waitForCompletion(ThreadWorld& world) {
        auto start = high_resolution_clock::now();
        size_t check_count = 0;
        
        while (!world.isProcessingComplete()) {
            auto elapsed = duration_cast<seconds>(high_resolution_clock::now() - start).count();
            
            if (elapsed > 30) {
                cerr << "Warning: Processing timeout after " << elapsed << " seconds" << endl;
                world.stop();
                throw runtime_error("Processing timeout after " + to_string(elapsed) + " seconds");
            }
            
            if (++check_count % 10 == 0) {
                cout << "Waiting for completion... (" << elapsed << "s elapsed)" << endl;
            }
            
            this_thread::sleep_for(100ms);
        }
        
        auto end = high_resolution_clock::now();
        cout << "Processing completed in " 
             << duration_cast<milliseconds>(end - start).count() 
             << " ms" << endl;
    }

    void printResults(const vector<TestResult>& results) {
        cout << "\nPERFORMANCE COMPARISON\n";
        cout << "+------------+-----------+-----------+-----------+------------------+---------+\n";
        cout << "| Config     | Extract   | Process   | Total     | Threads (V/D/F)  | Rows    |\n";
        cout << "+------------+-----------+-----------+-----------+------------------+---------+\n";

        for (const auto& res : results) {
            string threads_str;
            for (auto count : res.thread_counts) {
                threads_str += to_string(count) + "/";
            }
            threads_str.pop_back();

            printf("| %-10s | %9ld | %9ld | %9ld | %-16s | %7zu |\n",
                  res.config.c_str(),
                  res.extraction_ms,
                  res.processing_ms,
                  res.total_ms,
                  threads_str.c_str(),
                  res.data_size);
        }

        cout << "+------------+-----------+-----------+-----------+------------------+---------+\n";
    }
};

int main() {
    try {
        PipelineTester tester;
        tester.runComparisonTests();
    } catch (const exception& e) {
        cerr << "Error: " << e.what() << endl;
        return 1;
    }
    return 0;
}