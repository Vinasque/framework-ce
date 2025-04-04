#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <iomanip>
#include "series.hpp"
#include "dataframe.hpp"
#include "extractor.hpp"

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
void testDataFrameWithRealData() {
    std::cout << "\nTesting DataFrame with real data from orders.json" << std::endl;

    try {
        // Create extractor and load data
        Extractor extractor;
        DataFrame<std::string> df = extractor.extractFromJson("../generator/orders.json");

        // Print the entire DataFrame
        std::cout << "\nFull DataFrame:" << std::endl;
        df.print();

        // Print first 5 rows (if available)
        std::cout << "\nFirst 5 rows:" << std::endl;
        auto shape = df.getShape();
        int num_rows = shape.first;
        int num_cols = shape.second;
        auto columns = df.getColumns();
        
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
int main() {
    // Run tests
    testSeries();
    testDataFrameWithRealData();

    return 0;
}