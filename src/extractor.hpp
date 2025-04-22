#pragma once
#include <string>
#include "json.hpp"
#include <fstream>
#include <iostream>
#include "dataframe.hpp"
#include "queue.hpp"
#include <sqlite3.h>
#include <mutex>
#include <random>

class Extractor {
private:
    std::unordered_map<std::string, size_t> file_positions;
    std::mutex position_mutex;
    std::random_device rd;
    std::mt19937 gen;

public:
    bool bDebugMode = true;
    
    Extractor() : gen(rd()) {}

    // Reset file position for reprocessing
    void resetFilePosition(const std::string& filePath) {
        std::lock_guard<std::mutex> lock(position_mutex);
        file_positions[filePath] = 0;
    }

    // We're going to use to have no problems when dealing with the json
    std::string toString(const nlohmann::json& jsonValue) {
        if (jsonValue.is_string()) {
            return jsonValue.get<std::string>(); 
        } else if (jsonValue.is_number()) {
            return std::to_string(jsonValue.get<double>()); // For numbers 
        } else if (jsonValue.is_boolean()) {
            return jsonValue.get<bool>() ? "true" : "false"; // For booleans
        } else {
            return ""; 
        }
    }

    // Extract an entire chunk
    DataFrame<std::string> extractChunk(const std::string& filePath, const std::vector<std::string>& columns, size_t chunk_size = 0) {
        std::lock_guard<std::mutex> lock(position_mutex);
        size_t& current_pos = file_positions[filePath];

        try {
            std::ifstream file(filePath);
            if (!file.is_open()) {
                throw std::runtime_error("Could not open the file: " + filePath);
            }

            nlohmann::json jsonData;
            file >> jsonData;

            if (!jsonData.is_array()) {
                throw std::runtime_error("JSON data should be an array of records");
            }

            size_t total_records = jsonData.size();
            if (current_pos >= total_records) {
                return DataFrame<std::string>(); // Return empty if no more data
            }

            // Process either specified chunk or remaining lines
            size_t end_pos = (chunk_size == 0) ? total_records : std::min(current_pos + chunk_size, total_records);

            std::unordered_map<std::string, std::vector<std::string>> extracted_data;
            for (const auto& col : columns) {
                extracted_data[col] = std::vector<std::string>();  // Initialize empty vectors for each column
            }

            for (size_t i = current_pos; i < end_pos; ++i) {
                const auto& record = jsonData[i];

                for (const auto& col : columns) {
                    // Assure that the value will be treated as a string
                    if (record.contains(col)) {
                        extracted_data[col].push_back(toString(record[col]));
                    } else {
                        extracted_data[col].push_back("");  // Handle missing values
                    }
                }
            }

            current_pos = end_pos; // Update position

            // Create series for each column
            std::vector<Series<std::string>> series;
            for (const auto& col : columns) {
                series.push_back(Series<std::string>(extracted_data[col]));
            }

            return DataFrame<std::string>(columns, series);
        } catch (const std::exception& e) {
            std::cerr << "Extraction error: " << e.what() << std::endl;
            throw;
        }
    }

    void extractPartitionedChunk(const std::string& filePath, int numThreads, 
        Queue<int, DataFrame<std::string>>& partitionQueue,
        const std::vector<std::string>& columns, size_t chunk_size = 0) {
        std::lock_guard<std::mutex> lock(position_mutex);
        size_t& current_pos = file_positions[filePath];

        try {
            std::ifstream file(filePath);
            if (!file.is_open()) {
                throw std::runtime_error("Could not open the file: " + filePath);
            }

            nlohmann::json jsonData;
            file >> jsonData;

            if (!jsonData.is_array()) {
                throw std::runtime_error("JSON data should be an array of records");
            }

            size_t total_records = jsonData.size();
            if (current_pos >= total_records) {
                return; // No more data
            }

            size_t end_pos = (chunk_size == 0) ? total_records : std::min(current_pos + chunk_size, total_records);
            size_t records_remaining = end_pos - current_pos;
            size_t per_thread = records_remaining / numThreads;

            for (int i = 0; i < numThreads; ++i) {
                size_t thread_start = current_pos + (i * per_thread);
                size_t thread_end = (i == numThreads - 1) ? end_pos : thread_start + per_thread;

                std::unordered_map<std::string, std::vector<std::string>> extracted_data;
                for (const auto& col : columns) {
                    extracted_data[col] = std::vector<std::string>();  // Initialize empty vectors for each column
                }

                // Iterate over the chunk assigned to the current thread
                for (size_t j = thread_start; j < thread_end; ++j) {
                    const auto& record = jsonData[j];

                    for (const auto& col : columns) {
                        if (record.contains(col)) {
                            extracted_data[col].push_back(toString(record[col]));  // Using toString to handle any type safely
                        } else {
                            extracted_data[col].push_back("");  // Handle missing values as empty strings
                        }
                    }
                }

                // Create the DataFrame for the partition
                std::vector<Series<std::string>> series;
                for (const auto& col : columns) {
                    series.push_back(Series<std::string>(extracted_data[col]));
                }

                // Enqueue the partitioned DataFrame
                partitionQueue.enQueue({i, DataFrame<std::string>(columns, series)});
            }

            current_pos = end_pos; // Update position
        } catch (const std::exception& e) {
            std::cerr << "Extraction error: " << e.what() << std::endl;
            throw;
        }
    }


    // Random chunk generator for TimerTrigger
    DataFrame<std::string> extractRandomChunk(const std::string& filePath, const std::vector<std::string>& columns, size_t min_lines = 100, size_t max_lines = 1000) {
        std::uniform_int_distribution<size_t> dist(min_lines, max_lines);
        return extractChunk(filePath, columns, dist(gen));
    }

    // Extract from a TXT file (ig its fine)
    DataFrame<std::string> extractFromTxt(const std::string& filePath, const std::vector<std::string>& columns, char delimiter = '\t') {
        try {
            std::ifstream file(filePath);
            if (!file.is_open()) {
                throw std::runtime_error("Could not open the file: " + filePath);
            }

            std::string line;
            std::vector<std::vector<std::string>> data;

            // Read every line in the file
            while (std::getline(file, line)) {
                std::vector<std::string> row;
                std::stringstream ss(line);
                std::string cell;

                // Will use the delimiter char to be able to distinguish things
                while (std::getline(ss, cell, delimiter)) {
                    row.push_back(cell);
                }

                data.push_back(row);  // Add the line
            }

            file.close();

            // We create the series we need for the DF
            std::vector<Series<std::string>> series;
            for (size_t i = 0; i < columns.size(); ++i) {
                std::vector<std::string> columnData;
                for (const auto& row : data) {
                    if (i < row.size()) {
                        columnData.push_back(row[i]);
                    } else {
                        columnData.push_back("");
                    }
                }
                series.push_back(Series<std::string>(columnData));
            }

            // Add the columns together
            return DataFrame<std::string>(columns, series);

        } catch (const std::exception& e) {
            std::cerr << "TXT extraction error: " << e.what() << std::endl;
            throw;
        }
    }

    // Extract from a CSV file (the same as before but easier actually)
    DataFrame<std::string> extractFromCsv(const std::string& filePath) {
        try {
            std::ifstream file(filePath);
            if (!file.is_open()) {
                throw std::runtime_error("Could not open the file: " + filePath);
            }

            std::string line;
            std::vector<std::string> columns;
            std::vector<std::vector<std::string>> data;

            // Read columns (first line)
            std::getline(file, line);
            std::stringstream ss(line);
            std::string column;
            while (std::getline(ss, column, ',')) {
                columns.push_back(column);
            }

            // Read rows
            while (std::getline(file, line)) {
                std::stringstream rowStream(line);
                std::vector<std::string> row;
                while (std::getline(rowStream, column, ',')) {
                    row.push_back(column);
                }
                data.push_back(row);
            }

            // Prepare DataFrame
            std::vector<Series<std::string>> series;
            for (size_t i = 0; i < columns.size(); ++i) {
                std::vector<std::string> columnData;
                for (const auto& row : data) {
                    columnData.push_back(row[i]);
                }
                series.push_back(Series<std::string>(columnData));
            }

            return DataFrame<std::string>(columns, series);
        } catch (const std::exception& e) {
            std::cerr << "CSV extraction error: " << e.what() << std::endl;
            throw;
        }
    } 

    // Extract from a SQLite database
    DataFrame<std::string> extractFromSqlite(const std::string& dbPath, const std::string& tableName) {
        try {
            sqlite3* db;
            int rc = sqlite3_open(dbPath.c_str(), &db);

            if (rc) {
                throw std::runtime_error("Cannot open database: " + std::string(sqlite3_errmsg(db)));
            }

            // Get the query with the items from the table we want
            sqlite3_stmt* stmt;
            std::string query = ("SELECT * FROM " + tableName);
            rc = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, 0);
            if (rc != SQLITE_OK) {
                throw std::runtime_error("Failed to execute query: " + std::string(sqlite3_errmsg(db)));
            }

            std::vector<std::string> columns;
            std::vector<std::vector<std::string>> data;

            // Get column names
            int columnCount = sqlite3_column_count(stmt);
            for (int i = 0; i < columnCount; ++i) {
                columns.push_back(sqlite3_column_name(stmt, i));
            }

            // Get rows of data
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                std::vector<std::string> row;
                for (int i = 0; i < columnCount; ++i) {
                    row.push_back(reinterpret_cast<const char*>(sqlite3_column_text(stmt, i)));
                }
                data.push_back(row);
            }

            char* errMsg;
            query = ("DELETE FROM " + tableName + ";");
            rc = sqlite3_exec(db, query.c_str(), nullptr, nullptr, &errMsg);

            sqlite3_finalize(stmt);
            sqlite3_close(db);

            // Prepare tne DataFrame
            std::vector<Series<std::string>> series;
            for (size_t i = 0; i < columns.size(); ++i) {
                std::vector<std::string> columnData;
                for (const auto& row : data) {
                    columnData.push_back(row[i]);
                }
                series.push_back(Series<std::string>(columnData));
            }

            DataFrame<std::string> resultDf = DataFrame<std::string>(columns, series);

            return resultDf;
        } catch (const std::exception& e) {
            std::cerr << "SQLite extraction error: " << e.what() << std::endl;
            throw;
        }
    }

    void extractFromJsonPartitioned(const std::string& filePath, int numThreads, 
        Queue<int, DataFrame<std::string>>& partitionQueue, const std::vector<std::string>& columns) {
    
        try {
            std::ifstream file(filePath);
            if (!file.is_open()) {
                throw std::runtime_error("Could not open the file: " + filePath);
            }
    
            nlohmann::json jsonData;
            file >> jsonData;
    
            if (!jsonData.is_array()) {
                throw std::runtime_error("JSON data should be an array of records");
            }
    
            size_t totalRecords = jsonData.size();
            size_t recordsPerPartition = totalRecords / numThreads;
    
            for (int i = 0; i < numThreads; ++i) {
                size_t startIdx = i * recordsPerPartition;
                size_t endIdx = (i == numThreads - 1) ? totalRecords : (i + 1) * recordsPerPartition;
    
                // Map to store column data
                std::unordered_map<std::string, std::vector<std::string>> extracted_data;
    
                // Initialize vectors for each column
                for (const auto& col : columns) {
                    extracted_data[col] = std::vector<std::string>();
                }
    
                // Process the chunk of records assigned to this thread
                for (size_t j = startIdx; j < endIdx; ++j) {
                    const auto& record = jsonData[j];
    
                    // For each column in the DataFrame, convert the JSON value to a string
                    for (const auto& col : columns) {
                        if (record.contains(col)) {
                            extracted_data[col].push_back(toString(record[col]));  // Convert to string
                        } else {
                            extracted_data[col].push_back("");  // If column is missing, use an empty string
                        }
                    }
                }
    
                // Create the DataFrame for the partition
                std::vector<Series<std::string>> series;
                for (const auto& col : columns) {
                    series.push_back(Series<std::string>(extracted_data[col]));
                }
    
                // Enqueue the partitioned DataFrame
                partitionQueue.enQueue({i, DataFrame<std::string>(columns, series)});
            }
    
        } catch (const std::exception& e) {
            std::cerr << "Extraction error: " << e.what() << std::endl;
            throw;
        }
    }
};