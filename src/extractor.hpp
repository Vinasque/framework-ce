#pragma once
#include <string>
#include "json.hpp"
#include <fstream>
#include <iostream>
#include "dataframe.hpp"
#include "queue.hpp"
#include <sqlite3.h>
#include <mutex>

class Extractor {
private:
    std::unordered_map<std::string, size_t> file_positions;
    std::mutex position_mutex;

public:
    bool bDebugMode = true;

    // Modified to track position between extractions
    DataFrame<std::string> extractFromJson(const std::string& filePath, size_t num_lines = 0) {
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
                return DataFrame<std::string>(); // Return empty DataFrame if no new data
            }

            // If num_lines is 0, process all remaining lines
            size_t end_pos = (num_lines == 0) ? total_records : 
                            std::min(current_pos + num_lines, total_records);

            std::vector<std::string> flightIds, seats, userIds, customerNames;
            std::vector<std::string> statuses, paymentMethods, reservationTimes, prices;

            for (size_t i = current_pos; i < end_pos; ++i) {
                const auto& record = jsonData[i];
                flightIds.push_back(record["flight_id"].get<std::string>());
                seats.push_back(record["seat"].get<std::string>());
                userIds.push_back(record["user_id"].get<std::string>());
                customerNames.push_back(record["customer_name"].get<std::string>());
                statuses.push_back(record["status"].get<std::string>());
                paymentMethods.push_back(record["payment_method"].get<std::string>());
                reservationTimes.push_back(record["reservation_time"].get<std::string>());

                if (record.contains("price")) {
                    prices.push_back(std::to_string(record["price"].get<double>()));
                } else {
                    prices.push_back("0.0");
                }
            }

            current_pos = end_pos; // Update position for next extraction

            std::vector<std::string> columns = {
                "flight_id", "seat", "user_id", "customer_name", 
                "status", "payment_method", "reservation_time", "price"
            };

            std::vector<Series<std::string>> series = {
                Series<std::string>(flightIds),
                Series<std::string>(seats),
                Series<std::string>(userIds),
                Series<std::string>(customerNames),
                Series<std::string>(statuses),
                Series<std::string>(paymentMethods),
                Series<std::string>(reservationTimes),
                Series<std::string>(prices)
            };

            return DataFrame<std::string>(columns, series);
        } catch (const std::exception& e) {
            std::cerr << "Extraction error: " << e.what() << std::endl;
            throw;
        }
    }

    // Updated partitioned extraction
    void extractFromJsonPartitioned(const std::string& filePath, int numThreads, 
                                   Queue<int, DataFrame<std::string>>& partitionQueue,
                                   size_t num_lines = 0) {
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
                return; // No new data
            }

            size_t end_pos = (num_lines == 0) ? total_records : 
                           std::min(current_pos + num_lines, total_records);
            size_t chunk_size = (end_pos - current_pos) / numThreads;

            for (int i = 0; i < numThreads; ++i) {
                size_t start = current_pos + i * chunk_size;
                size_t end = (i == numThreads - 1) ? end_pos : start + chunk_size;

                std::vector<std::string> flightIds, seats, userIds, customerNames;
                std::vector<std::string> statuses, paymentMethods, reservationTimes, prices;

                for (size_t j = start; j < end; ++j) {
                    const auto& record = jsonData[j];
                    flightIds.push_back(record["flight_id"].get<std::string>());
                    seats.push_back(record["seat"].get<std::string>());
                    userIds.push_back(record["user_id"].get<std::string>());
                    customerNames.push_back(record["customer_name"].get<std::string>());
                    statuses.push_back(record["status"].get<std::string>());
                    paymentMethods.push_back(record["payment_method"].get<std::string>());
                    reservationTimes.push_back(record["reservation_time"].get<std::string>());

                    if (record.contains("price")) {
                        prices.push_back(std::to_string(record["price"].get<double>()));
                    } else {
                        prices.push_back("0.0");
                    }
                }

                std::vector<std::string> columns = {
                    "flight_id", "seat", "user_id", "customer_name",
                    "status", "payment_method", "reservation_time", "price"
                };

                std::vector<Series<std::string>> series = {
                    Series<std::string>(flightIds),
                    Series<std::string>(seats),
                    Series<std::string>(userIds),
                    Series<std::string>(customerNames),
                    Series<std::string>(statuses),
                    Series<std::string>(paymentMethods),
                    Series<std::string>(reservationTimes),
                    Series<std::string>(prices)
                };

                partitionQueue.enQueue({i, DataFrame<std::string>(columns, series)});
            }

            current_pos = end_pos;
        } catch (const std::exception& e) {
            std::cerr << "Extraction error: " << e.what() << std::endl;
            throw;
        }
    }


    // Método para extrair dados de um arquivo CSV
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

    // Método para extrair dados de um banco de dados SQLite
    DataFrame<std::string> extractFromSqlite(const std::string& dbPath, const std::string& query) {
        try {
            sqlite3* db;
            int rc = sqlite3_open(dbPath.c_str(), &db);

            if (rc) {
                throw std::runtime_error("Cannot open database: " + std::string(sqlite3_errmsg(db)));
            }

            sqlite3_stmt* stmt;
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

            sqlite3_finalize(stmt);
            sqlite3_close(db);

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
            std::cerr << "SQLite extraction error: " << e.what() << std::endl;
            throw;
        }
    }

    void extractFromJsonPartitioned(const std::string& filePath, int numThreads, Queue<int, DataFrame<std::string>>& partitionQueue) {
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

            int totalRecords = jsonData.size();
            int recordsPerPartition = totalRecords / numThreads;
            // std::cout << "Total: " << totalRecords << std::endl;

            // Lendo e particionando os dados diretamente
            for (int i = 0; i < numThreads; ++i) {
                int startIdx = i * recordsPerPartition;
                int endIdx = (i == numThreads - 1) ? totalRecords : (i + 1) * recordsPerPartition;

                std::vector<std::string> flightIds, seats, userIds, customerNames;
                std::vector<std::string> statuses, paymentMethods, reservationTimes, prices;

                for (int j = startIdx; j < endIdx; ++j) {
                    const auto& record = jsonData[j];
                    flightIds.push_back(record["flight_id"].get<std::string>());
                    seats.push_back(record["seat"].get<std::string>());
                    userIds.push_back(record["user_id"].get<std::string>());
                    customerNames.push_back(record["customer_name"].get<std::string>());
                    statuses.push_back(record["status"].get<std::string>());
                    paymentMethods.push_back(record["payment_method"].get<std::string>());
                    reservationTimes.push_back(record["reservation_time"].get<std::string>());

                    if (record.contains("price")) {
                        prices.push_back(std::to_string(record["price"].get<double>()));
                    } else {
                        prices.push_back("0.0");
                    }
                }

                std::vector<std::string> columns = {
                    "flight_id", "seat", "user_id", "customer_name",
                    "status", "payment_method", "reservation_time", "price"
                };

                std::vector<Series<std::string>> series = {
                    Series<std::string>(flightIds),
                    Series<std::string>(seats),
                    Series<std::string>(userIds),
                    Series<std::string>(customerNames),
                    Series<std::string>(statuses),
                    Series<std::string>(paymentMethods),
                    Series<std::string>(reservationTimes),
                    Series<std::string>(prices)
                };

                // Criando o DataFrame para a partição
                DataFrame<std::string> df(columns, series);

                // std::cout << df.numRows() << std::endl;

                // Enfileirando o DataFrame particionado
                partitionQueue.enQueue({i, df});
            }

        } catch (const std::exception& e) {
            std::cerr << "Extraction error: " << e.what() << std::endl;
            throw;
        }
    }
};