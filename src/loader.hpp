#include <sqlite3.h>
#include <string>
#include <iostream>
#include <stdexcept>
#include "dataframe.hpp"
#include "database.h"

class Loader {
private:
    DataBase& database;

    // Helper function to execute a query and get results
    std::vector<std::vector<std::string>> query(const std::string& sql, const std::vector<std::string>& params = {}) {
        std::vector<std::vector<std::string>> results;
        sqlite3_stmt* stmt;
        
        if (sqlite3_prepare_v2(database.db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(database.db) << std::endl;
            return results;
        }

        // Bind parameters
        for (size_t i = 0; i < params.size(); ++i) {
            sqlite3_bind_text(stmt, i+1, params[i].c_str(), -1, SQLITE_TRANSIENT);
        }

        // Execute query and process results
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            std::vector<std::string> row;
            int col_count = sqlite3_column_count(stmt);
            for (int i = 0; i < col_count; ++i) {
                const char* val = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
                row.push_back(val ? val : "");
            }
            results.push_back(row);
        }

        sqlite3_finalize(stmt);
        return results;
    }

public:
    Loader(DataBase& db) : database(db) {}

    void loadData(const std::string& table_name, 
                 const DataFrame<std::string>& df, 
                 const std::vector<std::string>& columns, 
                 bool bGroupBy) {
        
        // Determine the table type for special handling
        bool isRevenueTable = (table_name.find("faturamento") != std::string::npos);
        bool isPaymentMethodTable = (table_name.find("faturamentoMetodo") != std::string::npos);

        if (isRevenueTable || isPaymentMethodTable) {
            // For revenue tables, we need to accumulate values
            std::string key_column = columns[0];
            std::string value_column = columns[1];

            for (int i = 0; i < df.numRows(); ++i) {
                try {
                    std::string key_value = df.getValue(key_column, i);
                    std::string str_value = df.getValue(value_column, i);
                    double new_value = std::stod(str_value);

                    // Check if record exists using prepared statement
                    std::string select_sql = "SELECT " + value_column + " FROM " + table_name + 
                                           " WHERE " + key_column + " = ?";
                    auto result = query(select_sql, {key_value});

                    if (!result.empty()) {
                        // Record exists - update it by adding the new value
                        double current_value = std::stod(result[0][0]);
                        std::string update_sql = "UPDATE " + table_name + 
                                               " SET " + value_column + " = ?" +
                                               " WHERE " + key_column + " = ?";
                        database.execute(update_sql, {std::to_string(current_value + new_value), key_value});
                    } else {
                        // Record doesn't exist - insert new
                        std::vector<std::string> values = {key_value, str_value};
                        database.insertValuesintoTable(table_name, columns, values, bGroupBy);
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Error processing row " << i << ": " << e.what() << std::endl;
                    continue;
                }
            }
        } else {
            // For other tables, use standard insert
            for (int i = 0; i < df.numRows(); ++i) {
                try {
                    std::vector<std::string> values;
                    for (const auto& column : columns) {
                        values.push_back(df.getValue(column, i));
                    }
                    database.insertValuesintoTable(table_name, columns, values, bGroupBy);
                } catch (const std::exception& e) {
                    std::cerr << "Error inserting row " << i << ": " << e.what() << std::endl;
                    continue;
                }
            }
        }
    }
};