#include <sqlite3.h>
#include <string>
#include <iostream>
#include "dataframe.hpp"
#include "database.h"

class Loader {
private:
    DataBase& database;

public:
    Loader(DataBase& db) : database(db) {}

    void loadData(const std::string& table_name, const DataFrame<std::string>& df, 
                    const std::vector<std::string>& columns) {
        // For each row in the DataFrame
        for (int i = 0; i < df.numRows(); ++i) {
            std::vector<std::string> values;
            for (const auto& column : columns) {
                values.push_back(df.getValue(column, i));
            }

            // Build the upsert query
            std::string sql;
            if (table_name == "faturamento") {
                sql = "INSERT INTO " + table_name + " (reservation_time, price) " +
                        "VALUES ('" + values[0] + "', " + values[1] + ") " +
                        "ON CONFLICT(reservation_time) DO UPDATE SET " +
                        "price = price + excluded.price;";
            } 
            else if (table_name == "faturamentoMetodo") {
                sql = "INSERT INTO " + table_name + " (payment_method, price) " +
                        "VALUES ('" + values[0] + "', " + values[1] + ") " +
                        "ON CONFLICT(payment_method) DO UPDATE SET " +
                        "price = price + excluded.price;";
            }

            // Execute the query
            database.executeSQL(sql);
        }
    }
};

// class Loader {
// public:

//     void loadOrders(const DataFrame<std::string>& df) {
//         if (!db) return;
    
//         int rows = df.numRows();
//         for (int i = 0; i < rows; ++i) {
//             std::string sql = "INSERT INTO orders (flight_id, seat, user_id, customer_name, status, payment_method, reservation_time, price) "
//                               "VALUES (?, ?, ?, ?, ?, ?, ?, ?);";
    
//             sqlite3_stmt* stmt;
//             if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
//                 sqlite3_bind_text(stmt, 1, df.getValue("flight_id", i).c_str(), -1, SQLITE_STATIC);
//                 sqlite3_bind_text(stmt, 2, df.getValue("seat", i).c_str(), -1, SQLITE_STATIC);
//                 sqlite3_bind_text(stmt, 3, df.getValue("user_id", i).c_str(), -1, SQLITE_STATIC);
//                 sqlite3_bind_text(stmt, 4, df.getValue("customer_name", i).c_str(), -1, SQLITE_STATIC);
//                 sqlite3_bind_text(stmt, 5, df.getValue("status", i).c_str(), -1, SQLITE_STATIC);
//                 sqlite3_bind_text(stmt, 6, df.getValue("payment_method", i).c_str(), -1, SQLITE_STATIC);
//                 sqlite3_bind_text(stmt, 7, df.getValue("reservation_time", i).c_str(), -1, SQLITE_STATIC);
//                 sqlite3_bind_double(stmt, 8, std::stod(df.getValue("price", i)));
    
//                 if (sqlite3_step(stmt) != SQLITE_DONE) {
//                     std::cerr << "Falha ao inserir ordem: " << sqlite3_errmsg(db) << std::endl;
//                 }
    
//                 sqlite3_finalize(stmt);
//             } else {
//                 std::cerr << "Falha ao preparar statement: " << sqlite3_errmsg(db) << std::endl;
//             }
//         }
//     }    

//     double getRevenueByDate(const std::string& date) {
//         if (!db) return 0.0;
    
//         std::string sql = "SELECT SUM(price) FROM orders WHERE substr(reservation_time, 1, 10) = ?";
//         sqlite3_stmt* stmt;
//         double totalRevenue = 0.0;
    
//         if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
//             sqlite3_bind_text(stmt, 1, date.c_str(), -1, SQLITE_STATIC);
    
//             if (sqlite3_step(stmt) == SQLITE_ROW) {
//                 totalRevenue = sqlite3_column_double(stmt, 0);
//             } else {
//                 std::cerr << "Falha ao executar consulta de receita." << std::endl;
//             }
    
//             sqlite3_finalize(stmt);
//         } else {
//             std::cerr << "Erro ao preparar consulta: " << sqlite3_errmsg(db) << std::endl;
//         }
    
//         return totalRevenue;
//     }  

// private:
//     sqlite3* db;

//     bool entryExists(const std::string& date) {
//         std::string sql = "SELECT COUNT(*) FROM revenue WHERE date = ?";
//         sqlite3_stmt* stmt;
//         bool exists = false;

//         if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
//             sqlite3_bind_text(stmt, 1, date.c_str(), -1, SQLITE_STATIC);
//             if (sqlite3_step(stmt) == SQLITE_ROW) {
//                 int count = sqlite3_column_int(stmt, 0);
//                 exists = (count > 0);
//             }
//         } else {
//             std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
//         }

//         sqlite3_finalize(stmt);
//         return exists;
//     }

//     void insert(const std::string& date, int revenue) {
//         std::string sql = "INSERT INTO revenue (date, revenue) VALUES (?, ?)";
//         sqlite3_stmt* stmt;

//         if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
//             sqlite3_bind_text(stmt, 1, date.c_str(), -1, SQLITE_STATIC);
//             sqlite3_bind_int(stmt, 2, revenue);

//             if (sqlite3_step(stmt) != SQLITE_DONE) {
//                 std::cerr << "Insert failed: " << sqlite3_errmsg(db) << std::endl;
//             }
//         } else {
//             std::cerr << "Failed to prepare insert statement: " << sqlite3_errmsg(db) << std::endl;
//         }

//         sqlite3_finalize(stmt);
//     }

//     void update(const std::string& date, int revenue) {
//         std::string sql = "UPDATE revenue SET revenue = ? WHERE date = ?";
//         sqlite3_stmt* stmt;

//         if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
//             sqlite3_bind_int(stmt, 1, revenue);
//             sqlite3_bind_text(stmt, 2, date.c_str(), -1, SQLITE_STATIC);

//             if (sqlite3_step(stmt) != SQLITE_DONE) {
//                 std::cerr << "Update failed: " << sqlite3_errmsg(db) << std::endl;
//             }
//         } else {
//             std::cerr << "Failed to prepare update statement: " << sqlite3_errmsg(db) << std::endl;
//         }

//         sqlite3_finalize(stmt);
//     }  
// };

// #endif