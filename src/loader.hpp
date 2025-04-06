#ifndef LOADER_HPP
#define LOADER_HPP

#include <sqlite3.h>
#include <string>
#include <iostream>
#include "dataframe.hpp"

class Loader {
public:
    Loader(const std::string& db_name) {
        if (sqlite3_open(db_name.c_str(), &db)) {
            std::cerr << "Error opening database: " << sqlite3_errmsg(db) << std::endl;
            db = nullptr;
        }
    }

    ~Loader() {
        if (db) sqlite3_close(db);
    }

    void loadDataFrame(const DataFrame<std::string>& df) {
        if (!db) return;

        int rows = df.numRows();
        for (int i = 0; i < rows; ++i) {
            std::string date = df.getValue("date", i);
            int revenue = std::stoi(df.getValue("revenue", i));

            if (entryExists(date)) {
                update(date, revenue);
            } else {
                insert(date, revenue);
            }
        }
    }

private:
    sqlite3* db;

    bool entryExists(const std::string& date) {
        std::string sql = "SELECT COUNT(*) FROM revenue WHERE date = ?";
        sqlite3_stmt* stmt;
        bool exists = false;

        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, date.c_str(), -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                int count = sqlite3_column_int(stmt, 0);
                exists = (count > 0);
            }
        } else {
            std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
        }

        sqlite3_finalize(stmt);
        return exists;
    }

    void insert(const std::string& date, int revenue) {
        std::string sql = "INSERT INTO revenue (date, revenue) VALUES (?, ?)";
        sqlite3_stmt* stmt;

        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, date.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_int(stmt, 2, revenue);

            if (sqlite3_step(stmt) != SQLITE_DONE) {
                std::cerr << "Insert failed: " << sqlite3_errmsg(db) << std::endl;
            }
        } else {
            std::cerr << "Failed to prepare insert statement: " << sqlite3_errmsg(db) << std::endl;
        }

        sqlite3_finalize(stmt);
    }

    void update(const std::string& date, int revenue) {
        std::string sql = "UPDATE revenue SET revenue = ? WHERE date = ?";
        sqlite3_stmt* stmt;

        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_int(stmt, 1, revenue);
            sqlite3_bind_text(stmt, 2, date.c_str(), -1, SQLITE_STATIC);

            if (sqlite3_step(stmt) != SQLITE_DONE) {
                std::cerr << "Update failed: " << sqlite3_errmsg(db) << std::endl;
            }
        } else {
            std::cerr << "Failed to prepare update statement: " << sqlite3_errmsg(db) << std::endl;
        }

        sqlite3_finalize(stmt);
    }
};

#endif