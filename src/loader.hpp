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
};

#endif