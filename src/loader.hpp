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
            std::cerr << "Erro ao abrir banco: " << sqlite3_errmsg(db) << std::endl;
            db = nullptr;
        }
    }

    ~Loader() {
        if (db) sqlite3_close(db);
    }
};

#endif