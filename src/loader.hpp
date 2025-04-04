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

    void carregarDataFrame(const DataFrame<std::string>& df) {
        if (!db) return;

        int rows = df.numRows();
        for (int i = 0; i < rows; ++i) {
            std::string data = df.getValue("data", i);
            int faturamento = std::stoi(df.getValue("faturamento", i));

            if (entradaExiste(data)) {
                atualizar(data, faturamento);
            } else {
                inserir(data, faturamento);
            }
        }
    }

private:
    sqlite3* db;
};

#endif