#include <iostream>
#include <vector>
#include "series.hpp"
#include "dataframe.hpp"
#include "database.h"
#include "loader.hpp"

int main() {

    // 1. Cria/abre o banco de dados
    DataBase db("financas.db");
    
    // 2. Cria a tabela (se não existir)
    db.createTable("vendas", "(date TEXT PRIMARY KEY, revenue REAL)");

    // Criando Series
    Series<std::string> s1({"2025-03-06", "2025-03-07", "2025-04-05"});
    Series<std::string> s2({"5070.33", "3220.60", "3454.08"});

    // Criando DataFrame com as Series
    DataFrame<std::string> df({"date", "revenue"}, {s1, s2});

    df.print();
    // 5. Cria o Loader e carrega os dados
    Loader loader(db);
    loader.loadData("vendas", df);

    db.printTable("vendas");

    return 0;
}