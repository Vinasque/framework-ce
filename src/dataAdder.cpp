#include <iostream>
#include <string>
#include <vector>
#include "extractor.hpp"
#include "database.h"
#include "loader.hpp"

void addUserData() {
    try {
        // 1. Cria/abre o banco de dados
        DataBase db("database.db");

        // 2. Cria a tabela 'users' (se não existir)
        db.createTable("users", "(user_id TEXT PRIMARY KEY, username TEXT, email TEXT, airmiles INTEGER)");

        // 3. Usa o Extractor para pegar dados do CSV
        Extractor extractor;
        DataFrame<std::string> df = extractor.extractFromCsv("../generator/users.csv");

        std::cout << "\nDataFrame carregado do CSV:" << std::endl;
        df.print();

        // 4. Cria o Loader e carrega os dados na tabela 'users'
        Loader loader(db);

        // Define as colunas que serão usadas para inserir dados na tabela 'users'
        std::vector<std::string> userColumns = {"user_id", "username", "email", "airmiles"};

        // Carrega os dados na tabela 'users'
        loader.loadData("users", df, userColumns);

        std::cout << "\nTabela 'users' após inserção dos dados:" << std::endl;
        db.printTable("users");

    } catch (const std::exception& e) {
        std::cerr << "Erro: " << e.what() << std::endl;
    }
}

int main() {
    addUserData();
    return 0;
}
