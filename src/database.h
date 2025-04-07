#ifndef DATABASE_H
#define DATABASE_H

#include <fstream>  
#include <iostream> 
#include <sstream>   
#include <string>    
#include <vector>   
#include <cstring>
#include "sqlite3.h"

class DataBase {
public:
    sqlite3* db;      // ponteiro para o banco de dados
    int rc;           // mensagem de status do SQLite
    char* errMsg;     // mensagem de erro, caso ocorra
    std::string sql;  // query

    // abre o banco de dados
    DataBase(const std::string& db_name) {
        rc = sqlite3_open(db_name.c_str(), &db);
        if (rc) {
            std::cerr << "Erro ao abrir o banco de dados: " << sqlite3_errmsg(db) << std::endl;
            throw std::runtime_error("Erro ao abrir o banco de dados");
        }
        std::cout << "Banco de dados aberto com sucesso!" << std::endl;

    }

    ~DataBase() {
        if (db) {
            sqlite3_close(db); // Fecha a conexão com o modelo
            std::cout << "Banco de dados fechado!" << std::endl;
        }
    }

    // criar tabela
    void createTable(const std::string& table_name, const std::string& schema)
    {
        sql = "CREATE TABLE IF NOT EXISTS " + table_name + " " + schema + ";";
        // (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)
        errMsg = nullptr;
        rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);

        if (rc != SQLITE_OK) {   
            std::cout << "Erro ao criar tabela: " << errMsg << std::endl;
            sqlite3_free(errMsg); }
        else {
            std::cout << "Tabela criada corretamente!" << std::endl;
        }
    }

    // inserir valores na tabela, agora com colunas e valores passados como parâmetros
    void insertValuesintoTable(const std::string& table_name, 
                               const std::vector<std::string>& columns, 
                               const std::vector<std::string>& values) 
    {
        // Verifica se o número de colunas é igual ao número de valores
        if (columns.size() != values.size()) {
            throw std::invalid_argument("Number of columns and values must match.");
        }

        // Monta a parte das colunas da query
        std::string columnsStr = "(";
        for (size_t i = 0; i < columns.size(); ++i) {
            columnsStr += columns[i];
            if (i < columns.size() - 1) {
                columnsStr += ", ";
            }
        }
        columnsStr += ")";

        // Monta a parte dos valores da query
        std::string valuesStr = "(";
        for (size_t i = 0; i < values.size(); ++i) {
            valuesStr += "'" + values[i] + "'";
            if (i < values.size() - 1) {
                valuesStr += ", ";
            }
        }
        valuesStr += ")";

        // Cria a query SQL
        sql = "INSERT INTO " + table_name + " " + columnsStr + " VALUES " + valuesStr + ";";
        
        // Executa a query
        errMsg = nullptr;
        rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);

        if (rc != SQLITE_OK) {   
            std::cout << "Erro ao Inserir dados: " << errMsg << std::endl;
            sqlite3_free(errMsg); 
        } else {
            std::cout << "Dados inseridos corretamente!" << std::endl;
        }
    }

    // imprimir a tabela
    void printTable(const std::string& tableName) {
        // callback que será chamada para cada linha do resultado
        auto callback = [](void* data, int argc, char** argv, char** colNames) -> int {
            for(int i = 0; i < argc; i++) {
                std::cout << colNames[i] << ": " << (argv[i] ? argv[i] : "NULL") << "\t";
            }
            std::cout << std::endl;
            return 0;
        };

        std::string sql = "SELECT * FROM " + tableName + ";";
        errMsg = nullptr;
        
        std::cout << "\nConteúdo da tabela " << tableName << ":" << std::endl;
        rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &errMsg);

        if(rc != SQLITE_OK) {
            std::cerr << "Erro ao ler tabela: " << errMsg << std::endl;
            sqlite3_free(errMsg);
        }
    }

};

#endif // DATABASE_H
