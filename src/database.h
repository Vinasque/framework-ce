#ifndef DATABASE_H
#define DATABASE_H

#include <fstream>  
#include <iostream> 
#include <sstream>   
#include <string>    
#include <vector>   
#include <cstring>
#include "sqlite3.h"
#include <mutex>

class DataBase {
public:
    sqlite3* db;      // ponteiro para o banco de dados
    int rc;           // mensagem de status do SQLite
    char* errMsg;     // mensagem de erro, caso ocorra
    std::string sql;  // query
    std::mutex dbMutex;

    // abre o banco de dados
    DataBase(const std::string& db_name) {
        rc = sqlite3_open(db_name.c_str(), &db);
        if (rc) {
            std::cerr << "Erro ao abrir o banco de dados: " << sqlite3_errmsg(db) << std::endl;
            throw std::runtime_error("Erro ao abrir o banco de dados");
        }
        // std::cout << "Banco de dados aberto com sucesso!" << std::endl;

    }

    ~DataBase() {
        if (db) {
            sqlite3_close(db); // Fecha a conexão com o modelo
            // std::cout << "Banco de dados fechado!" << std::endl;
        }
    }

    // criar tabela
    void createTable(const std::string& table_name, const std::string& schema)
    {
        std::lock_guard<std::mutex> lock(dbMutex);
        sql = "CREATE TABLE IF NOT EXISTS " + table_name + " " + schema + ";";
        // (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)
        errMsg = nullptr;
        rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);

        if (rc != SQLITE_OK) {   
            std::cout << "Erro ao criar tabela: " << errMsg << std::endl;
            sqlite3_free(errMsg); }
        else {
            // std::cout << "Tabela criada corretamente!" << std::endl;
        }
    }

    // inserir valores na tabela, agora com colunas e valores passados como parâmetros
    void insertValuesintoTable(const std::string& table_name, 
                               const std::vector<std::string>& columns, 
                               const std::vector<std::string>& values,
                               bool bGroupBy) 
    {
        std::lock_guard<std::mutex> lock(dbMutex);
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
        if (bGroupBy) {
            // TODO: deixar isso genérico
            sql = "INSERT INTO " + table_name + " " + columnsStr + " VALUES " + valuesStr +
            " ON CONFLICT(" + columns[0] + ") DO UPDATE SET " + columns.back() + " = " + columns.back() + " + EXCLUDED." + columns.back() + ";";
        }
        else {
            sql = "INSERT INTO " + table_name + " " + columnsStr + " VALUES " + valuesStr + ";";
        }
        
        // Executa a query
        errMsg = nullptr;
        rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);

        // DEBUG: mostrar a query executada (ativa só se precisar)
        bool verbose = true;
        //if (verbose) {
            //std::cout << "[DEBUG] Executando query: " << sql << std::endl;
        //}

        if (rc != SQLITE_OK) {
            std::cerr << "Erro ao Inserir dados: " 
                    << (errMsg ? errMsg : "mensagem de erro desconhecida") << std::endl;

            if (verbose && errMsg) {
                std::cerr << "[ERRO SQL] " << errMsg << std::endl;
            }

            sqlite3_free(errMsg); 
        }
    }

    // imprimir a tabela
    void printTable(const std::string& tableName) {
        std::lock_guard<std::mutex> lock(dbMutex);
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

    void execute(const std::string& sql, const std::vector<std::string>& params = {}) {
        std::lock_guard<std::mutex> lock(dbMutex);
        sqlite3_stmt* stmt;
        
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
            return;
        }
    
        // Bind parameters
        for (size_t i = 0; i < params.size(); ++i) {
            sqlite3_bind_text(stmt, i+1, params[i].c_str(), -1, SQLITE_TRANSIENT);
        }
    
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            std::cerr << "Failed to execute statement: " << sqlite3_errmsg(db) << std::endl;
        }
    
        sqlite3_finalize(stmt);
    }

};

#endif // DATABASE_H
