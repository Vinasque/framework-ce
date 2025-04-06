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
    sqlite3* db;      // Ponteiro para o banco de dados
    int rc;           // Mensagem de status do SQLite
    char* errMsg;     // Mensagem de erro, caso ocorra
    std::string sql;  // Query

    // Construtor: abre o banco de dados
    DataBase(const std::string& db_name) {
        rc = sqlite3_open(db_name.c_str(), &db);
        if (rc) {
            std::cerr << "Erro ao abrir o banco de dados: " << sqlite3_errmsg(db) << std::endl;
            throw std::runtime_error("Erro ao abrir o banco de dados");
        }
        std::cout << "Banco de dados aberto com sucesso!" << std::endl;

    }

    // O destruidor é chamado automaticamnete assim que o objeto DataBase sair do escopo
    ~DataBase() {
        if (db) {
            sqlite3_close(db); // Fecha a conexão com o modelo
            std::cout << "Banco de dados fechado!" << std::endl;
        }
    }

    /*
        ==================================================================================================================
        ao invés de deixar os métodos de CREATE e INSERT genéricos, por agora eu criei específicos pro nosso caso, que estão abaxio
        ==================================================================================================================
    */

    // TODO: implementar com diferentes colunas
    void createTable(const std::string& table_name, const std::string& schema)
    {
        // Cria uma tabela
        sql = "CREATE TABLE IF NOT EXISTS " + table_name + " " + schema + ";";
        // O schema é do tipo: (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)
        errMsg = nullptr;
        rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
        
        // Teste se criou corretamente
        if (rc != SQLITE_OK) {   
            std::cout << "Erro ao criar tabela: " << errMsg << std::endl;
            sqlite3_free(errMsg); }
        else {
            std::cout << "Tabela criada corretamente!" << std::endl;
        }
    }

    // TODO: implementar com diferentes colunas
    void insertValuesintoTable(const std::string& table_name, const std::string& date, const std::string& revenue)
    {                                                                                   // como primeiro teste, usei str no revenue
        // Insere valores na tabela
        sql = "INSERT INTO " + table_name + " (date, revenue) VALUES ('" + date + "', " + revenue + ");";
        errMsg = nullptr;
        rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);

        // Teste se inseriu corretamente
        if (rc != SQLITE_OK) {   
            std::cout << "Erro ao Inserir dados: " << errMsg << std::endl;
            sqlite3_free(errMsg); }
        else {
            std::cout << "Dados inseridos corretamente!" << std::endl;
        }
    }

    /*
        ===========================================================================================================================

        ===========================================================================================================================
    */

    // Método para imprimir toda a  (usado pra teste)
    void printTable(const std::string& tableName) {
        // Callback que será chamada para cada linha do resultado
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
