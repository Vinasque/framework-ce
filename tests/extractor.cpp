#include <iostream>
#include <string>
#include "../src/extractor.hpp"
#include "../src/dataframe.hpp"
#include "../src/series.hpp"
#include <sqlite3.h>


// Função para criar uma tabela SQLite e inserir dados
void createAndInsertData(const std::string& dbPath) {
    sqlite3* db;
    char* errMsg = nullptr;
    int rc = sqlite3_open(dbPath.c_str(), &db);

    if (rc) {
        std::cerr << "Cannot open database: " << sqlite3_errmsg(db) << std::endl;
        return;
    }

    const char* createTableQuery = R"(
        CREATE TABLE IF NOT EXISTS flight_orders (
            name TEXT,
            flight TEXT,
            price REAL
        );
    )";

    rc = sqlite3_exec(db, createTableQuery, 0, 0, &errMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "Failed to create table: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        return;
    }

    const char* insertDataQuery = R"(
        INSERT INTO flight_orders (name, flight, price) VALUES
        ('Joao', 'A11', 120.90),
        ('Luis', 'A22', 240.90),
        ('Maria', 'A33', 320.70),
        ('Thiago', 'A33', 380.90);
    )";

    rc = sqlite3_exec(db, insertDataQuery, 0, 0, &errMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "Failed to insert data: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        return;
    }

    sqlite3_close(db);
}

// Função para testar a extração dos dados do SQLite
void testSqliteExtractor() {
    std::cout << "=== Testando Extractor de SQLite ===" << std::endl;

    // Caminho para o arquivo do banco SQLite
    std::string dbPath = "test.db";

    // Criar a tabela e inserir os dados
    createAndInsertData(dbPath);

    // Criar instância do extractor
    Extractor extractor;

    // Definir a query para extrair os dados
    std::string query = "SELECT * FROM flight_orders;";

    // Usar a função extractFromSqlite para extrair os dados
    DataFrame<std::string> df = extractor.extractFromSqlite(dbPath, query);

    // Imprimir o DataFrame resultante
    std::cout << "\nDataFrame extraído do SQLite:" << std::endl;
    df.print();

    std::cout << "=== Fim do teste ===" << std::endl;
}

void testCsvExtractor() {
    std::cout << "=== Testando Extractor de CSV ===" << std::endl;

    // Caminho para o arquivo CSV de teste
    std::string filePath = "../generator/testData.csv";

    // Criar instância do extractor
    Extractor extractor;

    // Usar a função extractFromCsv para extrair dados do CSV
    DataFrame<std::string> df = extractor.extractFromCsv(filePath);

    // Imprimir o DataFrame resultante
    std::cout << "\nDataFrame extraído do CSV:" << std::endl;
    df.print();

    std::cout << "=== Fim do teste ===" << std::endl;
}

int main() {
    // Chama a função de teste
    testCsvExtractor();
    testSqliteExtractor();
    return 0;
}
