#include "dataframe.hpp"
#include "loader.hpp"
#include "series.hpp"
#include "extractor.hpp"

int main() {
    {
        sqlite3* db;
        if (sqlite3_open("dados.db", &db) == SQLITE_OK) {
            const char* sql = "CREATE TABLE IF NOT EXISTS orders ("
                              "flight_id TEXT,"
                              "seat TEXT,"
                              "user_id TEXT,"
                              "customer_name TEXT,"
                              "status TEXT,"
                              "payment_method TEXT,"
                              "reservation_time TEXT,"
                              "price REAL);";
            char* errMsg = nullptr;
            if (sqlite3_exec(db, sql, nullptr, nullptr, &errMsg) != SQLITE_OK) {
                std::cerr << "Erro ao criar tabela: " << errMsg << std::endl;
                sqlite3_free(errMsg);
            }
            sqlite3_close(db);
        } else {
            std::cerr << "Erro ao abrir banco de dados." << std::endl;
        }
    }

    Extractor extractor;
    DataFrame<std::string> df = extractor.extractFromJson("../generator/orders.json");

    std::cout << "DataFrame extraído:" << std::endl;
    df.print();

    Loader loader("dados.db");
    loader.loadOrders(df); 

    std::cout << "Dados de pedidos inseridos com sucesso!" << std::endl;

    std::string dataDesejada = "2025-03-26";
    double lucro = loader.getRevenueByDate(dataDesejada);
    std::cout << "Lucro obtido em " << dataDesejada << ": R$ " << lucro << std::endl;

    return 0;
}