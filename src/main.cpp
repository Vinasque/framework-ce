#include <iostream>
#include "dataframe.hpp"
#include "extractor.hpp"
#include "handler.hpp"
#include "database.h"
#include "loader.hpp"

void testHandlerAndDatabase() {
    try {
        // 1. Cria/abre o banco de dados
        DataBase db("earning.db");
        
        // 2. Cria a tabela (se não existir)
        db.createTable("faturamento", "(date TEXT PRIMARY KEY, revenue REAL)");

        // 3. Usa o Extractor para pegar dados do JSON
        Extractor extractor;
        DataFrame<std::string> df = extractor.extractFromJson("../generator/orders.json");

        std::cout << "\nDataFrame carregado do JSON:" << std::endl;

        // 4. Cria os Handlers
        ValidationHandler validationHandler;
        DateHandler dateHandler;
        RevenueHandler revenueHandler;

        // 5. Passando o DataFrame pelos handlers sequencialmente
        DataFrame<std::string> df2 = validationHandler.process(df);  // Valida o DataFrame
        DataFrame<std::string> df3 = dateHandler.process(df2);        // Formata a data
        DataFrame<std::string> df4 = revenueHandler.process(df3);     // Calcula a receita

        std::cout << "\nProcessamento de reservas concluído!" << std::endl;
        df4.print();

        // 6. Exibir a receita total calculada pelo RevenueHandler
        std::cout << "Receita total: " << revenueHandler.getTotalRevenue() << std::endl;

        // 7. Cria o Loader e carrega os dados no banco de dados
        Loader loader(db);
        loader.loadData("faturamento", df4);  // Carrega os dados na tabela "faturamento"

        std::cout << "\nTabela 'faturamento' após inserção dos dados:" << std::endl;
        db.printTable("faturamento");

    } catch (const std::exception& e) {
        std::cerr << "Erro: " << e.what() << std::endl;
    }
}

int main() {
    testHandlerAndDatabase();
    return 0;
}
