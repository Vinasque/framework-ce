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

        // 2. Cria as tabelas (se não existir)
        db.createTable("faturamento", "(reservation_time TEXT PRIMARY KEY, price REAL)");
        db.createTable("faturamentoMetodo", "(payment_method TEXT PRIMARY KEY, price REAL)");

        // 3. Usa o Extractor para pegar dados do JSON
        Extractor extractor;
        DataFrame<std::string> df = extractor.extractFromJson("../generator/orders.json");

        std::cout << "\nDataFrame carregado do JSON:" << std::endl;

        // 4. Cria os Handlers
        ValidationHandler validationHandler;
        DateHandler dateHandler;
        RevenueHandler revenueHandler;
        CardRevenueHandler dataRevenueHandler;

        // 5. Passando o DataFrame pelos handlers sequencialmente
        DataFrame<std::string> df2 = validationHandler.process(df);  // Valida o DataFrame
        DataFrame<std::string> df3 = dateHandler.process(df2);        // Formata a data
        DataFrame<std::string> df4 = revenueHandler.process(df3);    // Calcula a receita por dia
        DataFrame<std::string> df5 = dataRevenueHandler.process(df3);    // Calcula a receita por metodo de pagamento
        df5.print();

        std::cout << "\nProcessamento de reservas concluído!" << std::endl;

        // 6. Exibir a receita total calculada pelo RevenueHandler
        std::cout << "Receita total: " << revenueHandler.getTotalRevenue() << std::endl;

        // 7. Definir as colunas que serão usadas em cada tabela
        std::vector<std::string> faturamentoColumns = {"reservation_time", "price"};
        std::vector<std::string> faturamentoMetodoColumns = {"payment_method", "price"};

        // 8. Cria o Loader e carrega os dados no banco de dados
        Loader loader(db);
        std::cout << "teste 1" << std::endl;
        
        // Carrega os dados na tabela "faturamento"
        loader.loadData("faturamento", df4, faturamentoColumns);  

        // Carrega os dados na tabela "faturamentoMetodo"
        loader.loadData("faturamentoMetodo", df5, faturamentoMetodoColumns);  

        std::cout << "\nTabela 'faturamento' após inserção dos dados:" << std::endl;
        db.printTable("faturamento");
        std::cout << "\nTabela 'faturamentoMetodo' após inserção dos dados:" << std::endl;
        db.printTable("faturamentoMetodo");

    } catch (const std::exception& e) {
        std::cerr << "Erro: " << e.what() << std::endl;
    }
}

int main() {
    testHandlerAndDatabase();
    return 0;
}
