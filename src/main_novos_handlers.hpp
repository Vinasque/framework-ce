#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>  
#include "dataframe.hpp"
#include "extractor.hpp"
#include "handler.hpp"
#include "database.h"
#include "loader.hpp"
#include "ThreadPool.hpp"
#include "queue.hpp"


void Test() {
    DataBase db("../databases/DB_Teste.db");
   
    std::string arquivoReservas = "ordersCemMil";
    std::string arquivoVoos = "flights";


    Extractor extractor;
    DataFrame<std::string> df_reservas = extractor.extractFromJson("../generator/" + arquivoReservas + ".json");
    DataFrame<std::string> df_voos = extractor.extractFromCsv("../generator/" + arquivoVoos + ".csv");


    // teste do GetFlightInfoHandler
    {
        std::cout << "\n=== TESTE FLIGHT INFO ENRICHER HANDLER ===" << std::endl;
       
        FlightInfoEnricherHandler enricher(df_voos);
        DataFrame<std::string> df_enriquecido = enricher.process(df_reservas);


        // verificando se as colunas foram adicionadas
        if (df_enriquecido.columnExists("origin") && df_enriquecido.columnExists("destination")) {
            std::cout << "SUCESSO: Colunas 'origin' e 'destination' adicionadas\n";
           
            // mostrando algumas amostras
            std::cout << "\nAmostras de dados enriquecidos:\n";
            for (int i = 0; i < 5 && i < df_enriquecido.numRows(); i++) {
                std::cout << "Voo " << df_enriquecido.getValue("flight_id", i)
                          << " - Origem: " << df_enriquecido.getValue("origin", i)
                          << ", Destino: " << df_enriquecido.getValue("destination", i)
                          << std::endl;
            }
        } else {
            std::cerr << "ERRO: Colunas não foram adicionadas corretamente\n";
        }
    }


    // teste do TopDestinationHandler
    {
        std::cout << "\n=== TESTE DESTINATION COUNTER HANDLER ===" << std::endl;
       
        // Primeiro enriquecemos os dados
        FlightInfoEnricherHandler enricher(df_voos);
        DataFrame<std::string> df_enriquecido = enricher.process(df_reservas);
       
        DestinationCounterHandler counter;
        DataFrame<std::string> df_resultado = counter.process(df_enriquecido);


        if (df_resultado.numRows() > 0) {
            std::string destino = df_resultado.getValue("most_common_destination", 0);
            std::string contagem = df_resultado.getValue("reservation_count", 0);
           
            std::cout << "SUCESSO: Destino mais comum encontrado\n";
            std::cout << "Destino mais popular: " << destino
                      << " (" << contagem << " reservas)\n";
           
            // mostrando estatísticas adicionais
            std::map<std::string, int> destinos;
            for (int i = 0; i < df_enriquecido.numRows(); i++) {
                destinos[df_enriquecido.getValue("destination", i)]++;
            }
           
            std::cout << "\nDistribuição completa de destinos:\n";
            for (const auto& [destino, count] : destinos) {
                std::cout << destino << ": " << count << " reservas\n";
            }
        } else {
            std::cerr << "ERRO: Não foi possível determinar o destino mais comum\n";
        }
    }


    // teste integrado
    {
        std::cout << "\n=== TESTE INTEGRADO ===" << std::endl;
       
        // criando tabelas no banco de dados
        db.createTable("destinos_mais_comuns", "(most_common_destination TEXT, reservation_count INTEGER)");
       
        FlightInfoEnricherHandler enricher(df_voos);
        DestinationCounterHandler counter;
        Loader loader(db);
       
        DataFrame<std::string> df_enriquecido = enricher.process(df_reservas);
        DataFrame<std::string> df_resultado = counter.process(df_enriquecido);
       
        // carregando no banco de dados
        loader.loadData("destinos_mais_comuns", df_resultado,
                       {"most_common_destination", "reservation_count"}, false);
       
        std::cout << "Dados carregados no banco de dados na tabela 'destinos_mais_comuns'\n";
    }
}


int main() {
    Test();  
    return 0;
}
