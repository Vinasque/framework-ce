#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>  // Para std::setw
#include "dataframe.hpp"
#include "extractor.hpp"
#include "handler.hpp"
#include "database.h"
#include "loader.hpp"
#include "ThreadPool.hpp"
#include "queue.hpp"

using Clock = std::chrono::high_resolution_clock;

// Função para imprimir o cabeçalho da tabela
void printTableHeader() {
    std::cout << "\n-------------------------------------------------------------------------------------------------------------------" << std::endl;
    std::cout << "| Arquivo        | Seq. Process | Seq. Load | Par. (4) Process | Par. (4) Load | Par. (8) Process | Par. (8) Load |" << std::endl;
    std::cout << "-------------------------------------------------------------------------------------------------------------------" << std::endl;
}

// Função para imprimir as linhas da tabela com os resultados dos testes
void printTableRow(const std::string& nomeArquivo, 
        const long& seqProc, const long& seqLoad,
        const long& par4Proc, const long& par4Load,
        const long& par8Proc, const long& par8Load) {
    std::cout << "| " << std::setw(14) << std::left << nomeArquivo
    << " | " << std::setw(12) << std::right << seqProc
    << " | " << std::setw(9) << std::right << seqLoad
    << " | " << std::setw(16) << std::right << par4Proc
    << " | " << std::setw(13) << std::right << par4Load
    << " | " << std::setw(16) << std::right << par8Proc
    << " | " << std::setw(13) << std::right << par8Load
    << " |" << std::endl;
    std::cout << "-----------------------------------------------------------------------------------------------------------------" << std::endl;
}

// Estrutura para armazenar os resultados dos testes
struct TestResults {
    long sequentialProcessingTime;
    long sequentialLoadTime;
    long parallel4ProcessingTime;
    long parallel4LoadTime;
    long parallel8ProcessingTime;
    long parallel8LoadTime;
};

// Função para executar o pipeline sequencial e calcular os tempos
void testSequentialPipeline(DataBase& db, std::string nomeArquivo, long& sequentialProcessingTime, long& sequentialLoadTime) {
    auto start = Clock::now();

    // Criação das tabelas no banco de dados
    db.createTable("faturamento" + nomeArquivo, "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo" + nomeArquivo, "(payment_method TEXT PRIMARY KEY, price REAL)");

    // Criação do extractor e carregamento do DataFrame
    Extractor extractor;
    DataFrame<std::string> df = extractor.extractFromJson("../generator/" + nomeArquivo + ".json");

    // Criando os handlers para processamento dos dados
    ValidationHandler validationHandler;
    StatusFilterHandler statusFilterHandler("confirmed");  // Adiciona o StatusFilterHandler
    DateHandler dateHandler;
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;

    // Início do processamento
    auto startProcessing = Clock::now();
    DataFrame<std::string> df2 = validationHandler.process(df);
    DataFrame<std::string> df3 = statusFilterHandler.process(df2);  // Aplica o StatusFilterHandler
    DataFrame<std::string> df4 = dateHandler.process(df3);
    DataFrame<std::string> df5 = revenueHandler.process(df4);
    DataFrame<std::string> df6 = cardHandler.process(df4);  // Corrigido para df4 (DataFrame atualizado)
    auto endProcessing = Clock::now();
    sequentialProcessingTime = std::chrono::duration_cast<std::chrono::milliseconds>(endProcessing - startProcessing).count();

    // Carregamento dos dados no banco de dados
    Loader loader(db);
    auto startLoad = Clock::now();
    loader.loadData("faturamento" + nomeArquivo, df5, {"reservation_time", "price"}, false);
    loader.loadData("faturamentoMetodo" + nomeArquivo, df6, {"payment_method", "price"}, false);
    auto endLoad = Clock::now();
    sequentialLoadTime = std::chrono::duration_cast<std::chrono::milliseconds>(endLoad - startLoad).count();

    auto end = Clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
}

// Função para executar o pipeline paralelo e calcular os tempos
void testParallelPipeline(int numThreads, DataBase& db, std::string nomeArquivo, 
        long& parallelProcessingTime, long& parallelLoadTime) {
    auto start = Clock::now();

    // Criação da thread pool
    ThreadPool pool(numThreads);

    // Criação das filas com capacidade para os pedaços de DataFrame
    Queue<int, DataFrame<std::string>> partitionQueue(numThreads);  // Primeira fila
    Queue<int, DataFrame<std::string>> revenueQueue(numThreads);    // Fila de revenue
    Queue<int, DataFrame<std::string>> cardQueue(numThreads);       // Fila de card

    // Criação do extractor e carregamento do DataFrame particionado
    Extractor extractor;
    extractor.extractFromJsonPartitioned("../generator/" + nomeArquivo + ".json", numThreads, partitionQueue);

    // Criando handlers para processamento dos dados
    ValidationHandler validationHandler;
    StatusFilterHandler statusFilterHandler("confirmed");  // Adiciona o StatusFilterHandler
    DateHandler dateHandler;
    RevenueHandler revenueHandler;
    CardRevenueHandler cardHandler;

    // Início do processamento paralelo
    auto startProcessing = Clock::now();

    // (1) Processamento de validação -> status -> data -> revenue/card
    std::vector<std::future<void>> futures;  // Para armazenar as futuras tarefas

    for (int i = 0; i < numThreads; ++i) {
    futures.push_back(pool.addTask([&, i] {
    // Etapa 1: Validação
    auto [index, chunk] = partitionQueue.deQueue();
    auto validChunk = validationHandler.process(chunk);

    // Etapa 2: Status (filtra "confirmed")
    auto filteredChunk = statusFilterHandler.process(validChunk);

    // Etapa 3: Data (agendada automaticamente)
    auto datedChunk = dateHandler.process(filteredChunk);

    // Etapa 4: Calcula revenue e card, mas não coloca nas filas ainda
    auto revenueChunk = revenueHandler.process(datedChunk);
    auto cardChunk = cardHandler.process(datedChunk);

    // Após todas as tarefas principais, agende as tarefas de enfileiramento
    pool.addTask([&, index, revenueChunk] {
    revenueQueue.enQueue({index, revenueChunk});
    });

    pool.addTask([&, index, cardChunk] {
    cardQueue.enQueue({index, cardChunk});
    });
    }));
    }

    // Espera TODAS as tarefas terminarem
    for (auto& fut : futures) {
    fut.get();  // Bloqueia até a tarefa acabar
    }

    auto endProcessing = Clock::now();
    parallelProcessingTime = std::chrono::duration_cast<std::chrono::milliseconds>(endProcessing - startProcessing).count();

    // Criando e conectando ao banco de dados
    db.createTable("faturamento" + nomeArquivo + "_" + std::to_string(numThreads), "(reservation_time TEXT PRIMARY KEY, price REAL)");
    db.createTable("faturamentoMetodo" + nomeArquivo + "_" + std::to_string(numThreads), "(payment_method TEXT PRIMARY KEY, price REAL)");

    Loader loader(db);

    auto startLoad = Clock::now();
    // Finalizando o carregamento dos dados processados
    for (int i = 0; i < numThreads; ++i) {
    auto [index, revenueChunk] = revenueQueue.deQueue();
    auto [indexCard, cardChunk] = cardQueue.deQueue();

    loader.loadData("faturamento" + nomeArquivo + "_" + std::to_string(numThreads), revenueChunk, {"reservation_time", "price"}, true);
    loader.loadData("faturamentoMetodo" + nomeArquivo + "_" + std::to_string(numThreads), cardChunk, {"payment_method", "price"}, true);
    }

    auto endLoad = Clock::now();
    parallelLoadTime = std::chrono::duration_cast<std::chrono::milliseconds>(endLoad - startLoad).count();

    auto end = Clock::now();
    auto totalDuration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

// Função para executar todos os testes e imprimir os resultados
void Test()
{
    std::vector<std::string> arquivos = {"ordersCemMil"};
    std::vector<TestResults> resultados(arquivos.size());
    DataBase db("DB_Teste.db");

    printTableHeader();

    // Testes Sequenciais
    for (size_t i = 0; i < arquivos.size(); ++i) {
        testSequentialPipeline(db, arquivos[i], resultados[i].sequentialProcessingTime, resultados[i].sequentialLoadTime);
    }

    // Testes Paralelos (4 threads)
    for (size_t i = 0; i < arquivos.size(); ++i) {
        testParallelPipeline(4, db, arquivos[i], resultados[i].parallel4ProcessingTime, resultados[i].parallel4LoadTime);
    }

    // Testes Paralelos (8 threads)
    for (size_t i = 0; i < arquivos.size(); ++i) {
        testParallelPipeline(8, db, arquivos[i], resultados[i].parallel8ProcessingTime, resultados[i].parallel8LoadTime);
    }

    // Imprime a tabela final com os resultados
    for (size_t i = 0; i < arquivos.size(); ++i) {
        printTableRow(
            arquivos[i],
            resultados[i].sequentialProcessingTime,
            resultados[i].sequentialLoadTime,
            resultados[i].parallel4ProcessingTime,
            resultados[i].parallel4LoadTime,
            resultados[i].parallel8ProcessingTime,
            resultados[i].parallel8LoadTime
        );
    }
}

int main() {
    Test();  // Executa os testes
    return 0;
}
