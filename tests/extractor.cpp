#include <iostream>
#include <string>
#include "../src/extractor.hpp"
#include "../src/dataframe.hpp"
#include "../src/series.hpp"

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
    return 0;
}
