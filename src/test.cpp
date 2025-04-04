#include <iostream>
#include <vector>
#include "series.hpp" 
#include "dataframe.hpp" 

// Função de teste para a classe Series
void testSeries() {
    std::cout << "Testando Series" << std::endl;

    // Criando uma série de inteiros
    Series<int> s1({1, 2, 3, 4, 5});

    // Adicionando um elemento à série
    s1.addElement(6);
    s1.print(); 

    // Criando uma segunda série e somando com a primeira
    Series<int> s2({6, 7, 8, 9, 10, 11});
    Series<int> s3 = s1.addSeries(s2);
    s3.print(); 

    // Testando acesso por índice
    std::cout << "Elemento na posição 3 de s1: " << s1[3] << std::endl;  // Esperado: 4
}

// Função de teste para a classe DataFrame
void testDataFrame() {
    std::cout << "\nTestando DataFrame" << std::endl;

    // Criando Series de exemplo
    Series<int> s1({1, 2, 3, 4});
    Series<int> s2({5, 6, 7, 8});
    Series<int> s3({9, 10, 11, 12});

    // Criando DataFrame com as Series
    DataFrame<int> df({"A", "B", "C"}, {s1, s2, s3});

    // Imprimindo DataFrame
    std::cout << "DataFrame original:" << std::endl;
    df.print();

    // Adicionando uma nova coluna
    Series<int> s4({13, 14, 15, 16});
    df.addColumn("D", s4);
    std::cout << "\nDataFrame após adicionar coluna D:" << std::endl;
    df.print();

    std::cout << "\nLOL" << std::endl;

    // Somando a coluna A
    int sumA = df.sum("A");
    std::cout << "\nSoma da coluna A: " << sumA << std::endl;  // Esperado: 10 (1 + 2 + 3 + 4)

    // Calculando a média da coluna B
    int meanB = df.mean("B");
    std::cout << "Média da coluna B: " << meanB << std::endl;  // Esperado: 6 (5 + 6 + 7 + 8 / 4)
}

int main() {
    // Rodando os testes
    testSeries();
    testDataFrame();

    return 0;
}
