#include <iostream>
#include <vector>
#include "series.hpp" 
#include "dataframe.hpp" 

// Função de teste para a classe Series
void testSeries() {
    std::cout << "Testando Series" << std::endl;

    // Criando uma série de inteiros
    Series<int> s1({1, 2, 3, 4, 5});

    int size_s1 = s1.size();
    std::cout << "Tamanho de s1: " << size_s1 << std::endl;

    // Adicionando um elemento à série
    s1.addElement(6);
    s1.print(); 

    // Criando uma segunda série e somando com a primeira
    Series<int> s2({6, 7, 8, 9, 10, 11});
    Series<int> s3 = s1.addSeries(s2);
    s3.print();

    Series<int> super_s4 = s2.appendSeries(s3);
    super_s4.print();

    // Testando acesso por índice
    std::cout << "Elemento na posição 3 de s1: " << s1[3] << std::endl;  // Esperado: 4
}

// Função de teste para a classe DataFrame
void testDataFrame() {
    std::cout << "\nTestando DataFrame" << std::endl;

    // Criando Series
    Series<int> s1({1, 2, 3, 4});
    Series<int> s2({5, 6, 7, 8});
    Series<int> s3({9, 10, 11, 12});

    // Criando DataFrame com as Series
    DataFrame<int> df({"A", "B", "C"}, {s1, s2, s3});

    std::cout << "DataFrame original:" << std::endl;
    df.print();

    // Adicionando uma nova coluna
    Series<int> s4({13, 14, 15, 16});
    df.addColumn("D", s4);
    std::cout << "\nDataFrame após adicionar coluna D:" << std::endl;
    df.print();

    std::cout << "\nLOL" << std::endl;

    int sumA = df.sum("A");
    std::cout << "\nSoma da coluna A: " << sumA << std::endl; //Esperado: 10

    int meanB = df.mean("B");
    std::cout << "Média da coluna B: " << meanB << std::endl; //Esperado: 6

    int maxB = df.max("B");
    std::cout << "Máximo da coluna B: " << maxB << std::endl; //Esperado: 8

    // Copiando e concatenando DataFrames
    DataFrame<int> df2 = df.copy();
    DataFrame<int> df3 = df2.concat(df);

    std::cout << "\nDataFrame concatenado (df2 + df):" << std::endl;
    df3.print();

    // Testando a função deleteLine
    std::cout << "\nTestando deleteLine" << std::endl;
    df.deleteLine(2);  // Remover a linha [3, 7, 11, 15]
    std::cout << "DataFrame após remover a linha [3, 7, 11, 15]:" << std::endl;
    df.print();

    // Testando a função deleteLastLine
    std::cout << "\nTestando deleteLastLine" << std::endl;
    df.deleteLastLine(); 
    std::cout << "DataFrame após remover a última linha:" << std::endl;
    df.print();

    // Filtrando DataFrame: Mantendo apenas as linhas onde B > 5
    DataFrame<int> filteredDF = df.filter("B", ">", 5);
    std::cout << "\nDataFrame após filtro (B > 5):" << std::endl;
    filteredDF.print();
}


int main() {
    testSeries();
    testDataFrame();

    return 0;
}
