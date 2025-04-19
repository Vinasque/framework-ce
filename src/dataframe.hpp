#pragma once

#include <vector>
#include <iostream>
#include <iomanip>
#include <stdexcept>
#include <map>
#include "series.hpp"

template <typename T>
class DataFrame {
public:
    // Construtor
    DataFrame() {};
    DataFrame(std::vector<std::string> columns, std::vector<Series<T>> series) {
        if (columns.size() != series.size()) {
            throw std::invalid_argument("Columns and series must have the same size");
        }

        for (int i = 0; i < columns.size(); i++) {
            addColumn(columns[i], series[i]);
        }

        shape.first = series[0].size();  // update no count das linhas
        shape.second = series.size();    // update no count das colunas
    }

    // retornar cópia
    DataFrame<T> copy() {
        return DataFrame<T>(columns, series);
    }

    ~DataFrame() {
        series.clear();
        columns.clear();
    }

    // adicionar coluna
    void addColumn(const std::string& columnName, const Series<T>& newSeries) {
        if (shape.first != 0 && shape.first != newSeries.size()) {
            throw std::invalid_argument("Series must have the same size as the DataFrame.");
        }
        columns.push_back(columnName);
        series.push_back(newSeries);
        shape.first = newSeries.size();  
        shape.second = series.size();   
    }

    // remover coluna
    void dropColumn(const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        columns.erase(columns.begin() + column);
        series.erase(series.begin() + column);
        shape.second = series.size(); 
    }

    // adicionar linha ao DataFrame
    void addLine(const std::vector<T>& newLine) {
        if (newLine.size() != columns.size()) {
            throw std::invalid_argument("New line must have the same number of elements as the number of columns.");
        }

        for (int i = 0; i < columns.size(); ++i) {
            series[i].addElement(newLine[i]);  // adiciona o novo valor à 'Series' correspondente
        }

        shape.first++; 
    }

    // deletar linha
    void deleteLine(int indexToRemove) {
        if (indexToRemove < 0 || indexToRemove >= shape.first) {
            throw std::out_of_range("Index out of range.");
        }
    
        // remover o elemento da série correspondente em cada coluna
        for (int j = 0; j < shape.second; ++j) {
            series[j].removeElementAt(indexToRemove);
        }
    
        shape.first--;
    }

    void updateValue(const std::string& columnName, int row, const T& newValue) {
        int colIdx = column_id(columnName);  // Obtém o índice da coluna
        if (colIdx == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);  // Verifica se a coluna existe
        }

        if (row < 0 || row >= shape.first) {
            throw std::out_of_range("Row index out of range.");  // Verifica se o índice da linha está no intervalo
        }

        // Atualiza o valor da célula na coluna e linha especificada
        series[colIdx].updateElementAt(row, newValue);
    }

    int numRows() const {
        return shape.first;
    }

    // retornar valor em uma célula (coluna + linha)
    T getValue(const std::string& columnName, int row) const {
        int colIdx = column_id(columnName);
        if (colIdx == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        if (row < 0 || row >= shape.first) {
            throw std::out_of_range("Row index out of range.");
        }
        return series[colIdx][row];
    }

    DataFrame<T> extractLines(size_t start, size_t end) {
        if (start >= end || end > shape.first) {
            throw std::out_of_range("Invalid range for extractLines");
        }
        
        DataFrame<T> result;
        for (size_t i = 0; i < columns.size(); ++i) {
            Series<T> new_series;
            for (size_t j = start; j < end; ++j) {
                new_series.addElement(series[i][j]);
            }
            result.addColumn(columns[i], new_series);
        }
        return result;
    }

    
    // Função para filtrar as linhas de um DataFrame baseado em uma condição numérica
    DataFrame<T> filter(const std::string& columnName, const std::string& condition, T value) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }

        // Criar um DataFrame de resultado que irá conter apenas as linhas que atendem à condição
        DataFrame<T> result;

        // Criar uma série temporária para cada coluna
        std::vector<Series<T>> tempSeries;  // Vetor para armazenar séries temporárias

        // Inicializar as séries temporárias para cada coluna
        for (const auto& columnName : columns) {
            tempSeries.push_back(Series<T>());
        }

        // Filtrar as linhas com base na condição
        for (int i = 0; i < shape.first; ++i) {
            T columnValue = series[column][i];  // Valor da coluna para a linha i
            bool conditionMet = false;

            // Verificar a condição
            if (condition == ">") {
                conditionMet = columnValue > value;
            } else if (condition == "<") {
                conditionMet = columnValue < value;
            } else if (condition == ">=") {
                conditionMet = columnValue >= value;
            } else if (condition == "<=") {
                conditionMet = columnValue <= value;
            } else if (condition == "==") {
                conditionMet = columnValue == value;
            } else if (condition == "!=") {
                conditionMet = columnValue != value;
            } else {
                throw std::invalid_argument("Invalid condition: " + condition);
            }

            // Se a condição for atendida, adicionar a linha ao DataFrame de resultado
            if (conditionMet) {
                for (int j = 0; j < shape.second; ++j) {
                    // Adicionar o valor da linha à série temporária correspondente à coluna
                    tempSeries[j].addElement(series[j][i]);
                }
            }
        }

        // Adicionar as séries temporárias ao DataFrame de resultado
        for (int j = 0; j < shape.second; ++j) {
            result.addColumn(columns[j], tempSeries[j]);
        }

        return result;
    }

    DataFrame<std::string> groupby(const std::string& groupByColumn, const std::string& sumColumn) {
        // Verificar se as colunas existem no DataFrame
        int groupByColIdx = column_id(groupByColumn);
        if (groupByColIdx == -1) {
            throw std::invalid_argument("GroupBy column does not exist: " + groupByColumn);
        }
        
        int sumColIdx = column_id(sumColumn);
        if (sumColIdx == -1) {
            throw std::invalid_argument("Sum column does not exist: " + sumColumn);
        }

        // Utilizar um mapa para agrupar e somar os valores
        std::map<std::string, double> groupedData;

        // Iterar sobre as linhas do DataFrame
        for (int i = 0; i < numRows(); ++i) {
            std::string groupByValue = getValue(groupByColumn, i);  // Obtém o valor da coluna de agrupamento
            double sumValue = std::stod(getValue(sumColumn, i));   // Obtém o valor da coluna de soma

            // Somar o valor no grupo correspondente
            groupedData[groupByValue] += sumValue;
        }

        // Preparar o DataFrame de resultado
        std::vector<std::string> columns = {groupByColumn, sumColumn};
        std::vector<Series<std::string>> series = {
            Series<std::string>(),  // Coluna de dias
            Series<std::string>()   // Coluna de soma de preços
        };

        // Preencher as séries com os resultados agrupados
        for (const auto& pair : groupedData) {
            series[0].addElement(pair.first);           // Adiciona o dia
            series[1].addElement(std::to_string(pair.second)); // Adiciona a soma de preços
        }

        // Retorna o DataFrame com o resultado do groupby
        return DataFrame<std::string>(columns, series);
    }



    void deleteLastLine() {
        if (shape.first == 0) {
            throw std::out_of_range("No rows to delete.");
        }
    
        for (int j = 0; j < shape.second; ++j) {
            series[j].removeLastElement();
        }
        shape.first--; 
    }
    
    // acessar coluna pelo nome
    Series<T>& operator[](const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        return series[column];
    }

    // acessar várias colunas pelos nome delas
    DataFrame<T> operator[](const std::vector<std::string>& columnNames) {
        DataFrame<T> result;
        for (const auto& column : columnNames) {
            result.addColumn(column, (*this)[column]);
        }
        return result;
    }

    // concantenação (adiciona uma embaixo da outra)
    DataFrame<T> concat(const DataFrame<T>& other) {
        if (columns != other.columns) {
            throw std::invalid_argument("DataFrames must have the same columns to concatenate.");
        }
        DataFrame<T> result;
        for (int i = 0; i < columns.size(); ++i) {
            result.addColumn(columns[i], series[i].appendSeries(other.series[i]));
        }
        return result;
    }

    // filtrar as linhas com base na condição
    DataFrame<T> filter(const Series<bool>& condition) {
        if (condition.size() != shape.first) {
            throw std::invalid_argument("Condition series must have the same length as the DataFrame.");
        }
        
        DataFrame<T> result;
        for (const auto& column : columns) {
            result.addColumn(column, Series<T>());
        }
    
        for (int i = 0; i < shape.first; ++i) {
            if (condition[i]) { 
                for (int j = 0; j < shape.second; ++j) {
                    result[columns[j]].addElement(series[j][i]);
                }
            }
        }
        return result;
    }

    // somar valores da coluna
    T sum(const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        T total = 0;
        for (int i = 0; i < shape.first; ++i) {
            total += series[column][i];
        }
        return total;
    }

    // média dos valores da coluna
    T mean(const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        T total = sum(columnName);
        return total / shape.first;
    }

    // máximo de uma coluna
    T max(const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        T maxVal = series[column][0];
        for (int i = 1; i < shape.first; ++i) {
            T value = series[column][i];
            if (value > maxVal) {
                maxVal = value;
            }
        }
        return maxVal;
    }

    // printar o df
    void print() const {
        for (const auto& column : columns) {
            std::cout << std::setw(15) << column;
        }
        std::cout << std::endl;

        for (int i = 0; i < shape.first; ++i) {
            for (int j = 0; j < shape.second; ++j) {
                std::cout << std::setw(15) << series[j][i];
            }
            std::cout << std::endl;
        }
    }

    // retornar o número de linhas
    std::pair<int, int> getShape() const {
        return shape;
    }

    // retornar o número de colunas
    const std::vector<std::string>& getColumns() const {
        return columns;
    }

    /**
 * Extrai as primeiras N linhas do DataFrame, removendo-as do original
 * @param n Número de linhas a extrair
 * @return Novo DataFrame com as N primeiras linhas
 * @throws std::out_of_range Se n for maior que o número de linhas
 */
DataFrame<T> extractFirstNLines(int n) {

    if (n <= 0) {
        return DataFrame<T>();  // Retorna DataFrame vazio
    }
    
    if (n > shape.first) {
        throw std::out_of_range("Cannot extract more lines than available in DataFrame");
    }
    
    // Cria novo DataFrame com as mesmas colunas
    DataFrame<T> result;
    for (const auto& col : columns) {
        result.addColumn(col, Series<T>());
    }
    
    // Extrai as linhas
    for (int i = 0; i < n; ++i) {
        // Pega a primeira linha do DataFrame original
        std::vector<T> line;
        for (int j = 0; j < shape.second; ++j) {
            line.push_back(series[j][0]);
        }
        
        // Adiciona ao novo DataFrame
        result.addLine(line);
        
        // Remove do DataFrame original
        deleteLine(0);  // Sempre remove a primeira linha (índice 0)
    }

    return result;
}

private:
    // achar o index da coluna por nome
    int column_id(const std::string& columnName) const {
        for (int i = 0; i < columns.size(); i++) {
            if (columns[i] == columnName) {
                return i;
            }
        }
        return -1;
    }

    std::vector<std::string> columns;  // nomes das colunas
    std::vector<Series<T>> series;     // series
    std::pair<int, int> shape;         // shape do DF
};