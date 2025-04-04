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

        shape.first = series[0].size();  // Update no count das linhas
        shape.second = series.size();    // Update no count das colunas
    }

    // Retorna uma cópia
    DataFrame<T> copy() {
        return DataFrame<T>(columns, series);
    }

    // Desconstrutor
    ~DataFrame() {
        series.clear();
        columns.clear();
    }

    // Adiciona coluna
    void addColumn(const std::string& columnName, const Series<T>& newSeries) {
        if (shape.first != 0 && shape.first != newSeries.size()) {
            throw std::invalid_argument("Series must have the same size as the DataFrame.");
        }
        columns.push_back(columnName);
        series.push_back(newSeries);
        shape.first = newSeries.size();  // Update no count das linhas
        shape.second = series.size();    // Update no count das colunas
    }

    // Remove yna coluna
    void dropColumn(const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        columns.erase(columns.begin() + column);
        series.erase(series.begin() + column);
        shape.second = series.size();  // Update no count das colunas
    }

    // Função para adicionar uma linha ao DataFrame
    void addLine(const std::vector<T>& newLine) {
        if (newLine.size() != columns.size()) {
            throw std::invalid_argument("New line must have the same number of elements as the number of columns.");
        }

        for (int i = 0; i < columns.size(); ++i) {
            series[i].addElement(newLine[i]);  // Adiciona o novo valor à Series correspondente
        }

        shape.first++;  // Incrementa o número de linhas no DataFrame
    }

    void deleteLine(int indexToRemove) {
        if (indexToRemove < 0 || indexToRemove >= shape.first) {
            throw std::out_of_range("Index out of range.");
        }
    
        // Remover o elemento da série correspondente em cada coluna
        for (int j = 0; j < shape.second; ++j) {
            series[j].removeElementAt(indexToRemove);  // Remove o elemento da série correspondente
        }
    
        shape.first--;  // Decrementa o número de linhas
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



    void deleteLastLine() {
        if (shape.first == 0) {
            throw std::out_of_range("No rows to delete.");
        }
    
        // Remover o último elemento em cada série
        for (int j = 0; j < shape.second; ++j) {
            series[j].removeLastElement();  // Remover o último elemento de cada série
        }
        shape.first--;  // Decrementa o número de linhas
    }
    

    // Acesso a uma coluna por nome
    Series<T>& operator[](const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        return series[column];
    }

    // Acesso a várias colunas por nome delas
    DataFrame<T> operator[](const std::vector<std::string>& columnNames) {
        DataFrame<T> result;
        for (const auto& column : columnNames) {
            result.addColumn(column, (*this)[column]);
        }
        return result;
    }

    // Concantenação (adiciona uma embaixo da outra)
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

    DataFrame<T> filter(const Series<bool>& condition) {
        if (condition.size() != shape.first) {
            throw std::invalid_argument("Condition series must have the same length as the DataFrame.");
        }
        DataFrame<T> result;
        for (const auto& column : columns) {
            result.addColumn(column, Series<T>());
        }
    
        for (int i = 0; i < shape.first; ++i) {
            if (condition[i]) {  // Condição booleana
                for (int j = 0; j < shape.second; ++j) {
                    result[columns[j]].addElement(series[j][i]);
                }
            }
        }
        return result;
    }

    // Soma de uma coluna
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

    // Média de uma coluna
    T mean(const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        T total = sum(columnName);
        return total / shape.first;
    }

    // Máximo de uma coluna
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

    // Printa o df
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

    std::pair<int, int> getShape() const {
        return shape;
    }

    const std::vector<std::string>& getColumns() const {
        return columns;
    }

private:
    // Acha o index da coluna por nome
    int column_id(const std::string& columnName) const {
        for (int i = 0; i < columns.size(); i++) {
            if (columns[i] == columnName) {
                return i;
            }
        }
        return -1;
    }

    std::vector<std::string> columns;  // Nomes das colunas
    std::vector<Series<T>> series;     // Series
    std::pair<int, int> shape;         // Shape do DF
};