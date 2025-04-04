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

        shape.first = series[0].size();  // Atualiza o número de linhas
        shape.second = series.size();    // Atualiza o número de colunas
    }

    // Cópia
    DataFrame<T> copy() {
        return DataFrame<T>(columns, series);
    }

    // Destructor
    ~DataFrame() {
        series.clear();
        columns.clear();
    }

    // Adicionar coluna
    void addColumn(const std::string& columnName, const Series<T>& newSeries) {
        if (shape.first != 0 && shape.first != newSeries.size()) {
            throw std::invalid_argument("Series must have the same size as the DataFrame.");
        }
        columns.push_back(columnName);
        series.push_back(newSeries);
        shape.first = newSeries.size();  // Atualiza o número de linhas
        shape.second = series.size();    // Atualiza o número de colunas
    }

    // Remover coluna
    void dropColumn(const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        columns.erase(columns.begin() + column);
        series.erase(series.begin() + column);
        shape.second = series.size();  // Atualiza o número de colunas
    }

    // Acesso por nome de coluna
    Series<T>& operator[](const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        return series[column];
    }

    // Acesso por lista de colunas
    DataFrame<T> operator[](const std::vector<std::string>& columnNames) {
        DataFrame<T> result;
        for (const auto& column : columnNames) {
            result.addColumn(column, (*this)[column]);
        }
        return result;
    }

    // Concatenação
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

    // Valor máximo de uma coluna
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

    // Impressão do DataFrame
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

private:
    // Identificar a coluna pelo nome
    int column_id(const std::string& columnName) const {
        for (int i = 0; i < columns.size(); i++) {
            if (columns[i] == columnName) {
                return i;
            }
        }
        return -1;
    }

    std::vector<std::string> columns;  // Nomes das colunas
    std::vector<Series<T>> series;     // Séries de dados (colunas)
    std::pair<int, int> shape;         // Forma do DataFrame (número de linhas e colunas)
};
