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
    // Constructor
    DataFrame() {};
    DataFrame(std::vector<std::string> columns, std::vector<Series<T>> series) {
        if (columns.size() != series.size()) {
            throw std::invalid_argument("Columns and series must have the same size");
        }

        for (int i = 0; i < columns.size(); i++) {
            addColumn(columns[i], series[i]);
        }

        shape.first = series[0].size();  // Update row count
        shape.second = series.size();    // Update column count
    }

    // Copy
    DataFrame<T> copy() {
        return DataFrame<T>(columns, series);
    }

    // Destructor
    ~DataFrame() {
        series.clear();
        columns.clear();
    }

    // Add column
    void addColumn(const std::string& columnName, const Series<T>& newSeries) {
        if (shape.first != 0 && shape.first != newSeries.size()) {
            throw std::invalid_argument("Series must have the same size as the DataFrame.");
        }
        columns.push_back(columnName);
        series.push_back(newSeries);
        shape.first = newSeries.size();  // Update row count
        shape.second = series.size();    // Update column count
    }

    // Remove column
    void dropColumn(const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        columns.erase(columns.begin() + column);
        series.erase(series.begin() + column);
        shape.second = series.size();  // Update column count
    }

    // Column access by name
    Series<T>& operator[](const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        return series[column];
    }

    // Access by list of columns
    DataFrame<T> operator[](const std::vector<std::string>& columnNames) {
        DataFrame<T> result;
        for (const auto& column : columnNames) {
            result.addColumn(column, (*this)[column]);
        }
        return result;
    }

    // Concatenation
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

    // Column sum
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

    // Column mean
    T mean(const std::string& columnName) {
        int column = column_id(columnName);
        if (column == -1) {
            throw std::invalid_argument("Column does not exist: " + columnName);
        }
        T total = sum(columnName);
        return total / shape.first;
    }

    // Column max value
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

    // Print DataFrame
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

    // Get shape of DataFrame
    std::pair<int, int> getShape() const {
        return shape;
    }

    // Get column names
    const std::vector<std::string>& getColumns() const {
        return columns;
    }

private:
    // Find column index by name
    int column_id(const std::string& columnName) const {
        for (int i = 0; i < columns.size(); i++) {
            if (columns[i] == columnName) {
                return i;
            }
        }
        return -1;
    }

    std::vector<std::string> columns;  // Column names
    std::vector<Series<T>> series;     // Data series (columns)
    std::pair<int, int> shape;         // DataFrame shape (rows, columns)
};