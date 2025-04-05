#pragma once

#include <vector>
#include <iostream>
#include <stdexcept>
#include <string>


template <typename T>
class Series {
public:
    // Construtor padrão
    Series() {}

    // Construtor que recebe um vetor de dados
    Series(std::vector<T> data) : data_(data) {}

    // Destruidor
    ~Series() {
        data_.clear();
    }

    // Função para adicionar um elemento à série
    void addElement(const T& value) {
        data_.push_back(value);
    }

    // Função para remover o último elemento da série
    void removeLastElement() {
        if (data_.empty()) {
            throw std::out_of_range("No elements to remove.");
        }
        data_.pop_back();  // Remove o último elemento
    }
    

    // Função para remover um elemento pelo índice
    void removeElementAt(int index) {
        if (index < 0 || index >= data_.size()) {
            throw std::out_of_range("Index out of range.");
        }
        data_.erase(data_.begin() + index);  // Remove o elemento na posição index
    }    

    // Função para adicionar todos os elementos de outra série
    Series<T> appendSeries(const Series<T>& other) const {
        Series<T> result = this->copy();  // Cópia da série atual
        for (const T& value : other.data_) {
            result.addElement(value);
        }
        return result;
    }

    T operator[](size_t index) const {
        if (index >= data_.size()) {
            throw std::out_of_range("Index out of range");
        }
        return data_[index];  // Retorna o valor por cópia, não por referência
    }

    // Obter o tamanho da série
    size_t size() const {
        return data_.size();
    }

    // Adicionar outra série de mesmo tipo
    Series<T> addSeries(const Series<T>& other) const {
        if (this->size() != other.size()) {
            throw std::invalid_argument("Both series must have the same size");
        }
        std::vector<T> result;
        for (size_t i = 0; i < this->size(); ++i) {
            result.push_back(data_[i] + other.data_[i]);
        }
        return Series<T>(result);
    }

    // Adicionar um valor escalar a cada elemento da série
    Series<T> addScalar(const T& scalar) const {
        std::vector<T> result;
        for (const T& value : data_) {
            result.push_back(value + scalar);
        }
        return Series<T>(result);
    }

    // Imprimir os elementos da série
    void print() const {
        std::cout << "Series: [ ";
        for (const T& value : data_) {
            std::cout << value << " ";
        }
        std::cout << "]" << std::endl;
    }

    // Criar uma cópia da série
    Series<T> copy() const {
        return Series<T>(data_);
    }

private:
    std::vector<T> data_;  // Dados armazenados na série
};
