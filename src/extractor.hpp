#pragma once

#include <string>
#include "json.hpp"
#include <fstream>
#include <iostream>
#include "dataframe.hpp"

class Extractor {
public:
    Extractor() = default;

    // extrair dados de um JSON e retornar como DataFrame
    DataFrame<std::string> extractFromJson(const std::string& filePath) {
        try {
            std::ifstream file(filePath);
            if (!file.is_open()) {
                throw std::runtime_error("Could not open the file: " + filePath);
            }

            nlohmann::json jsonData;
            file >> jsonData;

            if (!jsonData.is_array()) {
                throw std::runtime_error("JSON data should be an array of records");
            }

            std::vector<std::string> flightIds, seats, userIds, customerNames;
            std::vector<std::string> statuses, paymentMethods, reservationTimes, prices;

            for (const auto& record : jsonData) {
                flightIds.push_back(record["flight_id"].get<std::string>());
                seats.push_back(record["seat"].get<std::string>());
                userIds.push_back(record["user_id"].get<std::string>());
                customerNames.push_back(record["customer_name"].get<std::string>());
                statuses.push_back(record["status"].get<std::string>());
                paymentMethods.push_back(record["payment_method"].get<std::string>());
                reservationTimes.push_back(record["reservation_time"].get<std::string>());

                if (record.contains("price")) {
                    prices.push_back(std::to_string(record["price"].get<double>()));
                } else {
                    prices.push_back("0.0");
                }
            }

            std::vector<std::string> columns = {
                "flight_id", "seat", "user_id", "customer_name", 
                "status", "payment_method", "reservation_time", "price"
            };

            std::vector<Series<std::string>> series = {
                Series<std::string>(flightIds),
                Series<std::string>(seats),
                Series<std::string>(userIds),
                Series<std::string>(customerNames),
                Series<std::string>(statuses),
                Series<std::string>(paymentMethods),
                Series<std::string>(reservationTimes),
                Series<std::string>(prices)
            };

            return DataFrame<std::string>(columns, series);
        } catch (const std::exception& e) {
            std::cerr << "Extraction error: " << e.what() << std::endl;
            throw;
        }
    }
};