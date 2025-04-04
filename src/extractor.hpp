#pragma once

#include <string>
#include "json.hpp"
#include <fstream>
#include <iostream>
#include "dataframe.hpp"

class Extractor {
public:
    // Constructor
    Extractor() = default;

    // Method to extract data from a JSON file and return as DataFrame
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
            std::vector<std::string> statuses, paymentMethods, reservationTimes, amounts;

            for (const auto& record : jsonData) {
                flightIds.push_back(record["flight_id"]);
                seats.push_back(record["seat"]);
                userIds.push_back(record["user_id"]);
                customerNames.push_back(record["customer_name"]);
                statuses.push_back(record["status"]);
                paymentMethods.push_back(record["payment_method"]);
                reservationTimes.push_back(record["reservation_time"]);
                
                // Handle amount field - provide default if missing
                amounts.push_back(record.value("amount", "0.0")); // Default to 0.0 if amount is missing
            }

            std::vector<std::string> columns = {
                "flight_id", "seat", "user_id", "customer_name", 
                "status", "payment_method", "reservation_time", "amount"
            };

            std::vector<Series<std::string>> series = {
                Series<std::string>(flightIds),
                Series<std::string>(seats),
                Series<std::string>(userIds),
                Series<std::string>(customerNames),
                Series<std::string>(statuses),
                Series<std::string>(paymentMethods),
                Series<std::string>(reservationTimes),
                Series<std::string>(amounts)
            };

            return DataFrame<std::string>(columns, series);
        } catch (const std::exception& e) {
            std::cerr << "Extraction error: " << e.what() << std::endl;
            throw;
        }
    }
};