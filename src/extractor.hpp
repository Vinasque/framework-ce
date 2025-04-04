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
            // Open the file
            std::ifstream file(filePath);
            if (!file.is_open()) {
                throw std::runtime_error("Could not open the file: " + filePath);
            }

            // Parse JSON
            nlohmann::json jsonData;
            file >> jsonData;

            // Check if it's an array of records
            if (!jsonData.is_array()) {
                throw std::runtime_error("JSON data should be an array of records");
            }

            // Prepare vectors for each column
            std::vector<std::string> flightIds;
            std::vector<std::string> seats;
            std::vector<std::string> userIds;
            std::vector<std::string> customerNames;
            std::vector<std::string> statuses;
            std::vector<std::string> paymentMethods;
            std::vector<std::string> reservationTimes;

            // Extract data from each record
            for (const auto& record : jsonData) {
                flightIds.push_back(record["flight_id"]);
                seats.push_back(record["seat"]);
                userIds.push_back(record["user_id"]);
                customerNames.push_back(record["customer_name"]);
                statuses.push_back(record["status"]);
                paymentMethods.push_back(record["payment_method"]);
                reservationTimes.push_back(record["reservation_time"]);
            }

            // Create column names vector
            std::vector<std::string> columns = {
                "flight_id", "seat", "user_id", "customer_name", 
                "status", "payment_method", "reservation_time"
            };

            // Create Series for each column
            std::vector<Series<std::string>> series = {
                Series<std::string>(flightIds),
                Series<std::string>(seats),
                Series<std::string>(userIds),
                Series<std::string>(customerNames),
                Series<std::string>(statuses),
                Series<std::string>(paymentMethods),
                Series<std::string>(reservationTimes)
            };

            // Create and return DataFrame
            return DataFrame<std::string>(columns, series);

        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            throw; // Re-throw the exception for the caller to handle
        }
    }
};