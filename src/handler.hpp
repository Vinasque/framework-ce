// handler.hpp
#ifndef HANDLER_HPP
#define HANDLER_HPP

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <string>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <cmath>
#include <map>
#include <algorithm>
#include "dataframe.hpp"

class Trigger;

// BaseHandler agora lida com DataFrame
class BaseHandler {
protected:
    std::queue<DataFrame<std::string>> inputQueue;
    std::mutex inputMutex;
    std::condition_variable cv;
    bool running = true;

public:
    virtual DataFrame<std::string> process(DataFrame<std::string>& df) = 0;
    
    virtual std::vector<DataFrame<std::string>> processMulti(
        const std::vector<DataFrame<std::string>>& inputDfs) {
        return {};
    }

    void stop() {
        running = false;
        cv.notify_all();
    }

    virtual ~BaseHandler() {}

    void run() {
        while (running) {
            std::unique_lock<std::mutex> lock(inputMutex);
            cv.wait(lock, [this] { return !inputQueue.empty() || !running; });

            if (!running) break;

            DataFrame<std::string> df = inputQueue.front();
            inputQueue.pop();
            lock.unlock();

            process(df);
        }
    }

    bool isRunning() const {
        return running;
    }

    void addDataFrame(DataFrame<std::string>& df) {
        std::lock_guard<std::mutex> lock(inputMutex);
        inputQueue.push(df);
        cv.notify_one();
    }
};


class TriggerableHandler : public BaseHandler {
protected:
    std::shared_ptr<Trigger> trigger;

public:
    void setTrigger(std::shared_ptr<Trigger> t) {
        trigger = t;
        if (trigger) {
            trigger->setCallback([this]() {
                this->processNext();
            });
        }
    }

    virtual void processNext() {
        DataFrame<std::string> df;
        {
            std::unique_lock<std::mutex> lock(inputMutex);
            if (!inputQueue.empty()) {
                df = inputQueue.front();
                inputQueue.pop();
            }
        }
        
        if (df.numRows() > 0) {
            process(df);
        }
    }
};

// Handler de validação (exemplo de process)
class ValidationHandler : public BaseHandler {
public:
    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        for (int i = 0; i < df.numRows(); ++i) {
            if (df.getValue("flight_id", i).empty()) {
                df.deleteLine(i);
            }
        }
        return df;
    }
};

class DateHandler : public BaseHandler {
public:
    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        for (int i = 0; i < df.numRows(); ++i) {
            std::string datetime = df.getValue("reservation_time", i);
            if (datetime.length() >= 10) {
                df.updateValue("reservation_time", i, datetime.substr(0, 10));
            }
        }
        return df;
    }
};

class RevenueHandler : public BaseHandler {
private:
    double totalRevenue = 0.0;
    mutable std::mutex revenueMutex;

public:
    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        DataFrame<std::string> groupedDf = df.groupby("reservation_time", "price");
        for (int i = 0; i < df.numRows(); ++i) {
            if (df.getValue("status", i) == "confirmed") {
                totalRevenue += std::stod(df.getValue("price", i));
            }
        }
        return groupedDf;
    }

    double getTotalRevenue() const {
        std::lock_guard<std::mutex> lock(revenueMutex);
        return totalRevenue;
    }

    void resetRevenue() {
        std::lock_guard<std::mutex> lock(revenueMutex);
        totalRevenue = 0.0;
    }
};

class CardRevenueHandler : public BaseHandler {
public:
    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        return df.groupby("payment_method", "price");
    }
};

class StatusFilterHandler : public BaseHandler {
private:
    std::string targetStatus;
    
public:
    StatusFilterHandler(const std::string& status) : targetStatus(status) {}
    
    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        for (int i = df.numRows() - 1; i >= 0; --i) {
            if (df.getValue("status", i) != targetStatus) {
                df.deleteLine(i);
            }
        }
        return df;
    }
};

int extractFlightNumber(const std::string& flightId) {
    size_t dashPos = flightId.find('-');
    if (dashPos != std::string::npos && dashPos + 1 < flightId.size()) {
        try {
            return std::stoi(flightId.substr(dashPos + 1));
        } catch (...) {
            return -1;
        }
    }
    return -1;
}

class FlightInfoEnricherHandler : public BaseHandler {
private:
    DataFrame<std::string> flightsDf;

public:
    FlightInfoEnricherHandler(const DataFrame<std::string>& flightsDf) : flightsDf(flightsDf) {}

    std::vector<DataFrame<std::string>> processMulti(
        const std::vector<DataFrame<std::string>>& inputDfs) override {
        
        if (inputDfs.empty()) {
            throw std::runtime_error("Input DataFrames vazio");
        }

        DataFrame<std::string> reservationsDf = inputDfs[0];
        
        if (!reservationsDf.columnExists("origin")) {
            reservationsDf.addColumn("origin", Series<std::string>::createEmpty(reservationsDf.numRows(), ""));
        }
        if (!reservationsDf.columnExists("destination")) {
            reservationsDf.addColumn("destination", Series<std::string>::createEmpty(reservationsDf.numRows(), ""));
        }

        DataFrame<std::string> flightStatsDf;
        flightStatsDf.addColumn("flight_number", Series<std::string>::createEmpty(0, ""));
        flightStatsDf.addColumn("reservation_count", Series<std::string>::createEmpty(0, ""));
        
        std::unordered_map<int, int> flightCounts;

        for (int i = 0; i < reservationsDf.numRows(); ++i) {
            int flightNum = extractFlightNumber(reservationsDf.getValue("flight_id", i));
            if (flightNum == -1) continue;

            flightCounts[flightNum]++;
            
            for (int j = 0; j < flightsDf.numRows(); ++j) {
                if (extractFlightNumber(flightsDf.getValue("flight_id", j)) == flightNum) {
                    reservationsDf.updateValue("origin", i, flightsDf.getValue("from", j));
                    reservationsDf.updateValue("destination", i, flightsDf.getValue("to", j));
                    break;
                }
            }
        }

        for (const auto& [flightNum, count] : flightCounts) {
            flightStatsDf.addLine({std::to_string(flightNum), std::to_string(count)});
        }

        return {reservationsDf, flightStatsDf};
    }

    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        auto results = processMulti({df});
        return results[0];
    }
};

class DestinationCounterHandler : public BaseHandler {
public:
    DataFrame<std::string> process(DataFrame<std::string>& enrichedDf) override {
        DataFrame<std::string> resultDf;
       
        if (enrichedDf.numRows() == 0 || !enrichedDf.columnExists("destination")) {
            return resultDf;
        }

        std::map<std::string, int> destinationCount;
        for (int i = 0; i < enrichedDf.numRows(); ++i) {
            destinationCount[enrichedDf.getValue("destination", i)]++;
        }

        auto [mostCommon, count] = *std::max_element(
            destinationCount.begin(),
            destinationCount.end(),
            [](const auto& a, const auto& b) { return a.second < b.second; });

        resultDf.addColumn("most_common_destination",
            Series<std::string>::createEmpty(1, mostCommon));
        resultDf.addColumn("reservation_count",
            Series<std::string>::createEmpty(1, std::to_string(count)));

        return resultDf;
    }
};

class UsersCountryRevenue : public BaseHandler {
private:
    mutable std::mutex revenueMutex;
    DataFrame<std::string> users_df;

public:
    UsersCountryRevenue(const DataFrame<std::string>& users) : users_df(users) {}

    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        // Mapeamento de user_id para país
        std::unordered_map<std::string, std::string> userIdToCountry;
        for (int i = 0; i < users_df.numRows(); ++i) {
            userIdToCountry[users_df.getValue("user_id", i)] = users_df.getValue("country", i);
        }

        // Preparar novas séries para colunas desejadas
        Series<std::string> flight_ids;
        Series<std::string> seats;
        Series<std::string> countries;
        Series<std::string> prices;

        for (int i = 0; i < df.numRows(); ++i) {
            std::string userId = df.getValue("user_id", i);
            std::string country = userIdToCountry.count(userId) ? userIdToCountry[userId] : "Unknown";

            flight_ids.addElement(df.getValue("flight_id", i));
            seats.addElement(df.getValue("seat", i));
            countries.addElement(country);
            prices.addElement(df.getValue("price", i));
        }

        // Criar novo DataFrame com colunas desejadas
        DataFrame<std::string> enrichedDf(
            {"flight_id", "seat", "user_country", "price"},
            {flight_ids, seats, countries, prices}
        );

        // Agrupamento por país
        DataFrame<std::string> groupedDf = enrichedDf.groupby("user_country", "price");
        return groupedDf;
    }
};
    

#endif // HANDLER_HPP