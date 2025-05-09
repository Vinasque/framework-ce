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
#include <unordered_map>
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
            try {
                return std::stoi(flightId);
            } catch (...) {
                return -1;
            }
        }
    }
    
    try {
        return std::stoi(flightId);
    } catch (...) {
        return -1;
    }
}

class FlightInfoEnricherHandler : public BaseHandler {
private:
    DataFrame<std::string> flightsDf;

public:
    FlightInfoEnricherHandler(const DataFrame<std::string>& flightsDf) : flightsDf(flightsDf) {}

    std::vector<DataFrame<std::string>> processMulti(const std::vector<DataFrame<std::string>>& inputDfs) override {
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

        std::unordered_map<int, int> flightNumberToIndex;
        for (int j = 0; j < flightsDf.numRows(); ++j) {
            int flightNum = extractFlightNumber(flightsDf.getValue("flight_id", j));
            if (flightNum != -1) {
                flightNumberToIndex[flightNum] = j;
            }
        }

        for (int i = 0; i < reservationsDf.numRows(); ++i) {
            int flightNum = extractFlightNumber(reservationsDf.getValue("flight_id", i));
            if (flightNum == -1) continue;

            flightCounts[flightNum]++;

            if (flightNumberToIndex.count(flightNum)) {
                int flightIdx = flightNumberToIndex[flightNum];
                reservationsDf.updateValue("origin", i, flightsDf.getValue("from", flightIdx));
                reservationsDf.updateValue("destination", i, flightsDf.getValue("to", flightIdx));
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
    
            std::vector<std::pair<std::string, int>> sortedDestinations(
                destinationCount.begin(), destinationCount.end());
            
            std::sort(sortedDestinations.begin(), sortedDestinations.end(),
                [](const auto& a, const auto& b) {
                    return a.second > b.second; 
                });

            Series<std::string> countries;
            Series<std::string> counts;
    
            for (const auto& [country, count] : sortedDestinations) {
                countries.addElement(country);
                counts.addElement(std::to_string(count));
            }
    
            resultDf.addColumn("destination", countries);
            resultDf.addColumn("reservation_count", counts);
    
            return resultDf;
        }
    };
    
class UsersCountryRevenue : public BaseHandler {
    private:
        const std::unordered_map<std::string, std::string>& userIdToCountry;
    
    public:
        UsersCountryRevenue(const std::unordered_map<std::string, std::string>& map)
            : userIdToCountry(map) {}
    
        DataFrame<std::string> process(DataFrame<std::string>& df) override {
            Series<std::string> flight_ids, seats, countries, prices;
    
            for (int i = 0; i < df.numRows(); ++i) {
                std::string userId = df.getValue("user_id", i);
                std::string country = userIdToCountry.count(userId) ? userIdToCountry.at(userId) : "Unknown";
    
                flight_ids.addElement(df.getValue("flight_id", i));
                seats.addElement(df.getValue("seat", i));
                countries.addElement(country);
                prices.addElement(df.getValue("price", i));
            }
    
            DataFrame<std::string> enrichedDf(
                {"flight_id", "seat", "user_country", "price"},
                {flight_ids, seats, countries, prices}
            );
    
            return enrichedDf.groupby("user_country", "price");
        }
    };
    

class SeatTypeRevenue : public BaseHandler {
private:
    const std::unordered_map<std::string, std::string>& seatKeyToClass;

public:
    SeatTypeRevenue(const std::unordered_map<std::string, std::string>& map)
        : seatKeyToClass(map) {}

    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        Series<std::string> flight_ids, seats, seat_types, prices;

        // Prefixo a ser removido
        const std::string flightPrefix = "AAA-"; 

        for (int i = 0; i < df.numRows(); ++i) {
            std::string flightId = df.getValue("flight_id", i);
            std::string seat = df.getValue("seat", i);

            // Remove o prefixo "AAA-" do flight_id
            if (flightId.find(flightPrefix) == 0) {
                flightId = flightId.substr(flightPrefix.length()); 
            }

            // Formar a chave completa
            std::string key = flightId + "_" + seat;

            // Verificar se a chave está no mapa
            std::string seatType = seatKeyToClass.count(key) ? seatKeyToClass.at(key) : "Econômica";

            flight_ids.addElement(flightId);
            seats.addElement(seat);
            seat_types.addElement(seatType);
            prices.addElement(df.getValue("price", i));
        }

        DataFrame<std::string> enrichedDf(
            {"flight_id", "seat", "seat_type", "price"},
            {flight_ids, seats, seat_types, prices}
        );

        return enrichedDf.groupby("seat_type", "price");
    }
};

class MeanPricePerDestination_AirlineHandler : public BaseHandler {
public:
    // Método adicional para shared_ptr
    std::vector<DataFrame<std::string>> processMultiShared(
        const std::vector<std::shared_ptr<const DataFrame<std::string>>>& inputDfs) {
        
        std::vector<DataFrame<std::string>> rawDfs;
        for (const auto& df_ptr : inputDfs) {
            rawDfs.push_back(*df_ptr);
        }
        return processMulti(rawDfs);
    }

    std::vector<DataFrame<std::string>> processMulti(
        const std::vector<DataFrame<std::string>>& inputDfs) override {
        
        if (inputDfs.size() < 2) {
            throw std::runtime_error("São necessários pelo menos 2 DataFrames como entrada");
        }

        DataFrame<std::string> df1 = inputDfs[0]; // DataFrame de assentos
        DataFrame<std::string> df2 = inputDfs[1]; // DataFrame de voos

        // Verificar colunas necessárias
        if (!df1.columnExists("flight_id") || !df1.columnExists("price")) {
            throw std::runtime_error("DataFrame 1 deve conter colunas 'flight_id' e 'price'");
        }

        if (!df2.columnExists("flight_id")) {
            throw std::runtime_error("DataFrame 2 deve conter coluna 'flight_id'");
        }

        // Calcular preço médio
        DataFrame<std::string> avgPriceDf = df1.groupbyMean("flight_id", "price");

        // Criar cópia do DataFrame de voos para não modificar o original
        DataFrame<std::string> resultDf = df2;
        resultDf.addColumn("avg_price", Series<std::string>::createEmpty(resultDf.numRows(), "0.0"));

        // Mapear flight_id para índices
        std::unordered_map<std::string, int> flightIdToIndex;
        for (int i = 0; i < resultDf.numRows(); ++i) {
            flightIdToIndex[resultDf.getValue("flight_id", i)] = i;
        }

        // Preencher preços médios
        for (int i = 0; i < avgPriceDf.numRows(); ++i) {
            std::string flightId = avgPriceDf.getValue("flight_id", i);
            std::string avgPrice = avgPriceDf.getValue("mean_price", i);
            
            if (flightIdToIndex.count(flightId)) {
                int flightIdx = flightIdToIndex[flightId];
                resultDf.updateValue("avg_price", flightIdx, avgPrice);
            }
        }
        
        DataFrame<std::string> MeanPerDestiny = resultDf.groupbyMean("to", "avg_price");
        DataFrame<std::string> MeanPerAirline = resultDf.groupbyMean("airline", "avg_price");
        
        return {MeanPerDestiny, MeanPerAirline};
    }

    DataFrame<std::string> process(DataFrame<std::string>& df) override {
        throw std::runtime_error("Este handler requer dois DataFrames como entrada. Use processMulti().");
    }
};


#endif // HANDLER_HPP
