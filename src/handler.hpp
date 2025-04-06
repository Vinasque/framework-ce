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

struct Reservation {
    std::string flight_id;
    std::string seat;
    std::string user_id;
    std::string customer_name;
    std::string status;
    std::string payment_method;
    std::string reservation_time;
    double price;
};

class BaseHandler {
protected:
    std::queue<Reservation> inputQueue;
    std::mutex inputMutex;
    std::condition_variable cv;
    bool running = true;
    
public:
    virtual void process(Reservation& res) = 0;
    
    void addReservation(const Reservation& res) {
        std::lock_guard<std::mutex> lock(inputMutex);
        inputQueue.push(res);
        cv.notify_one();
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
            
            Reservation res = inputQueue.front();
            inputQueue.pop();
            lock.unlock();
            
            process(res);
        }
    }

    // ✅ Novo método para verificar status da thread
    bool isRunning() const {
        return running;
    }

    bool tryPop(Reservation& res) {
        std::unique_lock<std::mutex> lock(inputMutex);
        if (inputQueue.empty()) return false;
        res = inputQueue.front();
        inputQueue.pop();
        return true;
    }
};

class ValidationHandler : public BaseHandler {
public:
    void process(Reservation& res) override {
        if (res.flight_id.empty() || 
            res.seat.empty() || 
            res.user_id.empty() || 
            res.customer_name.empty() ||
            res.payment_method.empty()) {
            return;
        }
        
        if (res.price <= 0) {
            res.status = "invalid_price";
        }
        
    }
};

class DateHandler : public BaseHandler {
private:
    std::string extractDate(const std::string& datetime) {
        if (datetime.length() >= 10) {
            return datetime.substr(0, 10);
        }
        return datetime;
    }

public:
    void process(Reservation& res) override {
        res.reservation_time = extractDate(res.reservation_time);
    }
};

class RevenueHandler : public BaseHandler {
private:
    double totalRevenue = 0.0;
    mutable std::mutex revenueMutex;

public:
    void process(Reservation& res) override {
        if (res.status == "confirmed") { 
            std::lock_guard<std::mutex> lock(revenueMutex);
            totalRevenue += res.price;
        }
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

#endif // HANDLER_HPP