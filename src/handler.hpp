// handler.hpp
#ifndef HANDLER_HPP
#define HANDLER_HPP

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>

struct Reservation {
    std::string flight_id;
    std::string seat;
    std::string user_id;
    std::string customer_name;
    std::string status;
    std::string payment_method;
    std::string reservation_time;
    double amount;
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
};

class ValidationHandler : public BaseHandler {
public:
    void process(Reservation& res) override {
        // Validate reservation
        // Remove invalid reservations
    }
};

class DateHandler : public BaseHandler {
public:
    void process(Reservation& res) override {
        // Adjust dates to consider only days
    }
};

class RevenueHandler : public BaseHandler {
public:
    void process(Reservation& res) override {
        // Calculate aggregated revenue per day
    }
};

#endif // HANDLER_HPP