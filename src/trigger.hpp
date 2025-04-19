#pragma once
#include <functional>
#include <chrono>
#include <thread>
#include <atomic>
#include <memory>
#include "dataframe.hpp"

class Trigger {
protected:
    std::function<void()> callback; // Changed to void()
    std::atomic<bool> running{false};

public:
    virtual ~Trigger() = default;
    virtual void start() = 0;
    virtual void stop() = 0;
    void setCallback(std::function<void()> cb) { callback = cb; } // Changed to void()
};

class TimerTrigger : public Trigger {
private:
    std::chrono::milliseconds interval;
    std::thread timerThread;

public:
    TimerTrigger(int milliseconds) : interval(milliseconds) {}

    void start() override {
        running = true;
        timerThread = std::thread([this]() {
            while (running) {
                std::this_thread::sleep_for(interval);
                if (callback && running) {
                    callback(); // Now matches void() signature
                }
            }
        });
    }

    void stop() override {
        running = false;
        if (timerThread.joinable()) {
            timerThread.join();
        }
    }
};

class RequestTrigger : public Trigger {
public:
    void start() override { running = true; }
    void stop() override { running = false; }

    void trigger() {
        if (running && callback) {
            callback(); // Now matches void() signature
        }
    }
};