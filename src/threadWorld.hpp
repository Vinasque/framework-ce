#pragma once

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <atomic>
#include <functional>
#include <memory>
#include <map>
#include "dataframe.hpp"
#include "handler.hpp"

class ThreadWorld {
private:
    struct Stage {
        std::queue<DataFrame<std::string>> data;
        std::mutex mutex;
        std::condition_variable cv;
        std::atomic<bool> stop{false};
        std::function<DataFrame<std::string>(DataFrame<std::string>&)> processor;
        std::vector<std::thread> workers;
        std::atomic<int> pending{0};
        int max_threads;
        std::vector<std::string> required_columns; // Columns required for this stage
    };

    std::vector<std::shared_ptr<Stage>> stages;
    std::atomic<bool> running{true};
    std::thread monitor_thread;
    int base_threads;

    // Modify your verify_columns function:
    void verify_columns(const DataFrame<std::string>& df, 
                        const std::vector<std::string>& required) {
        auto available_cols = df.getColumns();
        std::vector<std::string> missing;

        for (const auto& req : required) {
            if (std::find(available_cols.begin(), available_cols.end(), req) == available_cols.end()) {
                missing.push_back(req);
            }
        }

        if (!missing.empty()) {
            std::string error = "Missing columns:";
            for (const auto& col : missing) error += " " + col;
            throw std::invalid_argument(error);
        }
    }

    void worker(int stage_idx) {
        auto& stage = *stages[stage_idx];
        auto& next_stage = (stage_idx + 1 < stages.size()) ? *stages[stage_idx + 1] : *stages.back();
    
        // Create thread-local handler instance
        std::unique_ptr<ValidationHandler> val_handler;
        std::unique_ptr<DateHandler> date_handler;
        std::unique_ptr<RevenueHandler> revenue_handler;
        std::unique_ptr<CardRevenueHandler> card_handler;
    
        if (stage_idx == 0) val_handler = std::make_unique<ValidationHandler>();
        else if (stage_idx == 1) date_handler = std::make_unique<DateHandler>();
        else if (stage_idx == 2) revenue_handler = std::make_unique<RevenueHandler>();
        else if (stage_idx == 3) card_handler = std::make_unique<CardRevenueHandler>();
    
        while (running && !stage.stop) {
            // Get data from input buffer
            DataFrame<std::string> df;
            {
                std::unique_lock<std::mutex> lock(stage.mutex);
                stage.cv.wait(lock, [&] {
                    return !stage.data.empty() || stage.stop || !running;
                });
    
                if (stage.stop || !running) break;
                if (stage.data.empty()) continue;
    
                df = std::move(stage.data.front());
                stage.data.pop();
                stage.pending--;
            }
    
            try {
                // Verify required columns exist
                verify_columns(df, stage.required_columns);
    
                // Process data with appropriate handler
                DataFrame<std::string> processed;
                if (stage_idx == 0) processed = val_handler->process(df);
                else if (stage_idx == 1) processed = date_handler->process(df);
                else if (stage_idx == 2) processed = revenue_handler->process(df);
                else if (stage_idx == 3) processed = card_handler->process(df);
    
                // Pass to next stage if not empty
                if (processed.numRows() > 0 && stage_idx + 1 < stages.size()) {
                    std::lock_guard<std::mutex> lock(next_stage.mutex);
                    next_stage.data.push(std::move(processed));
                    next_stage.pending++;
                    next_stage.cv.notify_one();
                
                }
    
            } catch (const std::exception& e) {
                std::cerr << "Error in stage " << stage_idx << ": " << e.what() << "\n";
                continue;
            }
        }
        
    }

    void monitor() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            for (size_t i = 0; i < stages.size(); i++) {
                auto& stage = *stages[i];
                int current_threads = stage.workers.size();
                int queue_size = stage.data.size();

                // Add threads if needed
                if (queue_size > current_threads * 2 && current_threads < stage.max_threads) {
                    int to_add = std::min(2, stage.max_threads - current_threads);
                    for (int j = 0; j < to_add; j++) {
                        stage.workers.emplace_back([this, i] { worker(i); });
                    }
                }
                // Remove threads if possible
                else if (queue_size < current_threads && current_threads > base_threads) {
                    int to_remove = std::min(1, current_threads - base_threads);
                    {
                        std::lock_guard<std::mutex> lock(stage.mutex);
                        stage.stop = true;
                        stage.cv.notify_all();
                    }
                    for (int j = 0; j < to_remove; j++) {
                        if (stage.workers.back().joinable()) {
                            stage.workers.back().join();
                            stage.workers.pop_back();
                        }
                    }
                    stage.stop = false;
                }
            }
        }
    }

public:
    struct PipelineResults {
        DataFrame<std::string> revenue;
        DataFrame<std::string> card;
    };

    PipelineResults getFinalResults() {
        PipelineResults results;
        
        // Lock all stages to get consistent results
        std::vector<std::unique_lock<std::mutex>> locks;
        for (auto& stage : stages) {
            locks.emplace_back(stage->mutex);
        }

        // Get revenue results (assuming stage 2)
        if (stages.size() > 2 && !stages[2]->data.empty()) {
            results.revenue = std::move(stages[2]->data.front());
            stages[2]->data.pop();
        }

        // Get card results (assuming stage 3)
        if (stages.size() > 3 && !stages[3]->data.empty()) {
            results.card = std::move(stages[3]->data.front());
            stages[3]->data.pop();
        }

        return results;
    }
    
    ThreadWorld(int base_threads = 1, int max_threads = 4) : base_threads(base_threads) {
        monitor_thread = std::thread([this] { monitor(); });
    }

    ~ThreadWorld() {
        try {
            running = false;
            if (monitor_thread.joinable()) {
                monitor_thread.join();
            }
            for (auto& stage : stages) {
                stage->stop = true;
                stage->cv.notify_all();
                for (auto& thread : stage->workers) {
                    if (thread.joinable()) thread.join();
                }
            }
        } catch (...) {
            std::cerr << "Error during ThreadWorld shutdown" << std::endl;
        }
    }

    void addStage(std::function<DataFrame<std::string>(DataFrame<std::string>&)> processor, 
                 const std::vector<std::string>& required_columns,
                 int max_threads = 4) {
        auto stage = std::make_shared<Stage>();
        stage->processor = processor;
        stage->max_threads = max_threads;
        stage->required_columns = required_columns;
        stages.push_back(stage);

        // Start base threads
        for (int i = 0; i < base_threads; i++) {
            stage->workers.emplace_back([this, i = stages.size() - 1] { worker(i); });
        }
    }

    void start(DataFrame<std::string>& initialData) {
        if (stages.empty()) return;

        // Verify columns for first stage
        verify_columns(initialData, stages[0]->required_columns);

        {
            std::lock_guard<std::mutex> lock(stages[0]->mutex);
            stages[0]->data.push(initialData);
            stages[0]->pending++;
            stages[0]->cv.notify_all();
        }
    }

    DataFrame<std::string> getResult() {
        if (stages.empty()) return DataFrame<std::string>();

        auto& last = *stages.back();
        std::unique_lock<std::mutex> lock(last.mutex);
        if (last.data.empty()) return DataFrame<std::string>();

        DataFrame<std::string> result = std::move(last.data.front());
        last.data.pop();
        return result;
    }

    void waitForCompletion() {
        while (running) {
            bool all_empty = true;
            for (auto& stage : stages) {
                std::lock_guard<std::mutex> lock(stage->mutex);
                if (!stage->data.empty()) {
                    all_empty = false;
                    break;
                }
            }
            if (all_empty) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    DataFrame<std::string> getResultFromStage(int stage_idx) {
        if (stage_idx < 0 || stage_idx >= stages.size()) {
            throw std::out_of_range("Invalid stage index");
        }
        std::lock_guard<std::mutex> lock(stages[stage_idx]->mutex);
        if (stages[stage_idx]->data.empty()) return DataFrame<std::string>();
        return std::move(stages[stage_idx]->data.front());
    }
};