#include <grpcpp/grpcpp.h>
#include "event.pb.h"
#include "dataframe.hpp"
#include "event.grpc.pb.h"
#include "database.h"
#include "extractor.hpp"
#include "etl.cpp"
#include <string>
#include <atomic>
#include <thread>
#include <iomanip>  // Para std::put_time
#include <ctime>    // Para std::localtime

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using events::Event;
using events::Ack;
using events::EventService;

// Função auxiliar para formatar timestamp
std::string formatTimestamp(int64_t timestamp) {
    const time_t time = timestamp / 1000;
    struct tm* timeinfo = std::localtime(&time);
    char buffer[80];
    strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", timeinfo);
    return buffer;
}

class EventServiceImpl final : public EventService::Service {
public:
    EventServiceImpl(const std::string& db_path) : 
        db(db_path), 
        extractor(),
        first_run(true)  
    {
        std::cout << "Servidor inicializado. Aguardando eventos..." << std::endl;
    }

    Status SendEvent(ServerContext* /*context*/, const Event* request, Ack* reply) override {
        try {
            // Log dos dados recebidos
            std::cout << "\n=== NOVO EVENTO RECEBIDO ===" << std::endl;
            std::cout << "Flight ID: " << request->flight_id() << std::endl;
            std::cout << "Seat: " << request->seat() << std::endl;
            std::cout << "User ID: " << request->user_id() << std::endl;
            std::cout << "Customer: " << request->customer_name() << std::endl;
            std::cout << "Status: " << request->status() << std::endl;
            std::cout << "Payment Method: " << request->payment_method() << std::endl;
            std::cout << "Reservation Time: " << request->reservation_time() << std::endl;
            std::cout << "Price: " << request->price() << std::endl;
            std::cout << "Timestamp: " << formatTimestamp(request->timestamp()) 
                      << " (" << request->timestamp() << ")" << std::endl;

            // Converter o evento para DataFrame
            DataFrame<std::string> df = extractor.extractFromGrpcEvent(request);
            
            // Configuração do processamento
            TestResults::RunStats stats;
            bool current_first_run = first_run.exchange(false);
            
            std::cout << "Iniciando processamento paralelo com " 
                      << std::thread::hardware_concurrency() << " threads..." << std::endl;
            
            processParallelChunk(
                getOptimalThreadCount(df.numRows()),
                db,                                 
                "grpc_stream",                      
                df,
                stats,
                current_first_run                   
            );
            
            std::cout << "Processamento concluído com sucesso" << std::endl;
            
            reply->set_message("Evento processado com sucesso");
            
            return Status::OK;
        } catch (const std::exception& e) {
            std::cerr << "ERRO NO PROCESSAMENTO: " << e.what() << std::endl;
            reply->set_message(std::string("Erro: ") + e.what());
            return Status(grpc::INTERNAL, e.what());
        }
    }

private:
    DataBase db; 
    Extractor extractor;
    std::atomic<bool> first_run;
    
    unsigned int getOptimalThreadCount(size_t workload_size) {
        const unsigned int hw_threads = std::thread::hardware_concurrency();
        const unsigned int max_threads = (hw_threads == 0) ? 4 : hw_threads;
        
        if (workload_size < 50) return 1;
        // if (workload_size < 5000) return std::min(4u, max_threads);
        return max_threads;
    }
};

void RunServer(const std::string& server_address, const std::string& db_path) {
    EventServiceImpl service(db_path);
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Servidor rodando em " << server_address << std::endl;
    server->Wait();
}

int main() {
    std::cout << "Iniciando servidor gRPC..." << std::endl;
    RunServer("localhost:50051", "../databases/Database.db");
    return 0;
}