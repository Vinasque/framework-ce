#include <grpcpp/grpcpp.h>
#include "event.pb.h"
#include "event.grpc.pb.h"
#include "database.h"
#include <string>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using events::Event;
using events::Ack;
using events::EventService;

class EventServiceImpl final : public EventService::Service {
public:
    EventServiceImpl(const std::string& db_path) : db(db_path) {
        db.createTable("MockData", "(flight_id TEXT, seat TEXT, user_id TEXT, customer_name TEXT, status TEXT, payment_method TEXT, reservation_time TEXT, price TEXT, timestamp INTEGER)");
    }

    Status SendEvent(ServerContext* /*context*/, const Event* request, Ack* reply) override {
        std::vector<std::string> columns = {"flight_id", "seat", "user_id", "customer_name", "status", "payment_method", "reservation_time", "price", "timestamp"};
        std::vector<std::string> values = {
            request->flight_id(),
            request->seat(),
            request->user_id(),
            request->customer_name(),
            request->status(),
            request->payment_method(),
            request->reservation_time(),
            request->price(),
            std::to_string(request->timestamp())
        };

        db.insertValuesintoTable("MockData", columns, values);
        reply->set_message("Event received");
        return Status::OK;
    }

private:
    DataBase db;
};

void RunServer(const std::string& server_address, const std::string& db_path) {
    EventServiceImpl service(db_path);
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int /*argc*/, char** /*argv*/) {
    RunServer("0.0.0.0:50051", "../databases/MockSQL.db");
    return 0;
}