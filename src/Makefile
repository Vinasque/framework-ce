SQLITE_DIR = ../libs/sqlite3
GRPC_DIR = ../grpc
TARGET_DIR = ./exes
TARGET = $(TARGET_DIR)/server.exe

CPP_SOURCES = server.cpp
PROTO_SOURCES = $(GRPC_DIR)/event.pb.cc $(GRPC_DIR)/event.grpc.pb.cc

TARGET_ETL = $(TARGET_DIR)/etl.exe
CPP_SOURCES_ETL = main.cpp

CXX = g++

CXXFLAGS = -std=c++17 -I$(SQLITE_DIR) -I$(GRPC_DIR) -I.

LDFLAGS = -lgrpc++ -lgrpc \
          -lprotobuf \
          -labsl_synchronization -labsl_raw_logging_internal -labsl_bad_optional_access \
          -labsl_str_format_internal -labsl_time -labsl_base \
          -laddress_sorting -lgpr -lupb -lcares -lre2 \
          -lpthread -lsqlite3 -lssl -lcrypto

all: $(TARGET_DIR) $(TARGET) $(TARGET_ETL)

$(TARGET_DIR):
	@mkdir -p $(TARGET_DIR)

sqlite3.o: $(SQLITE_DIR)/sqlite3.c
	gcc -Wno-unused-but-set-variable -I$(SQLITE_DIR) -c $< -o $@

$(GRPC_DIR)/event.pb.o: $(GRPC_DIR)/event.pb.cc $(GRPC_DIR)/event.pb.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(GRPC_DIR)/event.grpc.pb.o: $(GRPC_DIR)/event.grpc.pb.cc $(GRPC_DIR)/event.grpc.pb.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

server.o: server.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

main.o: main.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(TARGET): sqlite3.o server.o $(GRPC_DIR)/event.pb.o $(GRPC_DIR)/event.grpc.pb.o
	$(CXX) -o $@ $^ $(LDFLAGS)

$(TARGET_ETL): main.o sqlite3.o
	$(CXX) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET) $(TARGET_ETL) *.o *.db ../databases/*.db 2>/dev/null || true

run: $(TARGET)
	./$(TARGET)

.PHONY: all clean run