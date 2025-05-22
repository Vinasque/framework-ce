SQLITE_DIR = ../libs/sqlite3
GRPC_DIR = ../grpc
TARGET_DIR = ./exes
TARGET = $(TARGET_DIR)/server.exe

CPP_SOURCES = server.cpp
PROTO_SOURCES = $(GRPC_DIR)/event.pb.cc $(GRPC_DIR)/event.grpc.pb.cc

# Modified compiler flags with library paths
CXXFLAGS = -std=c++17 -I$(SQLITE_DIR) -I$(GRPC_DIR) -I. -I/usr/local/include
LDFLAGS = -L/usr/local/lib \
			-lgrpc++ -lgrpc \
			-laddress_sorting -lgpr \
			-lupb -lcares -lre2 \
			-labsl_synchronization \
			-labsl_raw_logging_internal -labsl_bad_optional_access \
			-labsl_str_format_internal -labsl_time -labsl_base \
			-lprotobuf -lpthread \
			-lsqlite3 -lssl -lcrypto

all:
	@mkdir -p $(TARGET_DIR)
	
	# Compile SQLite
	gcc -Wno-unused-but-set-variable -I$(SQLITE_DIR) -c $(SQLITE_DIR)/sqlite3.c -o sqlite3.o
	
	# Compile protocol buffers
	$(CXX) $(CXXFLAGS) -c $(PROTO_SOURCES)
	
	# Compile main sources
	$(CXX) $(CXXFLAGS) -c $(CPP_SOURCES)
	
	# Link with correct order
	$(CXX) -o $(TARGET) *.o $(LDFLAGS)

clean:
	rm -f $(TARGET) *.o *.db ../databases/*.db 2>/dev/null || true

run:
	./$(TARGET)

.PHONY: all clean run