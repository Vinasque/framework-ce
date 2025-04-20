SQLITE_DIR = ../libs/sqlite3
TARGET_DIR = ./exes
TARGET = $(TARGET_DIR)/main.exe

# Lista dos seus arquivos C++ (adicione/remova aqui)
CPP_SOURCES = main2.cpp

# Regra principal
all:
	mkdir -p $(TARGET_DIR)
	gcc -Wno-unused-but-set-variable -I$(SQLITE_DIR) -c $(SQLITE_DIR)/sqlite3.c
	g++ -std=c++17 -I$(SQLITE_DIR) -I. $(CPP_SOURCES) sqlite3.o -o $(TARGET)

# Limpeza
clean:
	rm -f $(TARGET) sqlite3.o *.db ../databases/*.db 2>/dev/null || del $(TARGET) sqlite3.o *.db ..\databases\*.db 2>nul

# Execução
run:
	./$(TARGET) || .\$(TARGET)

.PHONY: all clean run
