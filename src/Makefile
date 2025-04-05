# Arquivos permanentes (C - SQLite)
C_SOURCES = ../src/sqlite3.c
C_OBJECTS = $(C_SOURCES:.c=.o)

# Arquivos C++ (adicione/remova aqui)
CPP_SOURCES = \
    ../src/gustavo_teste.cpp \
    # ../src/outro_arquivo.cpp \

CPP_OBJECTS = $(CPP_SOURCES:.cpp=.o)

# Nome do executável final
TARGET = main.exe

# Regra principal
all: $(TARGET)

# Compilação do SQLite (C)
%.o: %.c        # Esse Wno é uma mensagem de aviso que o código dá
	gcc -Wall -Wno-unused-but-set-variable -c $< -o $@

# Compilação dos arquivos C++
%.o: %.cpp
	g++ -Wall -c $< -o $@

# Linkagem final
$(TARGET): $(C_OBJECTS) $(CPP_OBJECTS)
	g++ -o $@ $^ -L/c/msys64/mingw64/lib -lsqlite3

# Limpeza
clean:
	rm -f $(C_OBJECTS) $(CPP_OBJECTS) $(TARGET) 

# Execução
run: $(TARGET)
	./$(TARGET)