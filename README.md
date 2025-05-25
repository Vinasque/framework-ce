# Framework CE - Pipeline de Processamento de Dados Escalável

Framework para desenvolvimento de pipelines de processamento de dados utilizando técnicas de Computação Escalável.

### Membros do Grupo
- Guilherme Buss
- Guilherme Carvalho
- Gustavo Bianchi
- João Gabriel Machado
- Vinícius Nascimento
  
## Dependências e Configuração

### Bibliotecas Necessárias
Abaixo as bibliotecas em python para rodar o dashboard. O dashboard puxa os dados que são adicionados no SQlite (no Database.db)
- **SQLite3**: Biblioteca de banco de dados embarcado (armazenamento eficiente em arquivo único)
- **Streamlit**: Biblioteca utilizada para a criação do dashboard
- **pandas**: Biblioteca para análise e manipulação de dados
- **plotly**: Biblioteca para visualização de dados interativos

### Instalação no Windows (MSYS2/bash)
```bash
pacman -S make
pacman -S mingw-w64-x86_64-sqlite3
```
```bash
pip install streamlit pandas plotly
```

### Criando 'mock data'
Caso opte por gerar novamente os dados mock, basta executar os arquivos .py que estão dentro da pasta generator/. Atenção: certifique-se de executar os arquivos user_generator.py e flight_generator.py antes do orders_generator.py, já que as reservas devem ser de usuários e voos já criados.

```bash
cd generator/  # Entre na pasta generator
python3 user_generator.py
python3 flight_generator.py
python3 orders_generator.py
```

### Compilação do Dashboard em Streamlit 
```bash
python3 -m streamlit run main.py
```

### Compilação e Execução do Código em C++
Usando o makeFile
```bash
make       # Compila o projeto (gera main.exe)
make clean # Remove arquivos temporários
```

Ou, dentro da pasta src, gerar o executável e rodá-lo.
```bash
g++ -std=c++17 main.cpp sqlite3.o -o exes/main
\exes\main.exe
```

---
---

# GRPC - A2

Na solução inicial, a comunicação entre o simulador das fontes de dados e o pipeline ETL era feita por meio de arquivos intermediários (CSVs, Json, etc.), onde os dados eram gerados por scripts Python em arquivos CSV/JSON, que eram lidos e processados pelo ETL posteriormente. A abordagem limitava a execução a um único computador e introduzia latências relacionadas à escrita e leitura de disco, que é bem mais lenta que apenas passar os dados diretamente após serem "criados". Essa entregra permite que, através de um mecanismo de comunicação via gRPC, o que permite uma simulaçao de cargas reais distribuídas em rede de forma mais realista, facilita a expansão para o uso de múltiplas máquinas simultaneamente e menor latência, já que não depende mais da criação de arquivos no disco.

## Pré-requisitos

- **gRPC e Protobuf**: você deve ter o [gRPC](https://grpc.io/) e o `protoc` (instalador do Protocol Buffers) disponíveis no seu sistema.  
- **C++17**: compilador compatível (gcc ou clang).  
- **Python 3.8+**: para o cliente e geração de stubs Python.  

## Instalação

Para rodar o código em uma máquina Windows, foi-se utilizado do WSL (Windows Subsystem for Linux), já que o gRPC em C++ depende de várias bibliotecas e ferramentas que possuem suporte limitado ou apresentam instabilidades no ambiente Windows tradicional. 

### 1. Windows (via WSL)

Se você não estiver em Linux (se estiver, pode ignorar esse passo), instale o WSL e Ubuntu 22.04:

```bash
wsl --install
wsl --install -d Ubuntu-22.04
```

### 2. Dependências do Sistema e Protocol Buffers (protoc)

Após baixar o WSL, reinicie o seu PC e instale as dependências abaixo (já dentro do CMD do WSL, e não no CMD comum Windows):

```bash
# Dependências do Linux que serão necessárias para fazer o código rodar, como o sqlite, python, cmake, etc.
sudo apt update
sudo apt install -y \
  build-essential autoconf libtool pkg-config cmake git curl unzip \
  libgrpc++-dev libc-ares-dev libre2-dev libabsl-dev libabsl-synchronization-dev \
  libssl-dev sqlite3 libsqlite3-dev python3 python3-pip

# Protocol Buffers, usado para serializar dados, será utilizado no projeto
PROTOC_ZIP=protoc-21.12-linux-x86_64.zip
curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v21.12/$PROTOC_ZIP
sudo unzip -o $PROTOC_ZIP -d /usr/local bin/protoc
sudo unzip -o $PROTOC_ZIP -d /usr/local 'include/*'
rm -f $PROTOC_ZIP
```

### 3.1. gRPC em C++

```bash
git clone --recurse-submodules -b v1.56.0 https://github.com/grpc/grpc
cd grpc
mkdir -p cmake/build && cd cmake/build
cmake ../..
make -j$(nproc)
sudo make install
sudo ldconfig
```

### 3.2. gRPC em Python

```bash
sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 1
python -m venv ~/grpc-env
source ~/grpc-env/bin/activate
pip install grpcio grpcio-tools protobuf
```

## Geração de Código & Build

### Servidor C++ (stubs e compilação) e como Executar

Dentro da pasta do projeto, gere os exes através do ```make```, que você deve rodar dentro da pasta ```src```, que contem o Makefile, que contém instruções sobre como construir e compilar o projeto, onde o ```make``` automatiza o processo de compilação.

```bash
protoc --grpc_out=. --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) event.proto
protoc --cpp_out=. event.proto

cd .\src\
make        # gera executáveis em ./exes
./exes/server.exe
./exes/etl.exe
```

### Cliente Python (stubs) e como Executar

Para rodar os clientes, o código dos clientes está disponível na pasta ```grpc```, onde temos o código ```cliente.py```, que gera os dados que serão enviados para o ETL pelo python e envia ao servidor em C++, que já foi aberto.

```bash
cd grpc
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. event.proto

# Execução dentro da pasta grpc
python grpc/cliente.py <num_eventos> <intervalo_ms>  # roda os clientes
```

## Execução e Resultados

Ao rodar ambos os arquivos, você deve obter um resultado como esse, onde os terminais conseguem trocar mensagens:

![image](https://github.com/user-attachments/assets/259cd87c-3c47-426e-8d4d-19c58182fcd6)

Os resultados de forma mais detalhada estão disponíveis no relatório ```gRPC.pdf``` disponível neste repositório do GitHub.


## Estrutura do Projeto
```
framework-ce/
├── dashboard/
│ ├── main.py              # Executável principal do dashboard
│ └── requirements.txt     # Bibliotecas necessárias
│
├── databases/
│ └── Database.db          # Banco SQLite principal
│
├── generator/             # Onde os dados eram gerados (antes de começarmos a usar gRPC)
│ ├── flights.csv
│ ├── orders.json
│ ├── users.csv
│ └── flight_generator.py
│ ...
│
├── grpc/
│ ├── pycache/ 
│ ├── client.py             # Cliente que envia eventos via gRPC
│ ├── event.proto           # Definição dos eventos (Protobuf)
│ ├── event_pb2.py          # Código gerado do .proto (Python)
│ ├── event_pb2_grpc.py     # Código gRPC gerado (Python)
│ ├── event.pb.cc           # Código gerado do .proto (C++)
│ ├── event.pb.h            
│ ├── event.pb.o            # Objeto compilado (C++)
│ ├── event.grpc.pb.cc      # Código gRPC gerado (C++)
│ ├── event.grpc.pb.h       
│ └── event.grpc.pb.o       # Objeto compilado (C++)
│
├── src/
│ ├── exes/                 # Executáveis gerados (ex: server.exe)
│ ├── dataAdder.cpp         # Componente auxiliar de dados
│ ├── database.h            # Definições do banco SQLite
│ ├── dataframe.hpp
│ ├── extractor.hpp
│ ├── handler.hpp
│ ├── json.hpp
│ ├── loader.hpp
│ ├── main.cpp              # Entrada principal
│ ├── main2.cpp             # Variante de execução (debug/teste)
│ ├── Makefile              # Script de build principal
│ ├── queue.hpp
│ ├── series.hpp
│ ├── server.cpp            # Servidor gRPC que recebe os eventos
│ ├── server.o              # Objeto compilado
│ ├── sqlite3.o             # Objeto SQLite
│ ├── test.cpp            
│ ├── threadPool.hpp        # Gerenciamento de threads
│ └── trigger.hpp           # Lógica de triggers/ativadores
│
├── tests/                  # Testes do projeto
│
├── README.md               # Este arquivo
├── gRPC.pdf                # Relatório da Entrega 1 da A2
└── Relatório.pdf           # Relatório final da A1
```
