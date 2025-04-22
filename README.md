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
- **SQLite3**: Biblioteca de banco de dados embarcado (armazenamento eficiente em arquivo único)
- **Streamlit**: Biblioteca utilizada para a criação do dashboard
- **pandas**: Biblioteca para análise e manipulação de dados
- **plotly**: Biblioteca para visualização de dados interativos

### Instalação no Windows (MSYS2)
```bash
pacman -S make
pacman -S mingw-w64-x86_64-sqlite3
```
```bash
pip install streamlit pandas plotly
```

### Compilação e Execução
```bash
make       # Compila o projeto (gera main.exe)
make clean # Remove arquivos temporários
```

## Estrutura do Projeto
```
ETL_framework/
├── dashboard/                  # Pasta do dashboard
│   ├── main.py                 # Executável principal do dashboard
│   └── requirements.txt        # Bibliotecas necessárias

├── databases/                  # Contém os dados armazenados em .db
│   └── Database.db             # Banco SQLite principal

├── generator/                  # Dados mocks e arquivos que os geram
│   ├── flights.csv             
│   ├── orders.json             
│   ├── users.csv               
│   └── flight_generator.py
│   ...

├── src/                        # Código fonte principal
│   Contém os handlers, estruturas do extrator, loader, threads, pipeline principal, etc
│   └── main.cpp                # Main a ser executada

├── tests/                      # Testes feitos no decorrer do projeto

├── README.md                   # Documentação do projeto
├── Relatório.pdf               # Relatório 
```
