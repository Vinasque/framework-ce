# Framework CE - Pipeline de Processamento de Dados Escalável

Framework para desenvolvimento de pipelines de processamento de dados utilizando técnicas de Computação Escalável.

## Dependências e Configuração

### Bibliotecas Necessárias
- **SQLite3**: Biblioteca de banco de dados embarcado (armazenamento eficiente em arquivo único)

### Instalação no Windows (MSYS2)
```bash
pacman -S make
pacman -S mingw-w64-x86_64-sqlite3
```

### Compilação e Execução
```bash
make       # Compila o projeto (gera main.exe)
make clean # Remove arquivos temporários
```

## Estrutura do Projeto
```
src/
├── sqlite3.c      # Implementação SQLite
├── sqlite3.h      # Cabeçalho SQLite
└── pipeline.cpp   # Lógica principal
```
