#include "DataBase.h"

int main() {
    try {
        std::cout << "Iniciando teste do banco de dados..." << std::endl;
        
        // Cria/abre o banco de dados (arquivo será criado se não existir)
        DataBase db("test_database.db");

        db.printTable("tabela_teste");
        
        std::cout << "Teste concluído com sucesso!" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Erro fatal: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}