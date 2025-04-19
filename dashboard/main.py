import sqlite3
import matplotlib.pyplot as plt
import numpy as np

# Função para ler dados do SQLite e retornar como listas
def read_data_from_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Executar as queries para pegar os dados da tabela
    cursor.execute("SELECT payment_method, price FROM faturamentoMetodoordersCemMil")
    payment_rows = cursor.fetchall()

    cursor.execute("SELECT reservation_time, price FROM faturamentoordersCemMil")
    date_rows = cursor.fetchall()

    conn.close()

    # Separar os dados em listas: para as receitas por método de pagamento
    payment_methods = []
    payment_revenues = []
    
    for row in payment_rows:
        payment_methods.append(row[0])  # Método de pagamento
        payment_revenues.append(row[1])  # Receita do método de pagamento

    # Separar os dados em listas: para as receitas por data
    dates = []
    revenues = []
    
    for row in date_rows:
        dates.append(row[0])  # Data
        revenues.append(row[1])  # Receita

    return payment_methods, payment_revenues, dates, revenues

# Função para plotar os gráficos
def plot_revenue(dates, revenues, payment_methods, payment_revenues):
    # Criando subplots: 1 linha e 2 colunas
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Gráfico de linha para receita diária
    ax1.plot(dates, revenues, marker='o', linestyle='-', color='b', label='Receita Diária')
    ax1.set_xlabel('Data', fontsize=12)
    ax1.set_ylabel('Receita Diária (R$)', fontsize=12, color='b')
    ax1.set_title('Receita Diária ao Longo do Tempo', fontsize=14)
    ax1.tick_params(axis='y', labelcolor='b')
    ax1.grid(True, which='both', axis='both', linestyle='--', linewidth=0.5)
    ax1.legend(loc='upper left', fontsize=10)
    ax1.set_xticks(np.arange(0, len(dates), step=int(len(dates)/6)))  # Menos ticks no eixo X para não sobrecarregar
    ax1.set_xticklabels(dates[::int(len(dates)/6)], rotation=45)
    
    # Gráfico de barras para receita por método de pagamento
    ax2.bar(payment_methods, payment_revenues, color='g', alpha=0.7)
    ax2.set_xlabel('Método de Pagamento', fontsize=12)
    ax2.set_ylabel('Receita por Método de Pagamento (R$)', fontsize=12, color='g')
    ax2.set_title('Receita por Método de Pagamento', fontsize=14)
    ax2.tick_params(axis='y', labelcolor='g')
    ax2.grid(True, which='both', axis='y', linestyle='--', linewidth=0.5)
    ax2.legend(['Receita por Método de Pagamento'], loc='upper right', fontsize=10)
    ax2.set_xticklabels(payment_methods, rotation=45, ha='right')

    # Ajuste do layout para evitar sobreposição de textos
    plt.tight_layout()
    
    plt.show()

# Função principal para executar o código
def main():
    # Caminho para o banco de dados
    db_path = 'databases/DB_Teste.db'

    # Ler os dados do banco de dados SQLite
    payment_methods, payment_revenues, dates, revenues = read_data_from_sqlite(db_path)

    # Plotar os dados
    plot_revenue(dates, revenues, payment_methods, payment_revenues)

if __name__ == "__main__":
    main()
