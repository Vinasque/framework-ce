import sqlite3
import matplotlib.pyplot as plt
import numpy as np

# Função para ler dados do SQLite e retornar como listas
def read_data_from_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Executar as queries para pegar os dados da tabela
    cursor.execute("SELECT payment_method, price FROM faturamentoMetodo_ordersCemMil_4")
    payment_rows = cursor.fetchall()

    cursor.execute("SELECT reservation_time, price FROM faturamento_ordersCemMil_4")
    date_rows = cursor.fetchall()
    
    cursor.execute("SELECT user_country, price FROM faturamentoPaisUsuario_ordersCemMil_4")
    userC_rows = cursor.fetchall()
    
    cursor.execute("SELECT seat_type, price FROM faturamentoTipoAssento_ordersCemMil_4")
    seatT_rows = cursor.fetchall()

    conn.close()

    # Dados por método de pagamento
    payment_methods = [row[0] for row in payment_rows]
    payment_revenues = [row[1] for row in payment_rows]

    # Dados por data
    dates = [row[0] for row in date_rows]
    revenues = [row[1] for row in date_rows]

    # Dados por país do usuário
    countries = [row[0] for row in userC_rows]
    country_revenues = [row[1] for row in userC_rows]
    
    # Dados por país do usuário
    seat_types = [row[0] for row in seatT_rows]
    seat_revenues = [row[1] for row in seatT_rows]

    return payment_methods, payment_revenues, dates, revenues, countries, country_revenues, seat_types, seat_revenues

# Função para plotar os gráficos
def plot_revenue(dates, revenues, payment_methods, payment_revenues, countries, country_revenues, seat_types, seat_revenues):
    # Criando subplots: 1 linha e 3 colunas
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(1, 4, figsize=(18, 6))

    # Gráfico 1: Receita diária
    ax1.plot(dates, revenues, marker='o', linestyle='-', color='b', label='Receita Diária')
    ax1.set_xlabel('Data', fontsize=10)
    ax1.set_ylabel('Receita (R$)', fontsize=10)
    ax1.set_title('Receita Diária', fontsize=12)
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(True, linestyle='--', linewidth=0.5)
    ax1.legend(fontsize=9)
    ax1.set_xticks(np.arange(0, len(dates), step=max(1, int(len(dates)/6))))
    ax1.set_xticklabels(dates[::max(1, int(len(dates)/6))], rotation=45)

    # Gráfico 2: Receita por método de pagamento
    ax2.bar(payment_methods, payment_revenues, color='g', alpha=0.7)
    ax2.set_xlabel('Método de Pagamento', fontsize=10)
    ax2.set_ylabel('Receita (R$)', fontsize=10)
    ax2.set_title('Receita por Método de Pagamento', fontsize=12)
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(True, axis='y', linestyle='--', linewidth=0.5)
    ax2.legend(['Receita por Método'], fontsize=9)

    # Gráfico 3: Receita por país
    ax3.bar(countries, country_revenues, color='orange', alpha=0.7)
    ax3.set_xlabel('País do Usuário', fontsize=10)
    ax3.set_ylabel('Receita (R$)', fontsize=10)
    ax3.set_title('Receita por País do Usuário', fontsize=12)
    ax3.tick_params(axis='x', rotation=45)
    ax3.grid(True, axis='y', linestyle='--', linewidth=0.5)
    ax3.legend(['Receita por País'], fontsize=9)
    
    # Gráfico 4: Receita por tipo de assento
    ax4.bar(seat_types, seat_revenues, color='orange', alpha=0.7)
    ax4.set_xlabel('Tipo de Assento', fontsize=10)
    ax4.set_ylabel('Receita (R$)', fontsize=10)
    ax4.set_title('Receita por Tipo de Assento', fontsize=12)
    ax4.tick_params(axis='x', rotation=45)
    ax4.grid(True, axis='y', linestyle='--', linewidth=0.5)
    ax4.legend(['Receita por Assento'], fontsize=9)

    plt.tight_layout()
    plt.show()

# Função principal
def main():
    db_path = 'databases/DB_Teste.db'
    payment_methods, payment_revenues, dates, revenues, countries, country_revenues, seat_types, seat_revenues = read_data_from_sqlite(db_path)
    plot_revenue(dates, revenues, payment_methods, payment_revenues, countries, country_revenues, seat_types, seat_revenues)

if __name__ == "__main__":
    main()
