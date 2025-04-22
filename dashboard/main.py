import sqlite3
import matplotlib.pyplot as plt
import numpy as np

# Função para ler dados do SQLite e retornar como listas
def read_data_from_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Executar as queries para pegar os dados da tabela
    cursor.execute("SELECT payment_method, price FROM faturamentoMetodo_orders_4")
    payment_rows = cursor.fetchall()

    cursor.execute("SELECT reservation_time, price FROM faturamento_orders_4")
    date_rows = cursor.fetchall()
    
    cursor.execute("SELECT user_country, price FROM faturamentoPaisUsuario_orders_4")
    userC_rows = cursor.fetchall()
    
    cursor.execute("SELECT seat_type, price FROM faturamentoTipoAssento_orders_4")
    seatT_rows = cursor.fetchall()

    cursor.execute("SELECT flight_number, reservation_count FROM flight_stats_orders_4")
    flight_stats = cursor.fetchall()
    
    cursor.execute("SELECT destination, reservation_count FROM destination_stats_orders_4")
    destination_stats = cursor.fetchall()

    conn.close()

    flight_numbers = [row[0] for row in flight_stats]
    flight_counts = [row[1] for row in flight_stats]
    
    destinations = [row[0] for row in destination_stats]
    destination_counts = [row[1] for row in destination_stats]

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

    # Dados top 10 voos com mais reservas
    flight_data = sorted(zip(flight_numbers, flight_counts), 
                key=lambda x: x[1], 
                reverse=True)
    top_flights = flight_data[:10]
    top_flight_numbers = [x[0] for x in top_flights]
    top_flight_counts = [x[1] for x in top_flights]
    
    return (payment_methods, payment_revenues, dates, revenues, 
            countries, country_revenues, seat_types, seat_revenues,
            top_flight_numbers, top_flight_counts, destinations, destination_counts)

def plot_revenue(dates, revenues, payment_methods, payment_revenues, 
                countries, country_revenues, seat_types, seat_revenues,
                top_flight_numbers, top_flight_counts, destinations, destination_counts):
    
    # Criar figura com 2 linhas e 3 colunas de gráficos
    fig, axs = plt.subplots(2, 3, figsize=(24, 15))
    
    # Gráfico 1: Receita diária (superior esquerdo)
    axs[0, 0].plot(dates, revenues, marker='o', linestyle='-', color='b', label='Receita Diária')
    axs[0, 0].set_title('Receita Diária')
    axs[0, 0].tick_params(axis='x', rotation=45)
    axs[0, 0].set_xlabel('Data', fontsize=10)
    axs[0, 0].set_ylabel('Receita (R$)', fontsize=10)
    axs[0, 0].tick_params(axis='x', rotation=45)
    axs[0, 0].grid(True, linestyle='--', linewidth=0.5)
    axs[0, 0].legend(fontsize=9)
    axs[0, 0].set_xticks(np.arange(0, len(dates), step=max(1, int(len(dates)/6))))
    axs[0, 0].set_xticklabels(dates[::max(1, int(len(dates)/6))], rotation=45)
    
    # Gráfico 2: Métodos de pagamento (superior meio)
    axs[0, 1].bar(payment_methods, payment_revenues, color='g', alpha=0.7)
    axs[0, 1].set_xlabel('Método de Pagamento', fontsize=10)
    axs[0, 1].set_ylabel('Receita (R$)', fontsize=10)
    axs[0, 1].set_title('Receita por Método de Pagamento', fontsize=12)
    axs[0, 1].tick_params(axis='x', rotation=45)
    axs[0, 1].grid(True, axis='y', linestyle='--', linewidth=0.5)
    axs[0, 1].legend(['Receita por Método'], fontsize=9)


    # Gráfico 3: Países (superior direito)
    axs[0, 2].bar(countries, country_revenues, color='orange', alpha=0.7)
    axs[0, 2].set_xlabel('País do Usuário', fontsize=10)
    axs[0, 2].set_ylabel('Receita (R$)', fontsize=10)
    axs[0, 2].set_title('Receita por País do Usuário', fontsize=12)
    axs[0, 2].tick_params(axis='x', rotation=45)
    axs[0, 2].grid(True, axis='y', linestyle='--', linewidth=0.5)
    axs[0, 2].legend(['Receita por País'], fontsize=9)

    # Gráfico 4: Tipos de assento (inferior esquerdo)
    axs[1, 0].bar(seat_types, seat_revenues, color='purple', alpha=0.7)
    axs[1, 0].set_xlabel('Tipo de Assento', fontsize=10)
    axs[1, 0].set_ylabel('Receita (R$)', fontsize=10)
    axs[1, 0].set_title('Receita por Tipo de Assento', fontsize=12)
    axs[1, 0].tick_params(axis='x', rotation=45)
    axs[1, 0].grid(True, axis='y', linestyle='--', linewidth=0.5)
    axs[1, 0].legend(['Receita por Assento'], fontsize=9)
    
    # Gráfico 5: Voos mais populares (inferior meio)
    axs[1, 1].bar(top_flight_numbers, top_flight_counts, color='red', alpha=0.7)
    axs[1, 1].tick_params(axis='x', rotation=45)
    axs[1, 1].set_xlabel('Número do Voo', fontsize=10)
    axs[1, 1].set_ylabel('Número de Reservas', fontsize=10)
    axs[1, 1].set_title('Top 10 Voos com Mais Reservas', fontsize=12)
    axs[1, 1].tick_params(axis='x', rotation=45)
    axs[1, 1].grid(True, axis='y', linestyle='--', linewidth=0.5)
    axs[1, 1].legend(['Reservas por Voo'], fontsize=9)
    
    # Gráfico 6: Destinos mais populares (inferior direito)
    axs[1, 2].bar(destinations, destination_counts, color='blue', alpha=0.7)
    axs[1, 2].tick_params(axis='x', rotation=45)
    axs[1, 2].set_xlabel('País', fontsize=10)
    axs[1, 2].set_ylabel('Número de Voos', fontsize=10)
    axs[1, 2].set_title('Destinos Mais Populares', fontsize=12)
    axs[1, 2].tick_params(axis='x', rotation=45)
    axs[1, 2].grid(True, axis='y', linestyle='--', linewidth=0.5)
    axs[1, 2].legend(['Reservas de Voo para cada País'], fontsize=9)

    plt.tight_layout()
    plt.show()

# Função principal
def main():
    db_path = 'databases/Database.db'
    (payment_methods, payment_revenues, dates, revenues, 
     countries, country_revenues, seat_types, seat_revenues,
     flight_numbers, flight_counts, destinations, destination_counts) = read_data_from_sqlite(db_path)
    
    plot_revenue(dates, revenues, payment_methods, payment_revenues, 
                countries, country_revenues, seat_types, seat_revenues,
                flight_numbers, flight_counts, destinations, destination_counts)

if __name__ == "__main__":
    main()
