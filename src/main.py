import sqlite3
import matplotlib.pyplot as plt

# Função para ler dados do SQLite e retornar como listas
def read_data_from_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Executar a query para pegar os dados da tabela
    cursor.execute("SELECT date, revenue FROM faturamento")
    rows = cursor.fetchall()

    # Separar os dados em duas listas: datas e receitas
    dates = []
    revenues = []

    for row in rows:
        dates.append(row[0])  # Data
        revenues.append(row[1])  # Receita

    conn.close()

    return dates, revenues

# Função para plotar os dados
def plot_revenue(dates, revenues):
    plt.figure(figsize=(10,6))
    plt.plot(dates, revenues, marker='o', linestyle='-', color='b', label='Receita')

    plt.xlabel('Data')
    plt.ylabel('Receita (R$)')
    plt.title('Receita Diária')
    plt.xticks(rotation=45)  # Rotaciona as datas para uma melhor visualização
    plt.grid(True)
    plt.legend()
    plt.tight_layout()  # Ajusta o layout para não cortar labels

    plt.show()

# Função principal para executar o código
def main():
    # Caminho para o banco de dados
    db_path = 'earning.db'

    # Ler os dados do banco de dados SQLite
    dates, revenues = read_data_from_sqlite(db_path)

    # Plotar os dados
    plot_revenue(dates, revenues)

if __name__ == "__main__":
    main()
