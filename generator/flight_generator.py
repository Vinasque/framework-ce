import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# Países e companhias
foreign_countries = ['USA', 'Argentina', 'Germany', 'Italy', 'France', 'Canada', 'UK', 'Japan', 'Australia']
airlines = ['Gol', 'Inter', 'Azul', 'Thiago Air']
airline_weights = [0.5, 0.2, 0.2, 0.1]

def gerar_dados_voos(n=1000):
    voos = []
    assentos = []
    hoje = datetime.today()

    for flight_id in range(1, n + 1):
        # ORIGEM/DESTINO
        if random.random() < 0.5:
            from_country = 'Brazil'
            to_country = random.choice(foreign_countries)
        else:
            from_country = random.choice(foreign_countries)
            to_country = 'Brazil'

        remaining_seats = random.choice([100, 200, 300])
        date = hoje + timedelta(days=random.randint(0, 600))
        date_str = date.strftime('%Y-%m-%d')
        airline = random.choices(airlines, weights=airline_weights, k=1)[0]

        # Preço base do voo
        base_price = random.randint(500, 10000)

        voos.append([flight_id, from_country, to_country, airline, remaining_seats, date_str])

        # ASSENTOS
        if remaining_seats == 100:
            cols = ['A', 'B', 'C', 'D']
            rows = 25
            primeira_classe_linhas = 5
        elif remaining_seats == 200:
            cols = ['A', 'B', 'C', 'D']
            rows = 50
            primeira_classe_linhas = 10
        else:  # 300
            cols = ['A', 'B', 'C', 'D', 'E', 'F']
            rows = 50
            primeira_classe_linhas = 10

        for r in range(1, rows + 1):
            for c in cols:
                seat = f"{c}{r}"
                seat_class = 'Primeira' if r <= primeira_classe_linhas else 'Econômica'
                price = round(base_price * 1.5 if seat_class == 'Primeira' else base_price, 2)
                assentos.append([flight_id, seat, seat_class, price, 0])

    # Criando e salvando os DataFrames
    df_voos = pd.DataFrame(voos, columns=['flight_id', 'from', 'to', 'airline', 'remaining_seats', 'date'])
    df_assentos = pd.DataFrame(assentos, columns=['flight_id', 'seat', 'seat_class', 'price', 'taken'])

    df_voos.to_csv('generator/flights.csv', index=False)
    df_assentos.to_csv('generator/flights_seats.csv', index=False)

    return df_voos, df_assentos

# Gerando os CSVs
df_voos, df_assentos = gerar_dados_voos()
