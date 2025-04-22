import pandas as pd
import json
import random
from faker import Faker
from datetime import datetime

faker = Faker()
Faker.seed(42)

# Usamos os dados que criamos com o user_generator.py e flight_generator.py para simular dados com mais sentido,
# sem ids repetidos para diferentes usuários, voos com ids repetidos, nomes melhores para usuários, etc.

# Carrega os dados dos usuários
df_users = pd.read_csv("generator/users.csv")
user_map = df_users.set_index("user_id")["username"].to_dict()
user_ids = list(user_map.keys())

# Carrega os assentos disponíveis
df_seats = pd.read_csv("generator/flights_seats.csv")
df_available_seats = df_seats[df_seats["taken"] == 0].copy()

# Verificar se há assentos suficientes (Temos pouco menos de 200mil linhas no csv de assentos)
assert len(df_available_seats) >= 190000, "Não há assentos disponíveis suficientes!"

# Seleciona assentos únicos (randomizados)
sampled_seats = df_available_seats.sample(n=190000, replace=False).copy()

# Gera os pedidos co o faker
orders = []
for i, (_, seat_row) in enumerate(sampled_seats.iterrows()):
    flight_id = seat_row["flight_id"]
    seat = seat_row["seat"]
    price = seat_row["price"]
    user_id = random.choice(user_ids)
    customer_name = user_map[user_id]  # Nome do user do csv

    status = random.choices(
        ["pending", "confirmed", "cancelled"],
        weights=[0.05, 0.90, 0.05],
        k=1
    )[0]

    payment_method = random.choices(
        ["credit_card", "debit_card", "pix", "paypal"],
        weights=[0.46, 0.34, 0.17, 0.03],
        k=1
    )[0]

    reservation_time = faker.date_time_between(start_date="-600d", end_date="now").isoformat()

    order = {
        "flight_id": "AAA-" + str(flight_id),
        "seat": seat,
        "user_id": str(user_id),
        "customer_name": customer_name,
        "status": status,
        "payment_method": payment_method,
        "reservation_time": reservation_time,
        "price": round(price, 2)
    }

    orders.append(order)

with open("generator/orders.json", "w") as f:
    json.dump(orders, f, indent=2)
